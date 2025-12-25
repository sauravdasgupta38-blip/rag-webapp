import os
import json
import time
import base64
import logging
import inspect
from typing import Any, Dict, Optional, Tuple

import aiohttp
from fastapi import FastAPI, Request, Response

from botbuilder.core import (
    ActivityHandler,
    BotFrameworkAdapter,
    BotFrameworkAdapterSettings,
    TurnContext,
)
from botbuilder.schema import Activity
from botframework.connector.auth import MicrosoftAppCredentials


# ---------------- Logging ----------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
logger = logging.getLogger("surecover-bot")


# ---------------- Helpers ----------------
def _env_first(*names: str, default: str = "") -> str:
    """Return the first non-empty env var value from the provided names."""
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return default


def _mask(s: str, keep: int = 6) -> str:
    if not s:
        return ""
    if len(s) <= keep:
        return "*" * len(s)
    return "*" * (len(s) - keep) + s[-keep:]


def _b64url_decode(seg: str) -> bytes:
    seg += "=" * ((4 - len(seg) % 4) % 4)
    return base64.urlsafe_b64decode(seg.encode("utf-8"))


def decode_jwt_noverify(token: str) -> Dict[str, Any]:
    """
    Decode JWT without verifying signature. Good for logging tid/aud/appid.
    """
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return {"_error": "not_a_jwt"}
        payload = json.loads(_b64url_decode(parts[1]).decode("utf-8"))
        return payload
    except Exception as e:
        return {"_error": f"decode_failed: {type(e).__name__}: {e}"}


async def _read_response_text(resp: Any) -> Optional[str]:
    """
    Best-effort read of an async response body (aiohttp ClientResponse usually).
    """
    if resp is None:
        return None
    try:
        # aiohttp: resp.text() is coroutine
        if hasattr(resp, "text") and callable(resp.text):
            v = resp.text()
            if inspect.isawaitable(v):
                return await v
            return str(v)
        # sometimes .content is available
        if hasattr(resp, "content"):
            return str(resp.content)
    except Exception:
        return None
    return None


# ---------------- Bot config from env ----------------
APP_ID = _env_first("MICROSOFT_APP_ID", "MicrosoftAppId", default="")
APP_PASSWORD = _env_first("MICROSOFT_APP_PASSWORD", "MicrosoftAppPassword", default="")
APP_TENANT_ID = _env_first(
    "MicrosoftAppTenantId",
    "MICROSOFT_APP_TENANT_ID",
    default="",
)
APP_TYPE = _env_first("MicrosoftAppType", "MICROSOFT_APP_TYPE", default="").strip() or "MultiTenant"

# Language Studio / Azure AI Language (optional – keep your existing env vars)
LANGUAGE_ENDPOINT = _env_first("LANGUAGE_ENDPOINT", default="")
LANGUAGE_KEY = _env_first("LANGUAGE_KEY", default="")
CLU_PROJECT_NAME = _env_first("CLU_PROJECT_NAME", default="")
CLU_DEPLOYMENT_NAME = _env_first("CLU_DEPLOYMENT_NAME", default="")
QA_PROJECT_NAME = _env_first("QA_PROJECT_NAME", default="")
QA_DEPLOYMENT_NAME = _env_first("QA_DEPLOYMENT_NAME", default="")

logger.info(
    "BOOT_AUTH appType=%s appIdTail=%s tenantIdTail=%s appSecretLen=%s "
    "envUsed(appId=%s appPwd=%s tenant=%s type=%s)",
    APP_TYPE,
    _mask(APP_ID, keep=6),
    _mask(APP_TENANT_ID, keep=6),
    len(APP_PASSWORD) if APP_PASSWORD else 0,
    "MICROSOFT_APP_ID/MicrosoftAppId",
    "MICROSOFT_APP_PASSWORD/MicrosoftAppPassword",
    "MicrosoftAppTenantId/MICROSOFT_APP_TENANT_ID",
    "MicrosoftAppType/MICROSOFT_APP_TYPE",
)

if not APP_ID or not APP_PASSWORD:
    logger.warning(
        "BOOT_AUTH_MISSING appIdPresent=%s appPwdPresent=%s (this will cause auth failures)",
        bool(APP_ID),
        bool(APP_PASSWORD),
    )

# ---------------- Adapter setup ----------------
adapter_settings = BotFrameworkAdapterSettings(APP_ID, APP_PASSWORD)

# Force SingleTenant tenant binding when supported by the installed SDK
if APP_TYPE.lower() == "singletentant" or APP_TYPE.lower() == "singletenant":
    if APP_TENANT_ID:
        if hasattr(adapter_settings, "channel_auth_tenant"):
            adapter_settings.channel_auth_tenant = APP_TENANT_ID
            logger.info("BOOT_AUTH adapter_settings.channel_auth_tenant set to tenantIdTail=%s", _mask(APP_TENANT_ID, 6))
        elif hasattr(adapter_settings, "tenant_id"):
            adapter_settings.tenant_id = APP_TENANT_ID
            logger.info("BOOT_AUTH adapter_settings.tenant_id set to tenantIdTail=%s", _mask(APP_TENANT_ID, 6))
        else:
            logger.warning(
                "BOOT_AUTH could not set tenant on adapter_settings (SDK too old?). "
                "SingleTenant bots may fail unless the SDK supports channel_auth_tenant."
            )
    else:
        logger.warning("BOOT_AUTH SingleTenant selected but no tenant id found in env vars.")

adapter = BotFrameworkAdapter(adapter_settings)


def _log_bot_token_claims(tag: str, service_url: Optional[str] = None) -> None:
    """
    Try to acquire the same token the SDK uses and log key claims.
    If this fails or claims are wrong, it explains the Unauthorized.
    """
    try:
        creds = MicrosoftAppCredentials(APP_ID, APP_PASSWORD)
        if service_url:
            # Not required for outbound, but harmless and useful
            MicrosoftAppCredentials.trust_service_url(service_url)

        tok = creds.get_access_token()
        claims = decode_jwt_noverify(tok)
        logger.error(
            "BF_TOKEN %s tokenPrefix=%s tid=%s aud=%s appid=%s azp=%s iss=%s exp=%s",
            tag,
            tok[:20] + "...",
            claims.get("tid"),
            claims.get("aud"),
            claims.get("appid") or claims.get("azp") or claims.get("appId"),
            claims.get("azp"),
            claims.get("iss"),
            claims.get("exp"),
        )
    except Exception as e:
        logger.error("BF_TOKEN %s failed_to_get_token: %s: %s", tag, type(e).__name__, e)


async def safe_send_text(turn_context: TurnContext, text: str) -> None:
    """
    Send a message with deep diagnostics on Unauthorized.
    """
    svc = getattr(turn_context.activity, "service_url", None)
    ch = getattr(turn_context.activity, "channel_id", None)
    conv = getattr(getattr(turn_context.activity, "conversation", None), "id", None)
    try:
        # Trust service URL for completeness
        if svc:
            MicrosoftAppCredentials.trust_service_url(svc)

        await turn_context.send_activity(text)
    except Exception as e:
        # Log token claims right when it fails
        _log_bot_token_claims("on_send_fail", service_url=svc)

        # Try to extract HTTP response info if present
        resp = getattr(e, "response", None)
        status = getattr(resp, "status", None) or getattr(resp, "status_code", None)
        reason = getattr(resp, "reason", None)
        headers = getattr(resp, "headers", None) or {}
        www_auth = None
        try:
            if headers:
                www_auth = headers.get("WWW-Authenticate") or headers.get("www-authenticate")
        except Exception:
            pass

        body = await _read_response_text(resp)

        logger.error(
            "SEND_FAIL %s: status=%s reason=%s www_auth=%s channel=%s serviceUrl=%s convId=%s body=%s",
            type(e).__name__,
            status,
            reason,
            www_auth,
            ch,
            svc,
            conv,
            (body[:800] + "...") if body and len(body) > 800 else body,
        )
        raise


# ---------------- Azure AI Language calls (simple example) ----------------
async def call_clu(text: str) -> Tuple[str, float, Dict[str, Any]]:
    """
    Returns (intent, score, raw_json). If not configured, returns fallback.
    """
    if not (LANGUAGE_ENDPOINT and LANGUAGE_KEY and CLU_PROJECT_NAME and CLU_DEPLOYMENT_NAME):
        return ("SmallTalk", 0.0, {"_note": "CLU not configured; fallback intent"})
    url = f"{LANGUAGE_ENDPOINT}/language/:analyze-conversations?api-version=2022-10-01-preview"
    payload = {
        "kind": "Conversation",
        "analysisInput": {
            "conversationItem": {
                "id": "1",
                "participantId": "user",
                "text": text,
            }
        },
        "parameters": {
            "projectName": CLU_PROJECT_NAME,
            "deploymentName": CLU_DEPLOYMENT_NAME,
            "stringIndexType": "Utf16CodeUnit",
        },
    }
    headers = {"Ocp-Apim-Subscription-Key": LANGUAGE_KEY, "Content-Type": "application/json"}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=payload) as r:
            j = await r.json()
    top = j.get("result", {}).get("prediction", {}).get("topIntent", "")
    intents = j.get("result", {}).get("prediction", {}).get("intents", [])
    score = 0.0
    for it in intents:
        if it.get("category") == top:
            score = float(it.get("confidenceScore", 0.0))
            break
    return (top or "None", score, j)


async def call_qa(text: str) -> Tuple[str, float, Dict[str, Any]]:
    """
    Returns (answer, score, raw_json). If not configured, returns fallback.
    """
    if not (LANGUAGE_ENDPOINT and LANGUAGE_KEY and QA_PROJECT_NAME and QA_DEPLOYMENT_NAME):
        return ("(QA not configured)", 0.0, {"_note": "QA not configured"})
    url = f"{LANGUAGE_ENDPOINT}/language/:query-knowledgebases?api-version=2021-10-01"
    payload = {
        "question": text,
        "projectName": QA_PROJECT_NAME,
        "deploymentName": QA_DEPLOYMENT_NAME,
        "top": 1,
    }
    headers = {"Ocp-Apim-Subscription-Key": LANGUAGE_KEY, "Content-Type": "application/json"}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=payload) as r:
            j = await r.json()
    answers = j.get("answers", [])
    if not answers:
        return ("(no answer)", 0.0, j)
    best = answers[0]
    return (best.get("answer", "(empty)"), float(best.get("confidenceScore", 0.0)), j)


# ---------------- Bot logic ----------------
class SurecoverBot(ActivityHandler):
    async def on_message_activity(self, turn_context: TurnContext):
        user_text = (turn_context.activity.text or "").strip()
        if not user_text:
            await safe_send_text(turn_context, "I didn't receive any text.")
            return

        intent, score, _ = await call_clu(user_text)
        logger.info("CLU intent=%s score=%.3f", intent, score)

        # Example routing
        if intent.lower() == "smalltalk":
            await safe_send_text(turn_context, "Hi! (SmallTalk) — try asking me a question.")
            return

        ans, qa_score, _ = await call_qa(user_text)
        logger.info("QA score=%.3f", qa_score)
        await safe_send_text(turn_context, ans)


bot = SurecoverBot()


# ---------------- Error handler ----------------
async def on_error(context: TurnContext, error: Exception):
    logger.exception("[on_turn_error] %s", error)
    # Try to send a user-friendly error, but don’t create a second exception storm
    try:
        await safe_send_text(context, "Sorry, the bot hit an error.")
    except Exception as e:
        logger.error("on_error: failed to send error message (likely auth issue): %s", e)


adapter.on_turn_error = on_error


# ---------------- FastAPI app ----------------
app = FastAPI()


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "time": int(time.time()),
        "appType": APP_TYPE,
        "appIdTail": _mask(APP_ID, 6),
        "tenantIdTail": _mask(APP_TENANT_ID, 6),
    }


@app.get("/debug/token")
async def debug_token():
    """
    Returns token claims (no signature verification) for the token the SDK acquires.
    Do NOT expose this publicly in production.
    """
    try:
        creds = MicrosoftAppCredentials(APP_ID, APP_PASSWORD)
        tok = creds.get_access_token()
        claims = decode_jwt_noverify(tok)
        # return only safe-ish subset
        return {
            "tokenPrefix": tok[:20] + "...",
            "tid": claims.get("tid"),
            "aud": claims.get("aud"),
            "appid": claims.get("appid") or claims.get("azp"),
            "iss": claims.get("iss"),
            "exp": claims.get("exp"),
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@app.post("/api/messages")
async def messages(req: Request):
    body = await req.json()
    activity = Activity().deserialize(body)
    auth_header = req.headers.get("Authorization", "")

    # Incoming HTTP diagnostics (don’t log full bearer token)
    auth_present = bool(auth_header)
    auth_prefix = auth_header[:30] + "..." if auth_present else ""
    logger.info("HTTP_IN /api/messages authHeaderPresent=%s authHeaderPrefix=%s", auth_present, auth_prefix)

    # Activity diagnostics
    try:
        conv_id = getattr(getattr(activity, "conversation", None), "id", None)
        from_id = getattr(getattr(activity, "from_property", None), "id", None)
        recip_id = getattr(getattr(activity, "recipient", None), "id", None)
        service_url = getattr(activity, "service_url", None)
        channel_id = getattr(activity, "channel_id", None)
        logger.info(
            "ACTIVITY_IN type=%s channel=%s serviceUrl=%s convId=%s from=%s recipient=%s authHint=%s",
            getattr(activity, "type", None),
            channel_id,
            service_url,
            conv_id,
            from_id,
            recip_id,
            "hasChannelData" if getattr(activity, "channel_data", None) else "noChannelData",
        )
        if service_url:
            logger.info("BOT_AUTH trusted serviceUrl=%s", service_url)
            MicrosoftAppCredentials.trust_service_url(service_url)
    except Exception:
        logger.exception("ACTIVITY_IN logging failed")

    invoke_response = await adapter.process_activity(activity, auth_header, bot.on_turn)

    if invoke_response:
        return Response(
            content=invoke_response.body,
            status_code=invoke_response.status,
            media_type="application/json",
        )

    return Response(status_code=201)
