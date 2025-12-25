import os
import json
import base64
import asyncio
import logging
from typing import Any, Dict, Optional

import aiohttp
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from botbuilder.core import BotFrameworkAdapter, BotFrameworkAdapterSettings, TurnContext, ActivityHandler
from botbuilder.schema import Activity
from botbuilder.schema._models_py3 import ErrorResponseException

from botframework.connector.auth import MicrosoftAppCredentials

# Optional (for token self-test)
import msal


# -----------------------------
# Logging
# -----------------------------
logger = logging.getLogger("surecover-bot")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s:%(message)s")


# -----------------------------
# Helpers
# -----------------------------
def env_first(*names: str, default: str = "") -> str:
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return v
    return default


def mask(s: str, keep: int = 6) -> str:
    if not s:
        return ""
    if len(s) <= keep:
        return s
    return s[:keep] + "…"


def jwt_claims_noverify(token: str) -> Dict[str, Any]:
    """
    Decode JWT payload without verifying signature (for diagnostics only).
    """
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return {}
        payload = parts[1]
        payload += "=" * (-len(payload) % 4)
        data = base64.urlsafe_b64decode(payload.encode("utf-8"))
        return json.loads(data.decode("utf-8"))
    except Exception:
        return {}


async def safe_send_text(turn_context: TurnContext, text: str) -> None:
    """
    Send a message, but if BF connector returns Unauthorized, log extra detail.
    """
    try:
        await turn_context.send_activity(text)
    except ErrorResponseException as e:
        # Try to extract HTTP response details if present
        status = getattr(getattr(e, "response", None), "status_code", None)
        body = None
        try:
            resp = getattr(e, "response", None)
            if resp is not None:
                body = getattr(resp, "text", None)
                if callable(body):
                    body = body()
        except Exception:
            body = None

        logger.error(
            "SEND_FAIL ErrorResponseException: status=%s msg=%s channel=%s serviceUrl=%s convId=%s",
            status,
            str(e),
            getattr(turn_context.activity, "channel_id", None),
            getattr(turn_context.activity, "service_url", None),
            getattr(turn_context.activity.conversation, "id", None),
        )
        if body:
            logger.error("SEND_FAIL response_body (first 800 chars): %s", str(body)[:800])
        raise
    except Exception as e:
        logger.error("SEND_FAIL Unexpected: %s", str(e))
        raise


def configure_single_tenant_hints(settings: BotFrameworkAdapterSettings, tenant_id: str) -> None:
    """
    BotBuilder Python has changed over versions. This tries to set tenant hints
    only if the properties exist in your installed SDK.
    """
    if not tenant_id:
        return

    # Some versions support these attributes.
    for attr_name in ("channel_auth_tenant", "ChannelAuthTenant"):
        if hasattr(settings, attr_name):
            try:
                setattr(settings, attr_name, tenant_id)
            except Exception:
                pass

    # Some versions expose oauth endpoint fields (names vary).
    oauth_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    for attr_name in ("o_auth_endpoint", "oauth_endpoint", "OAuthEndpoint"):
        if hasattr(settings, attr_name):
            try:
                setattr(settings, attr_name, oauth_url)
            except Exception:
                pass


async def msal_token_self_test(app_id: str, secret: str, tenant_id: str) -> None:
    """
    Acquire a Bot Framework token using MSAL and log decoded claims.
    This helps distinguish 'bad secret/tenant' from 'connector unauthorized'.
    """
    if not app_id or not secret:
        logger.warning("TOKEN_SELF_TEST skipped (missing app_id or secret).")
        return

    authority = (
        f"https://login.microsoftonline.com/{tenant_id}"
        if tenant_id
        else "https://login.microsoftonline.com/botframework.com"
    )
    try:
        cca = msal.ConfidentialClientApplication(
            client_id=app_id,
            authority=authority,
            client_credential=secret,
        )
        result = cca.acquire_token_for_client(scopes=["https://api.botframework.com/.default"])
        if "access_token" not in result:
            logger.error("TOKEN_SELF_TEST failed. authority=%s error=%s desc=%s",
                         authority, result.get("error"), result.get("error_description"))
            return

        claims = jwt_claims_noverify(result["access_token"])
        logger.info(
            "TOKEN_SELF_TEST success. authority=%s claims.appid=%s claims.aud=%s claims.tid=%s claims.iss=%s",
            authority,
            claims.get("appid"),
            claims.get("aud"),
            claims.get("tid"),
            claims.get("iss"),
        )
    except Exception as e:
        logger.exception("TOKEN_SELF_TEST exception: %s", str(e))


# -----------------------------
# Optional CLU/QA (kept, but bot still works without these env vars)
# -----------------------------
LANGUAGE_ENDPOINT = env_first("LANGUAGE_ENDPOINT", "AZURE_LANGUAGE_ENDPOINT", default="")
LANGUAGE_KEY = env_first("LANGUAGE_KEY", "AZURE_LANGUAGE_KEY", default="")
CLU_PROJECT = env_first("CLU_PROJECT", default="")
CLU_DEPLOYMENT = env_first("CLU_DEPLOYMENT", default="")
QA_PROJECT = env_first("QA_PROJECT", default="")
QA_DEPLOYMENT = env_first("QA_DEPLOYMENT", default="")


async def call_clu(user_text: str) -> Dict[str, Any]:
    if not (LANGUAGE_ENDPOINT and LANGUAGE_KEY and CLU_PROJECT and CLU_DEPLOYMENT):
        return {"topIntent": "None", "confidenceScore": 0.0, "entities": []}

    url = (
        f"{LANGUAGE_ENDPOINT.rstrip('/')}/language/:analyze-conversations"
        f"?api-version=2023-04-01"
    )
    payload = {
        "kind": "Conversation",
        "analysisInput": {
            "conversationItem": {
                "id": "1",
                "participantId": "user",
                "text": user_text,
            }
        },
        "parameters": {
            "projectName": CLU_PROJECT,
            "deploymentName": CLU_DEPLOYMENT,
            "stringIndexType": "TextElement_V8",
        },
    }

    headers = {"Ocp-Apim-Subscription-Key": LANGUAGE_KEY, "Content-Type": "application/json"}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers, timeout=20) as resp:
            data = await resp.json(content_type=None)
            # best-effort parse
            try:
                pred = data["result"]["prediction"]
                top = pred.get("topIntent")
                intents = pred.get("intents", {})
                score = intents.get(top, {}).get("confidenceScore", 0.0)
                entities = pred.get("entities", [])
                return {"topIntent": top, "confidenceScore": score, "entities": entities}
            except Exception:
                return {"raw": data}


async def call_qa(user_text: str) -> Dict[str, Any]:
    if not (LANGUAGE_ENDPOINT and LANGUAGE_KEY and QA_PROJECT and QA_DEPLOYMENT):
        return {"answer": "", "confidence": 0.0}

    url = (
        f"{LANGUAGE_ENDPOINT.rstrip('/')}/language/:query-knowledgebases"
        f"?api-version=2021-10-01"
    )
    payload = {
        "projectName": QA_PROJECT,
        "deploymentName": QA_DEPLOYMENT,
        "question": user_text,
        "top": 1,
    }
    headers = {"Ocp-Apim-Subscription-Key": LANGUAGE_KEY, "Content-Type": "application/json"}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers, timeout=20) as resp:
            data = await resp.json(content_type=None)
            try:
                ans = data["answers"][0]
                return {"answer": ans.get("answer", ""), "confidence": ans.get("confidenceScore", 0.0)}
            except Exception:
                return {"raw": data}


# -----------------------------
# Bot
# -----------------------------
class SurecoverBot(ActivityHandler):
    async def on_message_activity(self, turn_context: TurnContext):
        text = (turn_context.activity.text or "").strip()

        # CLU/QA logging (doesn't block the bot if not configured)
        try:
            clu = await call_clu(text)
            logger.info("CLU intent=%s score=%s entities=%s",
                        clu.get("topIntent"), clu.get("confidenceScore"), clu.get("entities"))
        except Exception as e:
            logger.warning("CLU call failed: %s", str(e))

        try:
            qa = await call_qa(text)
            logger.info("QA confidence=%s", qa.get("confidence"))
        except Exception as e:
            logger.warning("QA call failed: %s", str(e))

        # For now, echo back (keeps the send path simple for auth debugging)
        await safe_send_text(turn_context, f"You said: {text}")


# -----------------------------
# Adapter + FastAPI
# -----------------------------
APP_ID = env_first("MicrosoftAppId", "MICROSOFT_APP_ID", default="")
APP_SECRET = env_first("MicrosoftAppPassword", "MICROSOFT_APP_PASSWORD", default="")
TENANT_ID = env_first("MicrosoftAppTenantId", "MICROSOFT_APP_TENANT_ID", default="")
APP_TYPE = env_first("MicrosoftAppType", "MICROSOFT_APP_TYPE", default="")

settings = BotFrameworkAdapterSettings(APP_ID, APP_SECRET)

# best-effort single-tenant hints
if (APP_TYPE or "").lower() == "singletenant" and TENANT_ID:
    configure_single_tenant_hints(settings, TENANT_ID)

adapter = BotFrameworkAdapter(settings)
bot = SurecoverBot()

logger.info(
    "BOT_AUTH_CONFIG appId=%s tenantId=%s appType=%s appSecretPresent=%s",
    mask(APP_ID), mask(TENANT_ID), APP_TYPE or "(unset)", bool(APP_SECRET)
)
if TENANT_ID:
    logger.info(
        "BOT_AUTH_CONFIG using tenant oauth_endpoint=%s",
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    )


async def on_error(turn_context: TurnContext, error: Exception):
    logger.exception("[on_turn_error] %s", str(error))
    # Don't endlessly fail trying to send an error message if auth is broken.
    try:
        await safe_send_text(turn_context, "Sorry, the bot hit an error.")
    except Exception as e:
        logger.error("on_error: failed to send error message (likely auth issue): %s", str(e))


# IMPORTANT: assignment (NOT decorator) to avoid NoneType callable crash
adapter.on_turn_error = on_error

app = FastAPI()


@app.on_event("startup")
async def startup():
    # Acquire token once at startup and log claims (helps isolate auth problems).
    await msal_token_self_test(APP_ID, APP_SECRET, TENANT_ID)


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/debug/token-claims")
async def debug_token_claims():
    """
    Returns claims from a freshly acquired Bot Framework token (if possible).
    No secrets returned.
    """
    authority = (
        f"https://login.microsoftonline.com/{TENANT_ID}" if TENANT_ID
        else "https://login.microsoftonline.com/botframework.com"
    )
    cca = msal.ConfidentialClientApplication(APP_ID, authority=authority, client_credential=APP_SECRET)
    result = cca.acquire_token_for_client(scopes=["https://api.botframework.com/.default"])
    if "access_token" not in result:
        return JSONResponse({"ok": False, "authority": authority, "error": result.get("error"),
                             "error_description": result.get("error_description")}, status_code=500)
    claims = jwt_claims_noverify(result["access_token"])
    # return only a few relevant claims
    return {
        "ok": True,
        "authority": authority,
        "claims": {k: claims.get(k) for k in ("appid", "aud", "tid", "iss", "azp", "ver")}
    }


@app.post("/api/messages")
async def messages(req: Request):
    auth_header = req.headers.get("Authorization", "")

    logger.info(
        "HTTP_IN /api/messages authHeaderPresent=%s authHeaderPrefix=%s",
        bool(auth_header),
        (auth_header[:25] + "...") if auth_header else ""
    )

    body = await req.json()
    activity = Activity().deserialize(body)

    logger.info(
        "ACTIVITY_IN type=%s channel=%s serviceUrl=%s convId=%s from=%s recipient=%s authHint=%s",
        activity.type,
        activity.channel_id,
        activity.service_url,
        getattr(activity.conversation, "id", None),
        getattr(activity.from_property, "id", None),
        getattr(activity.recipient, "id", None),
        "hasChannelData" if getattr(activity, "channel_data", None) else "noChannelData"
    )

    # Trust the inbound serviceUrl for replies (important for some channel/serviceUrl combos)
    if activity.service_url:
        try:
            MicrosoftAppCredentials.trust_service_url(activity.service_url)
            logger.info("BOT_AUTH trusted serviceUrl=%s", activity.service_url)
        except Exception as e:
            logger.warning("BOT_AUTH could not trust serviceUrl=%s err=%s", activity.service_url, str(e))

    # If there's a bearer token on the inbound auth header, log a couple claims
    if auth_header.lower().startswith("bearer "):
        inbound_token = auth_header.split(" ", 1)[1].strip()
        claims = jwt_claims_noverify(inbound_token)
        if claims:
            logger.info(
                "INBOUND_JWT claims.aud=%s claims.iss=%s claims.tid=%s claims.serviceurl=%s",
                claims.get("aud"), claims.get("iss"), claims.get("tid"), claims.get("serviceurl")
            )

    try:
        await adapter.process_activity(activity, auth_header, bot.on_turn)
        return Response(status_code=200)
    except Exception as e:
        logger.exception("PROCESS_ACTIVITY_FAILED: %s", str(e))
        return Response(status_code=500)
