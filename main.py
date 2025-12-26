import os
import json
import base64
import logging
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from fastapi import FastAPI, Request, Response

from botbuilder.core import (
    BotFrameworkAdapter,
    BotFrameworkAdapterSettings,
    TurnContext,
    ActivityHandler,
)
from botbuilder.schema import Activity


# ---------------- Logging ----------------
logger = logging.getLogger("surecover-bot")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s:%(message)s")


# ---------------- Helpers ----------------
def _redact(s: Optional[str], keep: int = 6) -> str:
    if not s:
        return ""
    if len(s) <= keep * 2:
        return s[:2] + "…"
    return f"{s[:keep]}…{s[-keep:]}"


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name, default)
    if v is None:
        return None
    v = v.strip()
    return v if v else None


def _decode_jwt_no_verify(token: str) -> Dict[str, Any]:
    """
    Decode JWT payload without verifying signature. Useful for diagnostics only.
    """
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return {}
        payload_b64 = parts[1]
        # pad
        payload_b64 += "=" * (-len(payload_b64) % 4)
        raw = base64.urlsafe_b64decode(payload_b64.encode("utf-8"))
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return {}


async def _read_response_text(resp: Any) -> str:
    try:
        if resp is None:
            return ""
        if hasattr(resp, "text"):
            return await resp.text()
    except Exception:
        pass
    return ""


def _safe_str(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v)
    except Exception:
        return str(v)


# ---------------- Config ----------------
MICROSOFT_APP_ID = _env("MicrosoftAppId") or _env("MICROSOFT_APP_ID")
MICROSOFT_APP_PASSWORD = _env("MicrosoftAppPassword") or _env("MICROSOFT_APP_PASSWORD")
MICROSOFT_APP_TENANT_ID = _env("MicrosoftAppTenantId") or _env("MicrosoftAppTenantID") or _env("MicrosoftAppTenantId")
MICROSOFT_APP_TYPE = _env("MicrosoftAppType") or "SingleTenant"

LANGUAGE_ENDPOINT = _env("LANGUAGE_ENDPOINT")  # like https://xxxx.cognitiveservices.azure.com
LANGUAGE_KEY = _env("LANGUAGE_KEY")

CLU_PROJECT_NAME = _env("CLU_PROJECT_NAME")
CLU_DEPLOYMENT_NAME = _env("CLU_DEPLOYMENT_NAME")

QA_PROJECT_NAME = _env("QA_PROJECT_NAME")
QA_DEPLOYMENT_NAME = _env("QA_DEPLOYMENT_NAME")

# Tuning knobs
QA_MIN_CONFIDENCE = float(_env("QA_MIN_CONFIDENCE", "0.2") or "0.2")
# If you want to always use QA regardless of CLU, set to 1
USE_CLU = (_env("USE_CLU", "1") or "1") != "0"


def _log_startup_config() -> None:
    logger.info(
        "BOT_AUTH_CONFIG appId=%s tenantId=%s appType=%s appSecretPresent=%s",
        _redact(MICROSOFT_APP_ID),
        _redact(MICROSOFT_APP_TENANT_ID),
        MICROSOFT_APP_TYPE,
        bool(MICROSOFT_APP_PASSWORD),
    )
    logger.info(
        "LANGUAGE_CONFIG endpoint=%s keyPresent=%s CLU(project=%s deployment=%s) QA(project=%s deployment=%s) QA_MIN_CONFIDENCE=%.2f USE_CLU=%s",
        LANGUAGE_ENDPOINT,
        bool(LANGUAGE_KEY),
        CLU_PROJECT_NAME,
        CLU_DEPLOYMENT_NAME,
        QA_PROJECT_NAME,
        QA_DEPLOYMENT_NAME,
        QA_MIN_CONFIDENCE,
        USE_CLU,
    )


# ---------------- Bot Adapter ----------------
settings = BotFrameworkAdapterSettings(app_id=MICROSOFT_APP_ID or "", app_password=MICROSOFT_APP_PASSWORD or "")
adapter = BotFrameworkAdapter(settings)


async def on_error(context: TurnContext, error: Exception) -> None:
    logger.exception("[on_turn_error] %s", str(error))
    # Try to tell the user, but don't crash if send fails (auth issues etc)
    try:
        await safe_send_text(context, "Sorry, the bot hit an error.")
    except Exception as e:
        logger.error("on_error: failed to send error message (likely auth issue): %s", str(e))


adapter.on_turn_error = on_error


# ---------------- Outbound send wrapper ----------------
async def safe_send_text(turn_context: TurnContext, text: str) -> None:
    """
    Sends a message and logs useful diagnostics if the connector returns Unauthorized.
    Ensures we never send an empty message (Web Chat may appear to 'echo only' otherwise).
    """
    svc = getattr(turn_context.activity, "service_url", None)
    ch = getattr(turn_context.activity, "channel_id", None)
    conv = getattr(getattr(turn_context.activity, "conversation", None), "id", None)

    # Never send empty
    text_to_send = (text or "").strip()
    if not text_to_send:
        text_to_send = "I’m not sure I understood that. Can you rephrase?"

    try:
        # Trust service URL (helps in multi-service-url scenarios)
        if svc:
            from botframework.connector.auth import MicrosoftAppCredentials
            MicrosoftAppCredentials.trust_service_url(svc)

        await turn_context.send_activity(text_to_send)
        logger.info("SEND_OK channel=%s serviceUrl=%s convId=%s chars=%d", ch, svc, conv, len(text_to_send))
    except Exception as e:
        # If available, log response details for ErrorResponseException
        resp = getattr(e, "response", None)
        status = getattr(resp, "status", None) or getattr(resp, "status_code", None)
        reason = getattr(resp, "reason", None)
        headers = {}
        try:
            headers = dict(getattr(resp, "headers", {}) or {})
        except Exception:
            headers = {}

        www_auth = headers.get("WWW-Authenticate") or headers.get("www-authenticate")
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
            body[:1200] if body else "",
        )
        raise


# ---------------- Azure AI Language Calls ----------------
def _require_language_config() -> None:
    missing = []
    if not LANGUAGE_ENDPOINT:
        missing.append("LANGUAGE_ENDPOINT")
    if not LANGUAGE_KEY:
        missing.append("LANGUAGE_KEY")
    if missing:
        raise RuntimeError(f"Missing language config env vars: {', '.join(missing)}")


async def call_clu(user_text: str) -> Tuple[Optional[str], float, List[Dict[str, Any]], Dict[str, Any]]:
    """
    Returns: (top_intent, score, entities, raw_json)
    """
    _require_language_config()
    if not CLU_PROJECT_NAME or not CLU_DEPLOYMENT_NAME:
        logger.warning("CLU not configured (missing CLU_PROJECT_NAME / CLU_DEPLOYMENT_NAME).")
        return None, 0.0, [], {}

    url = f"{LANGUAGE_ENDPOINT}/language/:analyze-conversations?api-version=2022-10-01-preview"
    payload = {
        "kind": "Conversation",
        "analysisInput": {
            "conversationItem": {
                "id": "1",
                "participantId": "user",
                "modality": "text",
                "language": "en-gb",
                "text": user_text,
            }
        },
        "parameters": {
            "projectName": CLU_PROJECT_NAME,
            "deploymentName": CLU_DEPLOYMENT_NAME,
            "verbose": True,
        },
    }

    headers = {
        "Ocp-Apim-Subscription-Key": LANGUAGE_KEY,
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=payload) as resp:
            txt = await resp.text()
            try:
                j = json.loads(txt) if txt else {}
            except Exception:
                j = {"_raw": txt}

            if resp.status >= 400:
                logger.error("CLU_HTTP status=%s body=%s", resp.status, (txt or "")[:1200])
                return None, 0.0, [], j

    prediction = (j.get("result") or {}).get("prediction") or {}
    top_intent = prediction.get("topIntent")
    intents = prediction.get("intents") or {}
    score = float(intents.get(top_intent, {}).get("confidenceScore", 0.0)) if top_intent else 0.0
    entities = prediction.get("entities") or []
    logger.info("CLU intent=%s score=%.3f entities=%s", top_intent, score, _safe_str(entities)[:800])
    return top_intent, score, entities, j


async def call_qa(user_text: str) -> Tuple[str, float, Dict[str, Any]]:
    """
    Returns: (answer, confidence, raw_json)
    """
    _require_language_config()
    if not QA_PROJECT_NAME or not QA_DEPLOYMENT_NAME:
        logger.warning("QA not configured (missing QA_PROJECT_NAME / QA_DEPLOYMENT_NAME).")
        return "", 0.0, {}

    url = f"{LANGUAGE_ENDPOINT}/language/:query-knowledgebases?api-version=2021-10-01"
    payload = {
        "question": user_text,
        "top": 1,
        "confidenceScoreThreshold": 0.0,
        "includeUnstructuredSources": True,
        "answersSpanRequest": {"enable": True},
        "projectName": QA_PROJECT_NAME,
        "deploymentName": QA_DEPLOYMENT_NAME,
    }

    headers = {
        "Ocp-Apim-Subscription-Key": LANGUAGE_KEY,
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=payload) as resp:
            txt = await resp.text()
            try:
                j = json.loads(txt) if txt else {}
            except Exception:
                j = {"_raw": txt}

            if resp.status >= 400:
                logger.error("QA_HTTP status=%s body=%s", resp.status, (txt or "")[:1200])
                return "", 0.0, j

    answers = j.get("answers") or []
    if not answers:
        logger.info("QA confidence=0.0 (no answers)")
        return "", 0.0, j

    best = answers[0] or {}
    ans = (best.get("answer") or "").strip()
    conf = float(best.get("confidenceScore", 0.0) or 0.0)
    logger.info("QA confidence=%.3f answerPreview=%s", conf, ans[:120])
    return ans, conf, j


# ---------------- Bot Logic ----------------
class SurecoverBot(ActivityHandler):
    async def on_message_activity(self, turn_context: TurnContext) -> None:
        user_text = (turn_context.activity.text or "").strip()
        if not user_text:
            await safe_send_text(turn_context, "Please type a message.")
            return

        # 1) CLU
        intent = None
        intent_score = 0.0
        entities: List[Dict[str, Any]] = []

        if USE_CLU:
            intent, intent_score, entities, _ = await call_clu(user_text)

        # 2) Route to QA (typical pattern: always QA unless you have other skills)
        # If you later add more intents (Claims, Policy, etc.), route here.
        answer, qa_score, _ = await call_qa(user_text)

        # 3) Fallbacks
        # If CLU says SmallTalk and QA is weak, use a friendly smalltalk fallback.
        if (intent or "").lower() == "smalltalk" and qa_score < QA_MIN_CONFIDENCE:
            await safe_send_text(turn_context, "Hey! 😊 How can I help you today?")
            return

        # If QA gives “no answer” or low confidence, still respond clearly.
        if not answer or qa_score < QA_MIN_CONFIDENCE:
            await safe_send_text(
                turn_context,
                "I don’t have a confident answer for that yet. Try rephrasing, or ask a more specific question."
            )
            return

        await safe_send_text(turn_context, answer)

    async def on_conversation_update_activity(self, turn_context: TurnContext) -> None:
        # Optional: greet on join. Keep quiet for now to avoid noise.
        return


bot = SurecoverBot()


# ---------------- FastAPI App ----------------
app = FastAPI()


@app.on_event("startup")
async def _startup() -> None:
    _log_startup_config()
    # Optional “self test” of Language config (does not call CLU/QA to avoid extra cost)
    if not LANGUAGE_ENDPOINT or not LANGUAGE_KEY:
        logger.warning("LANGUAGE_ENDPOINT / LANGUAGE_KEY not set; CLU/QA will not work.")
    else:
        logger.info("Language config present.")


@app.post("/api/messages")
async def messages(req: Request) -> Response:
    auth_header = req.headers.get("Authorization", "")
    auth_present = bool(auth_header)
    prefix = auth_header[:32] + "..." if auth_header else ""

    logger.info(
        "HTTP_IN /api/messages authHeaderPresent=%s authHeaderPrefix=%s",
        auth_present,
        prefix,
    )

    body = await req.json()
    activity = Activity().deserialize(body)

    # Log some activity basics
    svc = getattr(activity, "service_url", None)
    ch = getattr(activity, "channel_id", None)
    conv = getattr(getattr(activity, "conversation", None), "id", None)
    from_id = getattr(getattr(activity, "from_property", None), "id", None)
    recip = getattr(getattr(activity, "recipient", None), "id", None)
    has_cd = bool(getattr(activity, "channel_data", None))

    logger.info(
        "ACTIVITY_IN type=%s channel=%s serviceUrl=%s convId=%s from=%s recipient=%s authHint=%s",
        activity.type,
        ch,
        svc,
        conv,
        from_id,
        recip,
        "hasChannelData" if has_cd else "noChannelData",
    )

    # Decode inbound JWT claims (no verify) for troubleshooting
    if auth_header.lower().startswith("bearer "):
        claims = _decode_jwt_no_verify(auth_header.split(" ", 1)[1])
        logger.info(
            "INBOUND_JWT claims.aud=%s claims.iss=%s claims.tid=%s claims.serviceurl=%s",
            claims.get("aud"),
            claims.get("iss"),
            claims.get("tid"),
            claims.get("serviceurl"),
        )

    try:
        await adapter.process_activity(activity, auth_header, bot.on_turn)
        return Response(status_code=200)
    except Exception as e:
        logger.exception("HTTP_ERR /api/messages: %s", str(e))
        return Response(status_code=500, content="Error")


# Health check
@app.get("/")
async def root() -> Dict[str, str]:
    return {"status": "ok"}
