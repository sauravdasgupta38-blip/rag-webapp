import os
import json
import logging
import base64
from typing import Tuple, Optional, Any, Dict

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
from botbuilder.schema._models_py3 import ErrorResponseException  # for detailed exception handling


# ======================
# Optional: .env support
# ======================
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(override=False)
except Exception:
    pass


# -----------------------
# Logging
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
logger = logging.getLogger("surecover-bot")


def _mask(s: str, keep_last: int = 4) -> str:
    if not s:
        return ""
    if len(s) <= keep_last:
        return "*" * len(s)
    return "*" * (len(s) - keep_last) + s[-keep_last:]


def _get_first_env(keys) -> str:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return ""


def _decode_jwt_claims_no_verify(token: str) -> Dict[str, Any]:
    """
    Decodes JWT payload without verifying signature (safe for diagnostics).
    Returns {} on failure.
    """
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return {}
        payload_b64 = parts[1]
        # pad
        payload_b64 += "=" * (-len(payload_b64) % 4)
        payload = base64.urlsafe_b64decode(payload_b64.encode("utf-8")).decode("utf-8")
        return json.loads(payload)
    except Exception:
        return {}


# -----------------------
# Azure AI Language config
# -----------------------
LANGUAGE_ENDPOINT = os.getenv("LANGUAGE_ENDPOINT", "").rstrip("/")
LANGUAGE_KEY = os.getenv("LANGUAGE_KEY", "")

CLU_PROJECT_NAME = os.getenv("CLU_PROJECT_NAME", "surecover-clu")
CLU_DEPLOYMENT_NAME = os.getenv("CLU_DEPLOYMENT_NAME", "surecover_dep")
CLU_LANGUAGE = os.getenv("CLU_LANGUAGE", "en-gb")

QA_PROJECT_NAME = os.getenv("QA_PROJECT_NAME", "surecover-qa")
QA_DEPLOYMENT_NAME = os.getenv("QA_DEPLOYMENT_NAME", "production")

CLU_API_VERSION = os.getenv("CLU_API_VERSION", "2024-11-15-preview")
QA_API_VERSION = os.getenv("QA_API_VERSION", "2021-10-01")

INTENT_MIN_CONFIDENCE = float(os.getenv("INTENT_MIN_CONFIDENCE", "0.55"))
QA_MIN_CONFIDENCE = float(os.getenv("QA_MIN_CONFIDENCE", "0.45"))


# -----------------------
# Bot auth env (support multiple common names)
# -----------------------
MICROSOFT_APP_ID = _get_first_env(
    ["MICROSOFT_APP_ID", "MicrosoftAppId", "MicrosoftAppID", "BOT_APP_ID"]
)
MICROSOFT_APP_PASSWORD = _get_first_env(
    ["MICROSOFT_APP_PASSWORD", "MicrosoftAppPassword", "BOT_APP_PASSWORD"]
)
MICROSOFT_APP_TENANT_ID = _get_first_env(
    ["MicrosoftAppTenantId", "MICROSOFT_APP_TENANT_ID", "BOT_TENANT_ID"]
)
MICROSOFT_APP_TYPE = _get_first_env(
    ["MicrosoftAppType", "MICROSOFT_APP_TYPE", "BOT_APP_TYPE"]
)

logger.info(
    "BOT_AUTH startup: app_id=%s tenant_id=%s app_type=%s password_present=%s password_len=%s",
    MICROSOFT_APP_ID or "<EMPTY>",
    MICROSOFT_APP_TENANT_ID or "<EMPTY>",
    MICROSOFT_APP_TYPE or "<EMPTY>",
    "YES" if MICROSOFT_APP_PASSWORD else "NO",
    len(MICROSOFT_APP_PASSWORD) if MICROSOFT_APP_PASSWORD else 0,
)

if not MICROSOFT_APP_ID:
    logger.warning("BOT_AUTH: MICROSOFT_APP_ID/MicrosoftAppId is empty. Outbound calls will fail.")
if not MICROSOFT_APP_PASSWORD:
    logger.warning("BOT_AUTH: MICROSOFT_APP_PASSWORD/MicrosoftAppPassword is empty. Outbound calls will fail.")


# -----------------------
# REST calls (CLU + QA)
# -----------------------
async def call_clu(text: str) -> dict:
    if not LANGUAGE_ENDPOINT or not LANGUAGE_KEY:
        raise RuntimeError("Missing LANGUAGE_ENDPOINT or LANGUAGE_KEY.")

    url = f"{LANGUAGE_ENDPOINT}/language/:analyze-conversations?api-version={CLU_API_VERSION}"
    payload = {
        "kind": "Conversation",
        "analysisInput": {
            "conversationItem": {
                "id": "1",
                "text": text,
                "modality": "text",
                "language": CLU_LANGUAGE,
                "participantId": "user",
            }
        },
        "parameters": {
            "projectName": CLU_PROJECT_NAME,
            "deploymentName": CLU_DEPLOYMENT_NAME,
            "verbose": True,
            "stringIndexType": "TextElement_V8",
        },
    }
    headers = {
        "Ocp-Apim-Subscription-Key": LANGUAGE_KEY,
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers, timeout=20) as resp:
            data = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(f"CLU HTTP {resp.status}: {data}")
            return data


def _confidence_from_intents(top_intent: str, intents_obj: Any) -> float:
    if isinstance(intents_obj, dict):
        return float((intents_obj.get(top_intent) or {}).get("confidenceScore") or 0.0)

    if isinstance(intents_obj, list):
        for item in intents_obj:
            if not isinstance(item, dict):
                continue
            name = item.get("category") or item.get("intent") or item.get("name")
            if name == top_intent:
                return float(item.get("confidenceScore") or item.get("confidence") or 0.0)

    return 0.0


def _entities_to_string(entities_obj: Any) -> str:
    out = []
    if not isinstance(entities_obj, list):
        return ""

    for e in entities_obj:
        if not isinstance(e, dict):
            continue

        cat = e.get("category") or e.get("entity") or e.get("name")
        txt = e.get("text")

        if not txt:
            resolutions = e.get("resolutions")
            if isinstance(resolutions, list) and resolutions and isinstance(resolutions[0], dict):
                txt = resolutions[0].get("value")

        if cat and txt:
            out.append(f"{cat}={txt}")

    return ", ".join(out)


def parse_clu(clu_json: dict) -> Tuple[str, float, str]:
    prediction = (clu_json.get("result") or {}).get("prediction") or {}
    top_intent = prediction.get("topIntent") or prediction.get("top_intent") or "None"

    intents_obj = prediction.get("intents") or {}
    score = _confidence_from_intents(top_intent, intents_obj)

    entities_obj = prediction.get("entities") or []
    entities_str = _entities_to_string(entities_obj)

    return top_intent, score, entities_str


async def call_qa(question: str) -> dict:
    if not LANGUAGE_ENDPOINT or not LANGUAGE_KEY:
        raise RuntimeError("Missing LANGUAGE_ENDPOINT or LANGUAGE_KEY.")

    url = (
        f"{LANGUAGE_ENDPOINT}/language/:query-knowledgebases"
        f"?projectName={QA_PROJECT_NAME}"
        f"&deploymentName={QA_DEPLOYMENT_NAME}"
        f"&api-version={QA_API_VERSION}"
    )
    payload = {"question": question, "top": 3}
    headers = {
        "Ocp-Apim-Subscription-Key": LANGUAGE_KEY,
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers, timeout=20) as resp:
            data = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(f"QA HTTP {resp.status}: {data}")
            return data


def parse_qa(qa_json: dict) -> Tuple[Optional[str], float]:
    answers = qa_json.get("answers") or []
    if not answers or not isinstance(answers, list):
        return None, 0.0

    best = answers[0] if isinstance(answers[0], dict) else {}
    answer = best.get("answer")
    score = float(best.get("confidenceScore") or 0.0)
    return answer, score


# -----------------------
# Tool stubs
# -----------------------
async def tool_stub_reply(intent: str, entities_str: str) -> str:
    if intent == "ClaimStatus":
        return (
            f"I can help check claim status. Detected: {entities_str or 'no claim id yet'}.\n"
            f"What is your claim ID (e.g., CLM-10492)?"
        )
    if intent == "GetQuote":
        return (
            f"I can start a quote. Detected: {entities_str or 'missing details'}.\n"
            f"What’s the vehicle year/make/model and postal code?"
        )
    if intent == "UpdateContact":
        return (
            f"I can update contact details. Detected: {entities_str or 'missing details'}.\n"
            f"What would you like to update (phone/email/address)?"
        )
    if intent == "FileClaim":
        return "I can guide you through filing a claim. Is this auto or home, and when did it happen?"
    if intent == "Handoff":
        return "Sure — I can connect you to an agent. Before I do, what’s your policy number?"
    if intent == "Help":
        return "I can answer FAQs, check claim status, help file a claim, start a quote, or update contact details."
    return "Got it. How can I help?"


async def safe_send_text(turn_context: TurnContext, text: str) -> None:
    """
    Wraps send_activity with extra logging so Unauthorized errors show context.
    """
    activity = turn_context.activity
    try:
        await turn_context.send_activity(text)
    except ErrorResponseException as e:
        logger.error(
            "SEND_FAIL ErrorResponseException: status=%s msg=%s channel=%s serviceUrl=%s convId=%s recipient=%s from=%s",
            getattr(getattr(e, "response", None), "status", None),
            str(e),
            getattr(activity, "channel_id", None),
            getattr(activity, "service_url", None),
            getattr(getattr(activity, "conversation", None), "id", None),
            getattr(getattr(activity, "recipient", None), "id", None),
            getattr(getattr(activity, "from_property", None), "id", None),
        )
        raise
    except Exception as e:
        logger.exception(
            "SEND_FAIL Exception: %s channel=%s serviceUrl=%s convId=%s",
            e,
            getattr(activity, "channel_id", None),
            getattr(activity, "service_url", None),
            getattr(getattr(activity, "conversation", None), "id", None),
        )
        raise


# -----------------------
# Bot logic
# -----------------------
class SureCoverBot(ActivityHandler):
    async def on_message_activity(self, turn_context: TurnContext):
        activity = turn_context.activity
        user_text = (activity.text or "").strip()

        # Log inbound activity details (critical for diagnosing 401 send)
        logger.info(
            "ACTIVITY_IN type=%s channel=%s serviceUrl=%s convId=%s from=%s recipient=%s authHint=%s",
            getattr(activity, "type", None),
            getattr(activity, "channel_id", None),
            getattr(activity, "service_url", None),
            getattr(getattr(activity, "conversation", None), "id", None),
            getattr(getattr(activity, "from_property", None), "id", None),
            getattr(getattr(activity, "recipient", None), "id", None),
            # sometimes channel includes extra hints in channel_data
            "hasChannelData" if getattr(activity, "channel_data", None) else "noChannelData",
        )

        # Trust the serviceUrl before any outbound send.
        # If serviceUrl isn't trusted, SDK may not attach auth header -> 401 Unauthorized.
        try:
            if activity.service_url:
                MicrosoftAppCredentials.trust_service_url(activity.service_url)
                logger.info("BOT_AUTH trusted serviceUrl=%s", activity.service_url)
        except Exception as ex:
            logger.exception("BOT_AUTH failed to trust serviceUrl: %s", ex)

        if not user_text:
            await safe_send_text(turn_context, "I didn’t catch that—try typing a question.")
            return

        # 1) CLU
        try:
            clu_json = await call_clu(user_text)
            intent, intent_score, entities_str = parse_clu(clu_json)
            logger.info("CLU intent=%s score=%.3f entities=%s", intent, intent_score, entities_str)
        except Exception as ex:
            logger.exception("CLU failed: %s", ex)
            await safe_send_text(turn_context, "Sorry — I couldn’t analyze that message right now.")
            return

        # 2) Low-confidence
        if intent_score < INTENT_MIN_CONFIDENCE:
            await safe_send_text(
                turn_context,
                "I’m not fully sure what you need. Are you asking an FAQ, checking a claim, requesting a quote, or updating contact details?",
            )
            return

        # 3) FAQ + SmallTalk -> QA
        if intent in ("FAQ", "SmallTalk"):
            try:
                qa_json = await call_qa(user_text)
                answer, qa_score = parse_qa(qa_json)
                logger.info("QA intent=%s score=%.3f", intent, qa_score)
            except Exception as ex:
                logger.exception("QA failed: %s", ex)
                await safe_send_text(turn_context, "Sorry — I couldn’t reach the FAQ service right now.")
                return

            if answer and qa_score >= QA_MIN_CONFIDENCE:
                await safe_send_text(turn_context, answer)
                return

            if intent == "SmallTalk":
                await safe_send_text(turn_context, "Hi! How can I help with your insurance today?")
                return

            await safe_send_text(
                turn_context,
                "I couldn’t find a confident FAQ answer. Would you like me to search policy documents next (RAG) or connect you to an agent?",
            )
            return

        # 4) Other intents -> stub
        try:
            reply = await tool_stub_reply(intent, entities_str)
            await safe_send_text(turn_context, reply)
        except Exception as ex:
            logger.exception("Tool routing failed: %s", ex)
            await safe_send_text(turn_context, "Sorry — something went wrong while handling that request.")


# -----------------------
# FastAPI app
# -----------------------
app = FastAPI()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/debug/env")
def debug_env():
    """
    Confirms what the app sees (never returns secret values).
    """
    return {
        "LANGUAGE_ENDPOINT_set": bool(LANGUAGE_ENDPOINT),
        "LANGUAGE_KEY_set": bool(LANGUAGE_KEY),
        "CLU_PROJECT_NAME": CLU_PROJECT_NAME,
        "CLU_DEPLOYMENT_NAME": CLU_DEPLOYMENT_NAME,
        "CLU_LANGUAGE": CLU_LANGUAGE,
        "QA_PROJECT_NAME": QA_PROJECT_NAME,
        "QA_DEPLOYMENT_NAME": QA_DEPLOYMENT_NAME,
        "BOT_AUTH": {
            "app_id": MICROSOFT_APP_ID or "",
            "tenant_id": MICROSOFT_APP_TENANT_ID or "",
            "app_type": MICROSOFT_APP_TYPE or "",
            "password_present": bool(MICROSOFT_APP_PASSWORD),
            "password_masked": _mask(MICROSOFT_APP_PASSWORD) if MICROSOFT_APP_PASSWORD else "",
            "password_len": len(MICROSOFT_APP_PASSWORD) if MICROSOFT_APP_PASSWORD else 0,
        },
    }


@app.get("/debug/bot-token")
def debug_bot_token():
    """
    Attempts to acquire a Bot Framework token using the configured app id/secret.
    Returns token claims (no token string).
    """
    if not MICROSOFT_APP_ID or not MICROSOFT_APP_PASSWORD:
        return {
            "ok": False,
            "error": "Missing MICROSOFT_APP_ID/MICROSOFT_APP_PASSWORD (or MicrosoftAppId/MicrosoftAppPassword).",
        }

    creds = MicrosoftAppCredentials(MICROSOFT_APP_ID, MICROSOFT_APP_PASSWORD)
    try:
        token = creds.get_access_token()
        claims = _decode_jwt_claims_no_verify(token)
        # only return minimal claims
        return {
            "ok": True,
            "claims_subset": {
                "aud": claims.get("aud"),
                "iss": claims.get("iss"),
                "tid": claims.get("tid") or claims.get("tenant_id"),
                "appid": claims.get("appid") or claims.get("azp"),
                "exp": claims.get("exp"),
            },
        }
    except Exception as ex:
        logger.exception("BOT_AUTH token acquisition failed: %s", ex)
        return {"ok": False, "error": str(ex)}


# -----------------------
# Debug endpoints (existing)
# -----------------------
@app.get("/debug/clu")
async def debug_clu(q: str):
    clu_json = await call_clu(q)
    intent, score, entities = parse_clu(clu_json)
    return {
        "intent": intent,
        "score": score,
        "entities": entities,
        "language_sent": CLU_LANGUAGE,
        "project": CLU_PROJECT_NAME,
        "deployment": CLU_DEPLOYMENT_NAME,
    }


@app.get("/debug/qa")
async def debug_qa(q: str):
    qa_json = await call_qa(q)
    answer, score = parse_qa(qa_json)
    return {
        "answer": answer,
        "score": score,
        "project": QA_PROJECT_NAME,
        "deployment": QA_DEPLOYMENT_NAME,
    }


# -----------------------
# Bot Framework adapter
# -----------------------
adapter_settings = BotFrameworkAdapterSettings(MICROSOFT_APP_ID, MICROSOFT_APP_PASSWORD)
adapter = BotFrameworkAdapter(adapter_settings)
bot = SureCoverBot()


async def on_error(context: TurnContext, error: Exception):
    # Log the root error with max detail
    logger.exception("[on_turn_error] %s", error)

    # IMPORTANT: sending may also fail (and can recurse). So guard it.
    try:
        await safe_send_text(context, "Sorry, the bot hit an error.")
    except Exception as send_ex:
        logger.error("on_error: failed to send error message (likely auth issue): %s", send_ex)


adapter.on_turn_error = on_error


@app.post("/api/messages")
async def messages(req: Request):
    # Helpful inbound request logging
    auth_header = req.headers.get("Authorization", "")
    logger.info(
        "HTTP_IN /api/messages authHeaderPresent=%s authHeaderPrefix=%s",
        bool(auth_header),
        auth_header[:20] + "..." if auth_header else "",
    )

    body = await req.json()
    activity = Activity().deserialize(body)

    invoke_response = await adapter.process_activity(activity, auth_header, bot.on_turn)

    if invoke_response:
        content = invoke_response.body
        if isinstance(content, (dict, list)):
            content = json.dumps(content)
        return Response(content=content, status_code=invoke_response.status, media_type="application/json")

    return Response(status_code=201)
