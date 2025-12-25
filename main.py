import os
import json
import logging
from typing import Tuple, Optional, Any

import aiohttp
from fastapi import FastAPI, Request, Response

from botbuilder.core import (
    ActivityHandler,
    BotFrameworkAdapter,
    BotFrameworkAdapterSettings,
    TurnContext,
)
from botbuilder.schema import Activity


# ======================
# Optional: .env support
# ======================
# Works locally if you have python-dotenv installed and a .env file.
# Safe on Azure: App Service injects env vars; if no .env exists, this is a no-op.
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv(override=False)
except Exception:
    pass


# -----------------------
# Logging
# -----------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("surecover-bot")


# -----------------------
# Azure AI Language config
# -----------------------
LANGUAGE_ENDPOINT = os.getenv("LANGUAGE_ENDPOINT", "").rstrip("/")
LANGUAGE_KEY = os.getenv("LANGUAGE_KEY", "")

CLU_PROJECT_NAME = os.getenv("CLU_PROJECT_NAME", "surecover-clu")
CLU_DEPLOYMENT_NAME = os.getenv("CLU_DEPLOYMENT_NAME", "surecover_dep")
# You said your project supports en-gb, so we keep that as default:
CLU_LANGUAGE = os.getenv("CLU_LANGUAGE", "en-gb")

QA_PROJECT_NAME = os.getenv("QA_PROJECT_NAME", "surecover-qa")
QA_DEPLOYMENT_NAME = os.getenv("QA_DEPLOYMENT_NAME", "production")

# Match what you tested
CLU_API_VERSION = os.getenv("CLU_API_VERSION", "2024-11-15-preview")
QA_API_VERSION = os.getenv("QA_API_VERSION", "2021-10-01")

# Thresholds (tune later)
INTENT_MIN_CONFIDENCE = float(os.getenv("INTENT_MIN_CONFIDENCE", "0.55"))
QA_MIN_CONFIDENCE = float(os.getenv("QA_MIN_CONFIDENCE", "0.45"))


# -----------------------
# REST calls (CLU + QA)
# -----------------------
async def call_clu(text: str) -> dict:
    """
    POST {LANGUAGE_ENDPOINT}/language/:analyze-conversations?api-version=...
    """
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
    """
    Handles both shapes:
      - dict: {"FAQ": {"confidenceScore": 0.9}, ...}
      - list: [{"category":"FAQ","confidenceScore":0.9}, ...]
    """
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
    """
    Best-effort entity extraction across common response shapes.
    """
    out = []
    if not isinstance(entities_obj, list):
        return ""

    for e in entities_obj:
        if not isinstance(e, dict):
            continue

        # Common fields
        cat = e.get("category") or e.get("entity") or e.get("name")
        txt = e.get("text")

        # Sometimes entity text may be in a nested structure
        if not txt:
            # e.g. resolutions: [{"value":"..."}]
            resolutions = e.get("resolutions")
            if isinstance(resolutions, list) and resolutions and isinstance(resolutions[0], dict):
                txt = resolutions[0].get("value")

        if cat and txt:
            out.append(f"{cat}={txt}")

        # Some versions return extractions like: "extraInformation": [{"key":..., "value":...}]
        # We keep it simple here.

    return ", ".join(out)


def parse_clu(clu_json: dict) -> Tuple[str, float, str]:
    """
    Robust parser for CLU response.
    Returns: (top_intent, confidence, entities_str)
    """
    prediction = (clu_json.get("result") or {}).get("prediction") or {}
    top_intent = prediction.get("topIntent") or prediction.get("top_intent") or "None"

    intents_obj = prediction.get("intents") or {}
    score = _confidence_from_intents(top_intent, intents_obj)

    entities_obj = prediction.get("entities") or []
    entities_str = _entities_to_string(entities_obj)

    return top_intent, score, entities_str


async def call_qa(question: str) -> dict:
    """
    POST {LANGUAGE_ENDPOINT}/language/:query-knowledgebases
      ?projectName=...&deploymentName=...&api-version=...
    """
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
    """
    Returns: (best_answer_text_or_none, best_confidenceScore)
    """
    answers = qa_json.get("answers") or []
    if not answers or not isinstance(answers, list):
        return None, 0.0

    best = answers[0] if isinstance(answers[0], dict) else {}
    answer = best.get("answer")
    score = float(best.get("confidenceScore") or 0.0)
    return answer, score


# -----------------------
# Tool stubs (replace later with real APIs)
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
    # SmallTalk is handled via QA (chitchat). If QA fails, we fallback in the router.
    return "Got it. How can I help?"


# -----------------------
# Bot logic (CLU -> route; FAQ + SmallTalk -> QA)
# -----------------------
class SureCoverBot(ActivityHandler):
    async def on_message_activity(self, turn_context: TurnContext):
        user_text = (turn_context.activity.text or "").strip()
        if not user_text:
            await turn_context.send_activity("I didn’t catch that—try typing a question.")
            return

        # 1) CLU (intent + entities)
        try:
            clu_json = await call_clu(user_text)
            intent, intent_score, entities_str = parse_clu(clu_json)
            logger.info("CLU intent=%s score=%.3f entities=%s", intent, intent_score, entities_str)
        except Exception as ex:
            logger.exception("CLU failed: %s", ex)
            await turn_context.send_activity("Sorry — I couldn’t analyze that message right now.")
            return

        # 2) Low-confidence: ask clarifying question
        if intent_score < INTENT_MIN_CONFIDENCE:
            await turn_context.send_activity(
                "I’m not fully sure what you need. Are you asking an FAQ, checking a claim, requesting a quote, or updating contact details?"
            )
            return

        # 3) Route: FAQ + SmallTalk -> Question Answering (SmallTalk uses Chitchat)
        if intent in ("FAQ", "SmallTalk"):
            try:
                qa_json = await call_qa(user_text)
                answer, qa_score = parse_qa(qa_json)
                logger.info("QA intent=%s score=%.3f", intent, qa_score)
            except Exception as ex:
                logger.exception("QA failed: %s", ex)
                await turn_context.send_activity("Sorry — I couldn’t reach the FAQ service right now.")
                return

            if answer and qa_score >= QA_MIN_CONFIDENCE:
                await turn_context.send_activity(answer)
                return

            # If SmallTalk + QA didn't answer confidently, respond politely anyway
            if intent == "SmallTalk":
                await turn_context.send_activity("Hi! How can I help with your insurance today?")
                return

            # FAQ low-confidence fallback
            await turn_context.send_activity(
                "I couldn’t find a confident FAQ answer. Would you like me to search policy documents next (RAG) or connect you to an agent?"
            )
            return

        # 4) Other intents -> tool stubs (replace later)
        try:
            reply = await tool_stub_reply(intent, entities_str)
            await turn_context.send_activity(reply)
        except Exception as ex:
            logger.exception("Tool routing failed: %s", ex)
            await turn_context.send_activity("Sorry — something went wrong while handling that request.")


# -----------------------
# FastAPI app
# -----------------------
app = FastAPI()


@app.get("/health")
def health():
    return {"status": "ok"}


# -----------------------
# Debug endpoints (fast local tests)
# -----------------------
@app.get("/debug/clu")
async def debug_clu(q: str):
    """
    Quick test for CLU intent + entities.

    Example:
      /debug/clu?q=What%27s%20the%20status%20of%20claim%20CLM-10492
    """
    clu_json = await call_clu(q)
    intent, score, entities = parse_clu(clu_json)
    return {
        "intent": intent,
        "score": score,
        "entities": entities,
        "language_sent": CLU_LANGUAGE,
        "project": CLU_PROJECT_NAME,
        "deployment": CLU_DEPLOYMENT_NAME,
        "raw": clu_json,
    }


@app.get("/debug/qa")
async def debug_qa(q: str):
    """
    Quick test for Question Answering.

    Example:
      /debug/qa?q=What%20is%20a%20deductible%3F
    """
    qa_json = await call_qa(q)
    answer, score = parse_qa(qa_json)
    return {
        "answer": answer,
        "score": score,
        "project": QA_PROJECT_NAME,
        "deployment": QA_DEPLOYMENT_NAME,
        "raw": qa_json,
    }


# -----------------------
# Bot Framework adapter
# -----------------------
MICROSOFT_APP_ID = os.getenv("MICROSOFT_APP_ID", "")
MICROSOFT_APP_PASSWORD = os.getenv("MICROSOFT_APP_PASSWORD", "")

adapter_settings = BotFrameworkAdapterSettings(MICROSOFT_APP_ID, MICROSOFT_APP_PASSWORD)
adapter = BotFrameworkAdapter(adapter_settings)

bot = SureCoverBot()


async def on_error(context: TurnContext, error: Exception):
    logger.exception("[on_turn_error] %s", error)
    await context.send_activity("Sorry, the bot hit an error.")


adapter.on_turn_error = on_error


@app.post("/api/messages")
async def messages(req: Request):
    body = await req.json()
    activity = Activity().deserialize(body)
    auth_header = req.headers.get("Authorization", "")

    invoke_response = await adapter.process_activity(activity, auth_header, bot.on_turn)

    # Invoke activities may require a body + status returned.
    if invoke_response:
        content = invoke_response.body
        if isinstance(content, (dict, list)):
            content = json.dumps(content)
        return Response(content=content, status_code=invoke_response.status, media_type="application/json")

    # For normal message activities, empty 201 is fine.
    return Response(status_code=201)
