import os
import json
import base64
import asyncio
import logging
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request, Response

from botbuilder.core import (
    ActivityHandler,
    BotFrameworkAdapter,
    BotFrameworkAdapterSettings,
    TurnContext,
)
from botbuilder.schema import Activity
from botbuilder.schema._models_py3 import ErrorResponseException

# ---------------- Logging ----------------
LOG_LEVEL = (os.getenv("LOG_LEVEL") or "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("surecover-bot")

app = FastAPI()


# ---------------- Helpers ----------------
def _env_first(*names: str) -> Optional[str]:
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return v
    return None


def _mask(s: Optional[str], keep: int = 6) -> str:
    if not s:
        return "(empty)"
    s = str(s)
    if len(s) <= keep:
        return "*" * len(s)
    return s[:keep] + "…" + s[-2:]


def _decode_jwt_noverify(token: str) -> Dict[str, Any]:
    """Decode JWT payload without verifying signature (debug only)."""
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


def _safe_claims(claims: Dict[str, Any]) -> Dict[str, Any]:
    """Return a compact claim subset (avoid dumping long/PII-heavy stuff)."""
    keep_keys = ["iss", "aud", "appid", "azp", "tid", "ver", "scp", "roles", "idtyp"]
    out: Dict[str, Any] = {}
    for k in keep_keys:
        if k in claims:
            out[k] = claims[k]
    return out


async def _log_adapter_token_claims(adapter_obj: Any) -> None:
    """
    Best-effort: attempt to access the adapter's credentials and log the outbound
    connector token claims used to call the channel service (WebChat/DirectLine).
    """
    try:
        cred = None
        for attr in ["_credentials", "credentials", "_app_credentials"]:
            if hasattr(adapter_obj, attr):
                cred = getattr(adapter_obj, attr)
                if cred is not None:
                    break

        if cred is None or not hasattr(cred, "get_access_token"):
            logger.info("ADAPTER_TOKEN: cannot access adapter credentials/get_access_token()")
            return

        tok = cred.get_access_token()  # sometimes returns coroutine
        if asyncio.iscoroutine(tok):
            tok = await tok
        claims = _decode_jwt_noverify(str(tok))
        logger.info("ADAPTER_TOKEN_CLAIMS %s", json.dumps(_safe_claims(claims), ensure_ascii=False))
    except Exception as e:
        logger.warning("ADAPTER_TOKEN_CLAIMS failed: %s", e)


# ---------------- Bot credentials (support both naming styles) ----------------
MICROSOFT_APP_ID = _env_first("MICROSOFT_APP_ID", "MicrosoftAppId")
MICROSOFT_APP_PASSWORD = _env_first("MICROSOFT_APP_PASSWORD", "MicrosoftAppPassword")
MICROSOFT_APP_TENANT_ID = _env_first("MicrosoftAppTenantId", "MICROSOFT_APP_TENANT_ID")
MICROSOFT_APP_TYPE = (_env_first("MicrosoftAppType", "MICROSOFT_APP_TYPE") or "MultiTenant").strip()

APP_ID = MICROSOFT_APP_ID or ""
APP_PASSWORD = MICROSOFT_APP_PASSWORD or ""

logger.info(
    "BOT_AUTH_CONFIG appId=%s tenantId=%s appType=%s appSecretPresent=%s",
    _mask(APP_ID, 8),
    _mask(MICROSOFT_APP_TENANT_ID, 8),
    MICROSOFT_APP_TYPE,
    bool(APP_PASSWORD),
)

# Force tenant-specific OAuth endpoint for SingleTenant to avoid "Bot Framework" tenant confusion.
oauth_endpoint = None
openid_metadata = None
tenant = None

if MICROSOFT_APP_TYPE.lower() == "singletenant" and MICROSOFT_APP_TENANT_ID:
    tenant = MICROSOFT_APP_TENANT_ID.strip()
    oauth_endpoint = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    openid_metadata = f"https://login.microsoftonline.com/{tenant}/v2.0/.well-known/openid-configuration"
    logger.info("BOT_AUTH_CONFIG using tenant oauth_endpoint=%s", oauth_endpoint)
else:
    logger.info("BOT_AUTH_CONFIG using SDK default oauth endpoint (not forcing tenant)")

# Try to set global OAuth endpoint in connector credentials (varies by SDK version).
try:
    from botframework.connector.auth import AppCredentials, MicrosoftAppCredentials  # type: ignore

    if oauth_endpoint and hasattr(AppCredentials, "oauth_endpoint"):
        AppCredentials.oauth_endpoint = oauth_endpoint
    if oauth_endpoint and hasattr(MicrosoftAppCredentials, "oauth_endpoint"):
        MicrosoftAppCredentials.oauth_endpoint = oauth_endpoint

    # Some versions support tenant on credentials:
    if tenant and hasattr(MicrosoftAppCredentials, "tenant_id"):
        MicrosoftAppCredentials.tenant_id = tenant  # type: ignore

except Exception as e:
    logger.warning("BOT_AUTH_CONFIG could not set global credential oauth endpoint: %s", e)


# ---------------- Adapter ----------------
adapter_settings = BotFrameworkAdapterSettings(APP_ID, APP_PASSWORD)

# Also set endpoints on adapter settings if supported by this SDK version.
if oauth_endpoint:
    for attr, val in [("oauth_endpoint", oauth_endpoint), ("open_id_metadata", openid_metadata)]:
        try:
            if hasattr(adapter_settings, attr):
                setattr(adapter_settings, attr, val)
        except Exception:
            pass
    try:
        if tenant and hasattr(adapter_settings, "channel_auth_tenant"):
            setattr(adapter_settings, "channel_auth_tenant", tenant)
    except Exception:
        pass

adapter = BotFrameworkAdapter(adapter_settings)


# ---------------- Bot logic ----------------
class DiagEchoBot(ActivityHandler):
    async def on_message_activity(self, turn_context: TurnContext):
        text = (turn_context.activity.text or "").strip()
        logger.info("BOT_MSG_IN text=%s", _mask(text, 30))
        # This is the moment that currently fails for you (401 Unauthorized).
        await safe_send_text(turn_context, f"Echo: {text or '(empty)'}")


bot = DiagEchoBot()


# ---------------- Safer send wrapper (adds token diagnostics on failure) ----------------
async def safe_send_text(turn_context: TurnContext, text: str) -> None:
    try:
        await turn_context.send_activity(text)
        logger.info("SEND_OK channel=%s serviceUrl=%s", turn_context.activity.channel_id, turn_context.activity.service_url)
    except ErrorResponseException as e:
        # Pull adapter token claims so we can see if aud/tid look correct for api.botframework.com
        await _log_adapter_token_claims(turn_context.adapter)
        logger.error(
            "SEND_FAIL ErrorResponseException: msg=%s channel=%s serviceUrl=%s convId=%s",
            str(e),
            turn_context.activity.channel_id,
            turn_context.activity.service_url,
            getattr(turn_context.activity.conversation, "id", None),
        )
        raise
    except Exception as e:
        await _log_adapter_token_claims(turn_context.adapter)
        logger.error("SEND_FAIL_UNK %s", e)
        raise


@adapter.on_turn_error
async def on_error(context: TurnContext, error: Exception):
    logger.error("[on_turn_error] %s", error)
    # If auth is broken, even this will fail — so we swallow.
    try:
        await safe_send_text(context, "Sorry — the bot hit an error.")
    except Exception as e:
        logger.error("on_error: failed to send error message (likely auth issue): %s", e)


# ---------------- Routes ----------------
@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/debug/token")
async def debug_token():
    """
    Returns *claims only* for the outbound connector token (no secrets, no raw token).
    This helps confirm audience/tenant quickly.
    """
    try:
        # Try to access adapter credentials token (best-effort)
        await _log_adapter_token_claims(adapter)
        # Also return config summary
        return {
            "appIdMasked": _mask(APP_ID, 8),
            "tenantIdMasked": _mask(MICROSOFT_APP_TENANT_ID, 8),
            "appType": MICROSOFT_APP_TYPE,
            "forcedOauthEndpoint": oauth_endpoint or "(sdk-default)",
            "note": "Check Log Stream for ADAPTER_TOKEN_CLAIMS line.",
        }
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/messages")
async def messages(req: Request) -> Response:
    auth_header = req.headers.get("Authorization", "")

    logger.info(
        "HTTP_IN /api/messages authHeaderPresent=%s authHeaderPrefix=%s",
        bool(auth_header),
        (auth_header[:30] + "…") if auth_header else "(none)",
    )

    # Decode inbound JWT claims (no signature verify) to confirm audience/issuer/tenant
    try:
        if auth_header.lower().startswith("bearer "):
            token = auth_header.split(" ", 1)[1]
            claims = _decode_jwt_noverify(token)
            logger.info("AUTH_IN_CLAIMS %s", json.dumps(_safe_claims(claims), ensure_ascii=False))
    except Exception:
        pass

    body = await req.json()
    activity = Activity().deserialize(body)

    # Important: pass the incoming auth header through to adapter
    invoke_response = await adapter.process_activity(activity, auth_header, bot.on_turn)

    if invoke_response:
        return Response(
            content=json.dumps(invoke_response.body),
            status_code=invoke_response.status,
            media_type="application/json",
        )
    return Response(status_code=200)
