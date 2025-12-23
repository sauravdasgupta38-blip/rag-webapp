import os
from fastapi import FastAPI, Request, Response

from botbuilder.core import (
    ActivityHandler,
    BotFrameworkAdapter,
    BotFrameworkAdapterSettings,
    TurnContext,
)
from botbuilder.schema import Activity


# ---------- Bot logic (what the bot does) ----------
class EchoBot(ActivityHandler):
    async def on_message_activity(self, turn_context: TurnContext):
        user_text = turn_context.activity.text or ""
        await turn_context.send_activity(f"Echo: {user_text}")


# ---------- FastAPI app ----------
app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}


# ---------- Bot Framework adapter (auth + message processing) ----------
APP_ID = os.getenv("MICROSOFT_APP_ID", "")
APP_PASSWORD = os.getenv("MICROSOFT_APP_PASSWORD", "")

adapter_settings = BotFrameworkAdapterSettings(APP_ID, APP_PASSWORD)
adapter = BotFrameworkAdapter(adapter_settings)

bot = EchoBot()


@adapter.on_turn_error
async def on_error(context: TurnContext, error: Exception):
    # Shows up in logs; also tells the user something went wrong
    print(f"[on_turn_error] {error}")
    await context.send_activity("Sorry — the bot hit an error.")


@app.post("/api/messages")
async def messages(req: Request):
    body = await req.json()
    activity = Activity().deserialize(body)
    auth_header = req.headers.get("Authorization", "")

    invoke_response = await adapter.process_activity(activity, auth_header, bot.on_turn)

    # If it was an "invoke" activity, we may need to return a body/status
    if invoke_response:
        return Response(
            content=invoke_response.body,
            status_code=invoke_response.status,
            media_type="application/json",
        )

    # Normal message activities: Bot Framework expects 201/200
    return Response(status_code=201)
