import os
import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
from chat import get_agent
from tools import Dependencies
from auth import AlationAuth

app = FastAPI()

# Set up agent and dependencies once at startup
al_username = os.getenv("ALATION_USERNAME")
al_password = os.getenv("ALATION_PASSWORD")
al_base_url = os.getenv("ALATION_BASE_URL")
al_auth = AlationAuth(al_username, al_password, al_base_url)
session = al_auth.get_authenticated_session()
agent = get_agent(model_provider="bedrock", model_name="us.anthropic.claude-3-5-sonnet-20241022-v2:0")
deps = Dependencies(session=session, al_base_url=al_base_url)

class ChatRequest(BaseModel):
    message: str
    history: list = []

@app.post("/chat")
async def chat_endpoint(req: ChatRequest):
    async with agent.run_mcp_servers():
        result = await agent.run(
            user_prompt=req.message,
            message_history=req.history,
            deps=deps,
        )
    return {"response": result.output, "history": result.new_messages()}

import json

def to_serializable(obj):
    """Recursively convert an object to something JSON serializable."""
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    if isinstance(obj, dict):
        return {k: to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_serializable(i) for i in obj]
    # Try __dict__ or vars()
    if hasattr(obj, "__dict__"):
        return to_serializable(vars(obj))
    if hasattr(obj, "_asdict"):  # namedtuple
        return to_serializable(obj._asdict())
    return str(obj)  # fallback

@app.post("/chat/stream")
async def chat_stream_endpoint(req: ChatRequest):
    async def streamer():
        async with agent.run_mcp_servers():
            result = await agent.run(
                user_prompt=req.message,
                message_history=req.history,
                deps=deps,
            )
            # Stream the output line by line
            for line in result.output.splitlines():
                yield line + "\n"
            # At the end, send the updated history as JSON
            history = [to_serializable(msg) for msg in result.new_messages()]
            yield f"\n__HISTORY__{json.dumps({'history': history})}\n"
    return StreamingResponse(streamer(), media_type="text/plain")
# To run: poetry run uvicorn chat_api:app --reload