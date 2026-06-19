"""
AI Interview Coach — Python FastAPI Backend
- SSE relay: n8n pushes events here → we stream to browser
- Proxy endpoints: browser → here → n8n webhooks
"""

import asyncio
import json
import os
import httpx
from collections import defaultdict
from typing import AsyncGenerator

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

# ── Load environment variables ──
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ Loaded .env file")
except ImportError:
    print("⚠️ python-dotenv not installed. Using environment variables or defaults.")

app = FastAPI(title="AI Interview Coach API", version="1.0.0")

# ── CORS ──────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", os.getenv("FRONTEND_URL", "*")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Config ────────────────────────────────────────────────────────────────────
# Use environment variable or fallback to hardcoded URL
N8N_BASE = os.getenv("N8N_BASE_URL", "https://mykhann.app.n8n.cloud/webhook")
print(f"🔗 N8N_BASE: {N8N_BASE}")

# ── SSE Event Store ───────────────────────────────────────────────────────────
# sessionId → asyncio.Queue of event dicts
_queues: dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
# sessionId → list of past events (for reconnect replay)
_history: dict[str, list] = defaultdict(list)


# ── Pydantic models ───────────────────────────────────────────────────────────
class UploadCVRequest(BaseModel):
    sessionId: str | None = None
    firstName: str
    fileBase64: str
    fileName: str = "cv.pdf"


class SetJobRequest(BaseModel):
    sessionId: str
    jobTitle: str
    jobDescription: str
    email: str = ""


class StartInterviewRequest(BaseModel):
    sessionId: str
    email: str = ""


class AnswerRequest(BaseModel):
    sessionId: str
    answer: str


# ── SSE helpers ───────────────────────────────────────────────────────────────
def _format_sse(event: str, data: dict) -> str:
    payload = json.dumps(data)
    return f"event: {event}\ndata: {payload}\n\n"


async def _sse_generator(session_id: str) -> AsyncGenerator[str, None]:
    """Stream SSE events to the browser for a given session."""
    q = _queues[session_id]

    # Replay missed events on reconnect
    for past in _history.get(session_id, []):
        yield _format_sse(past["event"], past["data"])

    # Heartbeat + live events
    while True:
        try:
            item = await asyncio.wait_for(q.get(), timeout=25)
            if item is None:          # sentinel: session done
                yield _format_sse("done", {"message": "Session complete"})
                break
            yield _format_sse(item["event"], item["data"])
        except asyncio.TimeoutError:
            yield ": heartbeat\n\n"   # keep connection alive


# ── SSE receive from n8n ──────────────────────────────────────────────────────
@app.post("/push/{session_id}")
async def push_event(session_id: str, request: Request):
    """
    n8n calls this to push an SSE event to the client.
    Body: { "event": "cv_result", "sessionId": "...", "data": {...} }
    """
    try:
        body = await request.json()
    except json.JSONDecodeError:
        # Handle case where body might not be JSON
        body = await request.body()
        try:
            body = json.loads(body)
        except:
            return {"ok": False, "error": "Invalid JSON"}
    
    event_name = body.get("event", "message")
    data = body.get("data", body)

    item = {"event": event_name, "data": data}
    _history[session_id].append(item)
    await _queues[session_id].put(item)

    # Close stream if report is ready
    if event_name == "report_ready":
        await asyncio.sleep(0.5)
        await _queues[session_id].put(None)

    return {"ok": True}


# ── SSE stream to browser ─────────────────────────────────────────────────────
@app.get("/stream/{session_id}")
async def stream_events(session_id: str):
    """Browser subscribes here to receive live SSE events."""
    return StreamingResponse(
        _sse_generator(session_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# ── Proxy to n8n webhooks ─────────────────────────────────────────────────────
async def _post_n8n(path: str, payload: dict) -> dict:
    """Send a POST request to n8n webhook with better error handling."""
    url = f"{N8N_BASE}/{path}"
    
    # Debug logging
    print(f"🔗 Calling n8n webhook: {url}")
    print(f"📦 Payload keys: {list(payload.keys())}")
    
    async with httpx.AsyncClient(timeout=120) as client:
        try:
            r = await client.post(url, json=payload)
            
            # Debug response
            print(f"📊 Response status: {r.status_code}")
            print(f"📄 Response preview: {r.text[:200] if r.text else '(empty)'}")
            
            # Check if response is empty
            if not r.text or r.text.strip() == "":
                print("⚠️ Empty response from n8n")
                return {"ok": True, "message": "Webhook processed successfully", "sessionId": payload.get("sessionId")}
            
            # Try to parse as JSON
            try:
                return r.json()
            except json.JSONDecodeError as e:
                print(f"⚠️ Response is not valid JSON: {e}")
                print(f"Raw response: {r.text[:500]}")
                # Return a default response
                return {
                    "ok": True, 
                    "message": "Webhook processed (non-JSON response)",
                    "raw_response": r.text[:500],
                    "sessionId": payload.get("sessionId")
                }
                
        except httpx.HTTPStatusError as e:
            print(f"❌ HTTP Error: {e.response.status_code}")
            print(f"Response: {e.response.text[:500] if e.response.text else '(empty)'}")
            raise HTTPException(
                status_code=e.response.status_code,
                detail=f"n8n webhook error: {e.response.text[:200] if e.response.text else 'No response body'}"
            )
        except httpx.ConnectError as e:
            print(f"❌ Connection Error: {e}")
            raise HTTPException(
                status_code=503,
                detail=f"Could not connect to n8n at {N8N_BASE}. Is n8n running?"
            )
        except Exception as e:
            print(f"❌ Unexpected error: {type(e).__name__}: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Error communicating with n8n: {str(e)}"
            )


@app.post("/api/upload-cv")
async def upload_cv(body: UploadCVRequest):
    """Receive CV from browser, forward to n8n."""
    payload = body.model_dump()
    if not payload.get("sessionId"):
        import time
        payload["sessionId"] = f"session_{int(time.time() * 1000)}"
    
    print(f"📤 Upload CV for session: {payload['sessionId']}")
    
    try:
        result = await _post_n8n("upload-cv", payload)
        return {**result, "sessionId": payload["sessionId"]}
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Upload CV error: {e}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@app.post("/api/set-job")
async def set_job(body: SetJobRequest):
    """Forward job details to n8n."""
    print(f"📤 Set job for session: {body.sessionId}")
    try:
        result = await _post_n8n("set-job", body.model_dump())
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Set job error: {e}")
        raise HTTPException(status_code=500, detail=f"Set job failed: {str(e)}")


@app.post("/api/start-interview")
async def start_interview(body: StartInterviewRequest):
    """Start the interview process."""
    print(f"📤 Start interview for session: {body.sessionId}")
    try:
        result = await _post_n8n("start-interview", body.model_dump())
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Start interview error: {e}")
        raise HTTPException(status_code=500, detail=f"Start interview failed: {str(e)}")


@app.post("/api/answer")
async def submit_answer(body: AnswerRequest):
    """Submit an answer for the current question."""
    print(f"📤 Submit answer for session: {body.sessionId}")
    try:
        result = await _post_n8n("answer", body.model_dump())
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Submit answer error: {e}")
        raise HTTPException(status_code=500, detail=f"Submit answer failed: {str(e)}")


@app.get("/api/session-state")
async def session_state(sessionId: str):
    """Get the current state of a session."""
    print(f"📤 Get session state: {sessionId}")
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            url = f"{N8N_BASE}/session-state"
            r = await client.get(url, params={"sessionId": sessionId})
            
            if not r.text or r.text.strip() == "":
                return {"ok": True, "sessionId": sessionId, "state": "unknown"}
            
            try:
                return r.json()
            except json.JSONDecodeError:
                return {"ok": True, "sessionId": sessionId, "state": "unknown", "raw_response": r.text[:200]}
    except Exception as e:
        print(f"❌ Session state error: {e}")
        return {"ok": False, "sessionId": sessionId, "error": str(e)}


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "ok", 
        "n8n_base": N8N_BASE,
        "env_loaded": bool(os.getenv("N8N_BASE_URL"))
    }


# ── Root endpoint ─────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "message": "AI Interview Coach API",
        "version": "1.0.0",
        "n8n_base": N8N_BASE,
        "endpoints": [
            "POST /api/upload-cv",
            "POST /api/set-job", 
            "POST /api/start-interview",
            "POST /api/answer",
            "GET /api/session-state",
            "GET /stream/{session_id}",
            "POST /push/{session_id}",
            "GET /health"
        ]
    }