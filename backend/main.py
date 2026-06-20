"""
AI Interview Coach — Python FastAPI Backend
- SSE relay: n8n pushes events here → we stream to browser
- Proxy endpoints: browser → here → n8n webhooks
"""

import asyncio
import json
import os
import httpx
import time
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
    allow_origins=["http://localhost:3000", "http://localhost:3001", os.getenv("FRONTEND_URL", "*")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Config ────────────────────────────────────────────────────────────────────
N8N_BASE = os.getenv("N8N_BASE_URL", "https://mykhann.app.n8n.cloud/webhook")
print(f"🔗 N8N_BASE: {N8N_BASE}")

# ── SSE Event Store ───────────────────────────────────────────────────────────
_queues: dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
_sent_events: dict[str, set] = defaultdict(set)  # Track sent events to prevent duplicates
_session_state: dict[str, dict] = defaultdict(dict)  # Track current state per session
_latest_events: dict[str, dict] = defaultdict(dict)  # Store latest event per type


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


def _generate_event_id(session_id: str, event_name: str, data: dict) -> str:
    """Generate a unique ID for each event to prevent duplicates."""
    question_num = data.get('questionNumber', data.get('questionId', ''))
    timestamp = data.get('timestamp', time.time())
    return f"{session_id}_{event_name}_{question_num}_{timestamp}"


async def _sse_generator(session_id: str) -> AsyncGenerator[str, None]:
    """Stream SSE events to the browser for a given session."""
    q = _queues[session_id]
    
    # 🔥 Don't replay old events - send only current state
    state = _session_state.get(session_id, {})
    if state:
        yield _format_sse("sync", {
            "step": state.get("step", "upload"),
            "questionNumber": state.get("questionNumber", 0),
            "totalQuestions": state.get("totalQuestions", 0)
        })

    # Live events
    while True:
        try:
            item = await asyncio.wait_for(q.get(), timeout=25)
            if item is None:  # sentinel: session done
                yield _format_sse("done", {"message": "Session complete"})
                break
            
            event_name = item["event"]
            data = item["data"]
            
            # Generate unique ID
            event_id = _generate_event_id(session_id, event_name, data)
            
            # 🔥 Skip if already sent
            if event_id in _sent_events[session_id]:
                print(f"⏭️ Skipping duplicate event: {event_name}")
                continue
            
            # Mark as sent
            _sent_events[session_id].add(event_id)
            
            # Keep sent events set manageable
            if len(_sent_events[session_id]) > 100:
                _sent_events[session_id] = set(list(_sent_events[session_id])[-50:])
            
            # 🔥 Update session state
            if event_name == "cv_result":
                _session_state[session_id]["step"] = "set_job"
            elif event_name == "job_match":
                _session_state[session_id]["step"] = "ready"
            elif event_name == "question":
                _session_state[session_id]["step"] = "interview"
                _session_state[session_id]["questionNumber"] = data.get("questionNumber", 0)
                _session_state[session_id]["totalQuestions"] = data.get("totalQuestions", 0)
            elif event_name == "report_ready":
                _session_state[session_id]["step"] = "done"
            
            yield _format_sse(event_name, data)
            
        except asyncio.TimeoutError:
            yield ": heartbeat\n\n"  # keep connection alive


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
        body = await request.body()
        try:
            body = json.loads(body)
        except:
            return {"ok": False, "error": "Invalid JSON"}
    
    event_name = body.get("event", "message")
    data = body.get("data", body)

    # Ensure sessionId is in the data
    if 'sessionId' not in data:
        data['sessionId'] = session_id
    
    # Add timestamp to make each event unique
    data['timestamp'] = str(time.time())

    item = {"event": event_name, "data": data}
    
    # Store latest event per type
    _latest_events[session_id][event_name] = data
    
    # 🔥 Clear old queue entries to prevent backlog
    if _queues[session_id].qsize() > 5:
        while not _queues[session_id].empty():
            try:
                _queues[session_id].get_nowait()
            except:
                break
    
    # Add small delay for question events
    if event_name == "question":
        await asyncio.sleep(0.3)
    
    await _queues[session_id].put(item)

    # Send sentinel to close stream when done
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
    
    print(f"🔗 Calling n8n webhook: {url}")
    print(f"📦 Payload keys: {list(payload.keys())}")
    
    async with httpx.AsyncClient(timeout=120) as client:
        try:
            r = await client.post(url, json=payload)
            
            print(f"📊 Response status: {r.status_code}")
            print(f"📄 Response preview: {r.text[:200] if r.text else '(empty)'}")
            
            if not r.text or r.text.strip() == "":
                print("⚠️ Empty response from n8n")
                return {"ok": True, "message": "Webhook processed successfully", "sessionId": payload.get("sessionId")}
            
            try:
                return r.json()
            except json.JSONDecodeError as e:
                print(f"⚠️ Response is not valid JSON: {e}")
                return {
                    "ok": True, 
                    "message": "Webhook processed (non-JSON response)",
                    "raw_response": r.text[:500],
                    "sessionId": payload.get("sessionId")
                }
                
        except httpx.HTTPStatusError as e:
            print(f"❌ HTTP Error: {e.response.status_code}")
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