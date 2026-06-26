# Public Demo
from fastapi import FastAPI, Depends, Header, HTTPException, status
from pydantic import BaseModel
from uuid import uuid4

app = FastAPI(title="ZeroGate Public Sandbox Gateway")

demo_db = {}

class InferenceRequest(BaseModel):
    model: str
    prompt: str

async def verify_demo_key(x_zerogate_key: str = Header(None)):
    if x_zerogate_key != "zerogate-alpha-demo":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, 
            detail="Invalid Demo Key"
        )
    return x_zerogate_key

@app.post("/v1/compute", status_code=status.HTTP_202_ACCEPTED)
async def demo_compute(payload: InferenceRequest, x_key: str = Depends(verify_demo_key)):
    request_id = str(uuid4())
    
    # Store the user's prompt and model context locally in memory
    demo_db[request_id] = {
        "prompt": payload.prompt,
        "model": payload.model
    }
    
    return {
        "status": "accepted",
        "request_id": request_id,
        "polling_check_url": f"https://api.zerogate.cloud/v1/status/{request_id}",
        "message": f"Payload enqueued. Check results by running: curl -X GET https://api.zerogate.cloud/v1/status/{request_id}"
    }

@app.get("/v1/status/{request_id}")
async def demo_status(request_id: str):
    # Fetch user data, fallback gracefully if they call GET directly without POSTing
    cached = demo_db.get(request_id, {
        "prompt": "Why keeping high-end NVIDIA GPUs running idle 24/7 is a multi-million dollar mistake.",
        "model": "unsloth/Meta-Llama-3.1-8B-Instruct"
    })
    
    return {
        "request_id": request_id,
        "status": "completed",
        "metrics": {
            "execution_duration_seconds": 4.501172,
            "estimated_savings_usd": 0.002476
        },
        "prompt": cached["prompt"],
        "result": f"Mock engine output verified for model context: {cached['model']}",
        "message": "Inference lifecycle finished. Idle footprint flattened."
    }
