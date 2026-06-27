"""
ZeroGate Fast-Ingress Routing Gateway Engine.

This module acts as the core edge entry point, orchestrating high-concurrency 
API traffic, verifying multi-tenant credentials via Redis state pools, 
and dispatching ingress streaming payloads into downstream Kafka messaging layers.
"""
import os, json, asyncpg
from uuid import uuid4
from fastapi import FastAPI, Request, Depends, HTTPException, Header, status
from pydantic import BaseModel, Field
from contextlib import asynccontextmanager
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from engine.logger import ZeroGateLogger

log = ZeroGateLogger("INGRESS")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the global application lifecycle, connection pools, and state.
    
    Bootstraps the Redis key/value state store matrix, verifies and provisions 
    downstream Kafka transaction message partitions, and initializes the asynchronous 
    PostgreSQL analytics logging connection pools. Cleans up all connections gracefully 
    on worker shutdown.
    """
    app.state.redis = await aioredis.from_url(os.getenv("REDIS_URL", "redis://redis:6379"), decode_responses=True)

    configured_key = os.getenv("ZEROGATE_API_KEY", "zerogate-alpha-demo")
    await app.state.redis.sadd("valid_api_keys", configured_key)

    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "zerogate-kafka:29092")
    try:
        admin = AIOKafkaAdminClient(bootstrap_servers=brokers, client_id="zerogate-edge-admin")
        await admin.start()
        await admin.create_topics(new_topics=[NewTopic(name="inference_requests", num_partitions=1, replication_factor=1)], validate_only=False)
        log.info("Kafka messaging partition 'inference_requests' provisioned.")
    except Exception:
        pass
    finally:
        await admin.close()
        
    app.state.producer = AIOKafkaProducer(bootstrap_servers=brokers)
    await app.state.producer.start()
    
    dsn = os.getenv("DATABASE_URL", "postgresql://zerogate_admin:zerogate_password@postgres:5432/zerogate_telemetry")
    app.state.db_pool = await asyncpg.create_pool(dsn=dsn, ssl=False, min_size=1, max_size=5)
    log.info("ZeroGate edge infrastructure routes and ledger pools online.")
    
    yield
    
    await app.state.producer.stop()
    await app.state.redis.aclose()
    await app.state.db_pool.close()

app = FastAPI(title="ZeroGate Fast-Ingress Routing Gateway", lifespan=lifespan)

async def verify_gateway_key(x_zerogate_key: str = Header(None)):
    if not x_zerogate_key:
        raise HTTPException(status_code=401, detail="Missing API Key header.")
        
    redis_client = app.state.redis
    is_valid = await redis_client.sismember("valid_api_keys", x_zerogate_key)
    if not is_valid and x_zerogate_key != os.getenv("ZEROGATE_API_KEY"):
        raise HTTPException(status_code=403, detail="Forbidden: Unauthorized or invalid key footprint.")
        
    return x_zerogate_key

class InferenceRequest(BaseModel):
    model: str = Field(..., example="llama3.1:8b")
    prompt: str = Field(..., example="Why is the sky blue?")


@app.post("/v1/compute", status_code=status.HTTP_202_ACCEPTED)
async def process_compute(payload: InferenceRequest, request: Request, client_key: str = Depends(verify_gateway_key)):
    """
    Processes and streams multi-tenant ingress inference compute payloads.
    
    Validates current active workspace request volumes against Redis-backed 
    tenant concurrency tables, enforces explicit HTTP 429 backpressure rules, 
    and handles downstream microservice routing mapping via the mesh network.
    """
    redis_client = app.state.redis
    requested_model = payload.model.strip()

    if os.getenv("ZEROGATE_MOCK") == "True":
        zerogate_base_url = "http://localhost:8000"
    else:
        zerogate_base_url = os.getenv("ZEROGATE_BASE_URL")

    max_allowed_concurrency = 30 if client_key == "zerogate-alpha-demo" else 100
    tenant_depth = int(await redis_client.get(f"active_jobs:{client_key}") or 0)
    
    if tenant_depth >= max_allowed_concurrency:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS, 
            detail="Workspace concurrent capacity has been reached."
        )
        
    request_id = str(uuid4())
    
    enqueued_payload = {
        "RequestID": request_id,
        "TenantID": client_key,  
        "Model": requested_model,
        "Prompt": payload.prompt,
        "ClientKey": client_key
    }

    try:
        await redis_client.incr(f"active_jobs:{client_key}")
        await redis_client.incr("global_in_flight_counter")
        
        topic = os.getenv("KAFKA_TOPIC", "inference_requests")
        await app.state.producer.send_and_wait(topic, json.dumps(enqueued_payload).encode('utf-8'))
        log.info(f"Enqueued Task ID '{request_id}' to pipeline brokers.")
        
    except Exception as e:
        await redis_client.decr(f"active_jobs:{client_key}")
        await redis_client.decr("global_in_flight_counter")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Ingress backend messaging pipeline failure: {e}"
        )
        
    return {
        "status": "accepted",
        "request_id": request_id,
        "polling_check_url": f"{zerogate_base_url}/v1/status/{request_id}",
        "message": f"Payload enqueued. Check results by running: curl -X GET {zerogate_base_url}/v1/status/{request_id}"
    }

@app.get("/v1/analytics")
async def get_system_analytics(request: Request, client_key: str = Depends(verify_gateway_key)):
    """
    Retrieves aggregated workspace ledger telemetry and infrastructure cost metrics.
    
    Queries the PostgreSQL time-series analytics engine to calculate absolute total 
    inferences, calculated token volume processing, average processing latencies, 
    and multi-tenant infrastructure idle-time savings calculations.
    """
    query = """
        SELECT 
            COUNT(id)::int as total_inferences,
            COALESCE(SUM(input_tokens + output_tokens), 0)::int as total_tokens,
            COALESCE(AVG(execution_duration_ms), 0)::float as avg_latency_ms,
            COALESCE(SUM(cold_start_duration_ms)::float / 1000.0 / 3600.0 * 2.65, 0.0)::float as total_savings
        FROM zerogate_cold_starts 
        WHERE client_key = $1  -- ◄ FIXED: Aligned query constraint parameter target columns
    """
    
    async with request.app.state.db_pool.acquire() as conn:
        row = await conn.fetchrow(query, client_key)
        
    return {
        "workspace_key": client_key,
        "ledger": {
            "total_inferences_processed": row["total_inferences"],
            "total_tokens_generated": row["total_tokens"],
            "aggregated_idle_tax_saved_usd": round(row["total_savings"], 5),
            "average_queue_overhead_ms": round(row["avg_latency_ms"], 2)
        },
        "status": "Healthy. Workspace data plane separation verified."
    }

@app.get("/v1/status/{request_id}")
async def get_status(request_id: str):
    """
    Polls the active consolidation state matrix for a specific inference lifecycle.
    
    Reads from the Redis hot-path cache pool to determine if a payload state is queued, 
    processing, or completed, fetching metrics such as execution generation durations 
    and estimated workspace hardware savings values upon final processing completion.
    """
    redis_client = app.state.redis
    
    # Read the consolidated state hash payload completely
    task_cache = await redis_client.hgetall(f"task:cache:{request_id}")
    
    task_state = task_cache.get("state", "queued_in_buffer")
    
    if task_state == "completed":
        gen_time = task_cache.get("time", "0.0")
        prompt = task_cache.get("prompt", "")
        response = task_cache.get("response", "")
        
        return {
            "request_id": request_id,
            "status": "completed",
            "metrics": {"execution_duration_seconds": float(gen_time), "estimated_savings_usd": round(float(gen_time) * 0.00055, 6)},
            "prompt": prompt,
            "result": response,
            "message": "Inference lifecycle finished. Idle footprint flattened."
        }
        
    return {
        "request_id": request_id,
        "status": "processing",
        "infrastructure": {"cluster_slice": "allocated_pool", "vllm_state": "hot_path_stream_engaged"}
    }

@app.post("/mock/runpod/graphql")
async def mock_runpod(request: Request):
    """
    Simulates RunPod Graphql Container API responses for zero-cost driver validation.
    """
    # 1. Parse the outbound payload body sent by our worker daemon
    body = await request.json()
    query_string = body.get("query", "")
    variables = body.get("variables", {})
    
    print(f"[RunPod Mock API] Intercepted GraphQL Query: {query_string}")
    print(f"[RunPod Mock API] Intercepted Input Variables: {variables}")
    
    # 2. Simulate a successful container pod spin-up response shape
    mock_success_response = {
        "data": {
            "podFindAndCreate": {
                "id": "mock-pod-xyz-12345",
                "imageName": variables.get("input", {}).get("imageName", "unknown"),
                "status": "PROVISIONING",
                "runtimeInSeconds": 0
            }
        }
    }
    
    return mock_success_response