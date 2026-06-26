"""
ZeroGate Autonomic Scaling and Inference Message Worker.

This engine component runs as a persistent background daemon, consuming 
ingress streaming events from Kafka topics, managing distributed cluster 
locks in Redis for resource scaling, and tracking analytical telemetry.
"""
import json, asyncio, time, os, httpx, asyncpg,logging
from aiokafka import AIOKafkaConsumer, TopicPartition
import redis.asyncio as aioredis
from engine.logger import ZeroGateLogger
from engine.providers import manage_hyperstack_lifecycle, scale_to_zero

# Global Memory Anchor to protect tasks from Python's garbage collector
ACTIVE_TASKS = set()
log = ZeroGateLogger("WORKER")

async def bootstrap_database_schema(db_pool):
    """
    Validates and provisions the core analytical database schema on startup.
    
    Creates the relational transactional ledger table and enforces multi-column 
    indexing strings to guarantee fast multi-tenant queries during data separation.
    """
    schema_query = """
    CREATE TABLE IF NOT EXISTS zerogate_cold_starts (
        id SERIAL PRIMARY KEY,
        request_id VARCHAR(255) NOT NULL UNIQUE,
        model VARCHAR(100) NOT NULL,
        cold_start_duration_ms INT NOT NULL DEFAULT 0,
        execution_duration_ms INT NOT NULL DEFAULT 0,
        input_tokens INT NOT NULL DEFAULT 0,
        output_tokens INT NOT NULL DEFAULT 0,
        client_key VARCHAR(255) NOT NULL DEFAULT 'zerogate-alpha-demo',
        tenant_id VARCHAR(255) NOT NULL DEFAULT 'default_tenant',
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_zg_telemetry_request_id ON zerogate_cold_starts(request_id);
    CREATE INDEX IF NOT EXISTS idx_zg_telemetry_client_key ON zerogate_cold_starts(client_key);
    CREATE INDEX IF NOT EXISTS idx_zg_telemetry_tenant_id ON zerogate_cold_starts(tenant_id);
    """
    async with db_pool.acquire() as conn:
        await conn.execute(schema_query)
    log.info("Telemetry and multi-tenant schema verification complete.")


async def process_inference_job(payload, redis_client, db_pool, consumer, msg, in_flight_offsets, commit_lock):
    """
    Orchestrates the entire asynchronous multi-tenant inference job lifecycle.
    
    This core engine pipeline executes the following sequential lifecycle steps:
      1. Evaluates routing paths against global in-flight traffic burst thresholds.
      2. Coordinates cluster state locks in Redis to manage hypervisor auto-scaling.
      3. Awaits target vLLM model engine synchronization and container path heating.
      4. Dispatches prompt requests to the live backend or evaluates mock layers.
      5. Commits token usage logs, transactional latencies, and metrics to PostgreSQL.
      6. Executes cleanup states, decreases concurrent depths, and commits Kafka offsets.
    """
    req_id, tenant_id = payload.get("RequestID"), payload.get("TenantID", "default_tenant")
    client_key, target_model = payload.get("ClientKey", "zerogate-alpha-demo"), payload.get("Model")
    start_time, cold_start_ms, target_ip = time.time(), 0, None

    try:
        # Streamlined Routing Selector Path
        global_depth = int(await redis_client.get("global_in_flight_counter") or 0)
        
        if global_depth <= int(os.getenv("BURST_THRESHOLD", 0)):
            log.info(f"Task {req_id} mapped directly to static buffer pool.")
            target_ip = os.getenv("HYPERSTACK_MAIN_NODE_IP")
        else:
            # Multi-Tenant Auto-Scaling Infrastructure Track
            cluster_key, lock_key = f"cluster_state:{tenant_id}:burst", f"lock:provisioning:{tenant_id}:burst"
            
            # Simple, native text string dictionary extraction
            cluster = await redis_client.hgetall(cluster_key)
            gpu_status, target_ip = cluster.get("status", "cold"), cluster.get("ip", "")

            if gpu_status == "cold" or not target_ip:
                log.info(f"Task {req_id} queued at provisioning lock entrance...")
                
                async with redis_client.lock(lock_key, timeout=420):
                    cluster = await redis_client.hgetall(cluster_key)
                    gpu_status = cluster.get("status", "cold")
                    
                    # Flattened Lock Guardrails
                    if gpu_status == "booting":
                        log.info(f"Task {req_id} waiting on active boot sequence...")
                        while gpu_status == "booting":
                            await asyncio.sleep(3)
                            cluster = await redis_client.hgetall(cluster_key)
                            gpu_status = cluster.get("status", "cold")
                        target_ip = cluster.get("ip", "")

                    elif gpu_status == "cold":
                        log.info(f"Task {req_id} building infrastructure...")
                        await redis_client.hset(cluster_key, "status", "booting")
                        
                        boot_start = time.time()
                        target_ip = await manage_hyperstack_lifecycle(redis_client, "start", tenant_id, "burst")
                        cold_start_ms = int((time.time() - boot_start) * 1000)
                        
                        await redis_client.hset(cluster_key, mapping={"status": "active", "ip": target_ip})
                        await redis_client.expire(cluster_key, 1800)
                    else:
                        target_ip = cluster.get("ip", "")

        if not target_ip: 
            raise Exception("Failed to secure active target IP from hypervisor layer.")

        if os.getenv("ZEROGATE_MOCK") != "True":
            async with httpx.AsyncClient(timeout=2.0) as client:
                for attempt in range(90):
                    try:
                        if (await client.get(f"http://{target_ip}:11434/v1/models")).status_code == 200:
                            if await redis_client.set(f"log_gate:{target_ip}:ready", "1", ex=5, nx=True):
                                log.info("Target interface synchronized! vLLM model engine is hot.")
                            break
                    except (httpx.ConnectError, httpx.ConnectTimeout):
                        # Only one thread prints the ticker step
                        if await redis_client.set(f"log_gate:{target_ip}:tick:{attempt}", "1", ex=5, nx=True):
                            log.warning(f"Synchronizing cloud instance model cache layer (Checking step {attempt + 1}/90)...")
                        
                        await asyncio.sleep(2)

        start_time = time.time()

        if os.getenv("ZEROGATE_MOCK") == "True":
            # Emulate model token processing delay
            await asyncio.sleep(4.5)
            res_json = {
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": f"[ZEROGATE MOCK] Token array successfully processed by ZeroGate event streams. Synchronized via mock node target: {target_ip}"
                    }
                }],
                "usage": {"prompt_tokens": 32, "completion_tokens": 114}
            }
        else:
            vllm_payload = {
                "model": target_model,
                "messages": [{"role": "user", "content": payload.get("Prompt")}],
                "temperature": 0.7,
                "max_tokens": 1024
            }
            async with httpx.AsyncClient(timeout=120.0) as client:
                res_json = (await client.post(f"http://{target_ip}:11434/v1/chat/completions", json=vllm_payload)).json()

        choices = res_json.get("choices", [])
        ai_reply = choices[0].get("message", {}).get("content", "") if choices else "No content returned."

        pure_inference_seconds = float(time.time() - start_time)

        # Update the Redis cache hash token with pure inference speed parameters
        await redis_client.hset(f"task:cache:{req_id}", mapping={
            "response": ai_reply, 
            "prompt": payload.get("Prompt", ""), 
            "time": str(pure_inference_seconds), 
            "state": "completed"
        })
        await redis_client.expire(f"task:cache:{req_id}", 600)

        # On conflict do nothing if kafka replays a message exists due to network glitch or mid batch restart
        # Prove at-least-once delivery with an explicit idempotent data consumer.
        usage = res_json.get("usage", {})
        async with db_pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO zerogate_cold_starts 
                (request_id, model, cold_start_duration_ms, execution_duration_ms, input_tokens, output_tokens, client_key, tenant_id) 
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (request_id) DO NOTHING;""",
                req_id, target_model, cold_start_ms, int(pure_inference_seconds * 1000),
                usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0), client_key, tenant_id
            )
        log.info(f"Task {req_id} committed to SQL telemetry grid.")

    except Exception as error:
        log.info(f"Pipeline execution failed for {req_id}: {error}")
    
    finally:
        # Streamlined Cleanup and Kafka Commits
        await redis_client.decr(f"active_jobs:{client_key}")
        if int(await redis_client.decr("global_in_flight_counter") or 0) < 0:
            await redis_client.set("global_in_flight_counter", 0)
            
        async with commit_lock:
            in_flight_offsets[msg.offset] = True
            if min(in_flight_offsets.keys()) == msg.offset:
                await consumer.commit({TopicPartition(msg.topic, msg.partition): msg.offset + 1})

async def main_worker_loop():
    """
    Main orchestration runtime loop initializing background event stream listeners.
    
    Establishes core cache connections and analytical database pools, applies internal 
    broker connection retry loops, and continuously dispatches ingested payload jobs 
    into memory-anchored background execution tasks.
    """
    log.info("Control Plane Core Initializing...")
    in_flight_offsets = {}
    commit_lock = asyncio.Lock()
    
    redis_client = await aioredis.from_url(os.getenv("REDIS_URL"), decode_responses=True)
    dsn = os.getenv("DATABASE_URL", "")
    db_pool = await asyncpg.create_pool(dsn=dsn, ssl=False, min_size=1, max_size=5)
    await bootstrap_database_schema(db_pool)
    
    logging.getLogger("aiokafka.consumer.group_coordinator").setLevel(logging.CRITICAL)
    logging.getLogger("aiokafka.cluster").setLevel(logging.CRITICAL)
    
    if os.getenv("ZEROGATE_MOCK") == "True":
        enable_auto_commit = True
        auto_offset_reset = "latest"
    else:
        enable_auto_commit = False
        auto_offset_reset = "earliest"

    consumer = AIOKafkaConsumer(
        os.getenv("KAFKA_TOPIC", "inference_requests"),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        group_id="zerogate-worker-group",
        enable_auto_commit=enable_auto_commit,
        auto_offset_reset=auto_offset_reset
    )
    
    for attempt in range(1, 7):
        try:
            await consumer.start()
            log.info("Channels fully initialized and listening.")
            break
        except Exception as e:
            if attempt == 6: return log.info("Metadata broker offline.")
            await asyncio.sleep(attempt)

    # Initialize scale-to-zero tracker background daemon
    asyncio.create_task(scale_to_zero(redis_client))
    
    try:
        async for msg in consumer:
            payload = json.loads(msg.value.decode('utf-8'))
            client_key = payload.get("ClientKey", "zerogate-alpha-demo")
            
            in_flight_offsets[msg.offset] = False
            
            global_flight = int(await redis_client.get("global_in_flight_counter") or 0)

            log_gate_key = f"log_gate:pipeline_load:{global_flight}"

            # Log for 1 in a batch
            if await redis_client.set(log_gate_key, "1", ex=1, nx=True):
                log.info(f"Tenant: {client_key} | Ingress Surge Detected | Global Pipeline Load: {global_flight}")
                
            # Create task and hook it into our strong reference tracking set
            task = asyncio.create_task(process_inference_job(
                payload, redis_client, db_pool, consumer, msg, in_flight_offsets, commit_lock
            ))
            ACTIVE_TASKS.add(task)
            task.add_done_callback(ACTIVE_TASKS.discard)

    except Exception as stream_err:
        log.critical(f"Continuous stream cracked: {stream_err}")
    finally:
        log.info( "Intercepted shutdown signal. Flushing pending commit logs...")
        if consumer:
            try:
                await consumer.commit()
            except:
                pass
            await consumer.stop()
        await redis_client.aclose()
        await db_pool.close()

if __name__ == "__main__":
    asyncio.run(main_worker_loop())
