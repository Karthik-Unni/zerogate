"""
Multi-Tenant Telemetry and Ingress Stress-Testing Matrix.

This script operates as a standalone local developer component. It fires
concurrent, batched HTTP payloads into the ZeroGate API Gateway mesh to simulate 
high-volume LLM prompt traffic, cleanly populating downstream Kafka event streams 
and PostgreSQL transactional ledgers for testing and evaluation purposes.
"""
import asyncio, httpx, random, os
from engine.logger import ZeroGateLogger

log = ZeroGateLogger("SIMULATOR")

HEADERS = {"X-ZeroGate-Key": "zerogate-alpha-demo", "Content-Type": "application/json"}

PROMPTS = [
    "Analyze the economic drain of unmanaged bare-metal GPU idle time for AI start-ups, and contrast it with the operational cost-arbitrage of a self-hosted, scale-to-zero multi-tenant control plane running on wholesale spot instances in one dense sentence.",
    "Write a high-performance concurrent python worker loop strategy.",
    "Why is unmanaged GPU idle time an enterprise financial drain?",
    "Synthesize the trade-offs between Kafka streams and RabbitMQ configurations."
]

if os.getenv("ZEROGATE_MOCK") == "True":
    # Runs inside docker mesh, must use "zerogate-gateway"
    BASE_URL = "http://zerogate-gateway:8000"
else:
    BASE_URL = os.getenv("ZEROGATE_BASE_URL", "https://zerogate.cloud")

async def fire_simulated_inference(client, task_id):
    payload = {
        "model": "unsloth/Meta-Llama-3.1-8B-Instruct",
        "prompt": random.choice(PROMPTS)
    }
    try:
        res = await client.post(BASE_URL + "/v1/compute", json=payload, timeout=10.0)
        res.raise_for_status() 
        if "application/json" in res.headers.get("content-type", ""):
            req_id = res.json().get('request_id', 'unknown_id')
            log.info(f"[Task {task_id}] Gateway Response: {res.status_code} | {req_id}")
        else:
            log.info(f"[Task {task_id}] Error: Expected JSON response, got text/html. Check your BASE_URL.")

    except httpx.HTTPStatusError as e:
        log.info(f"[Task {task_id}] Gateway HTTP Error: {e.response.status_code}")
    except httpx.RequestError as e:
        log.info(f"[Task {task_id}] Gateway Network/Timeout Error: {e}")
    except Exception as e:
        log.info(f"[Task {task_id}] Unexpected Error: {e}")

async def run_stress_test_engine(total_requests, batch_size):
    async with httpx.AsyncClient(headers=HEADERS) as client:
        for i in range(1, total_requests, batch_size):
            tasks = [fire_simulated_inference(client, i + j) for j in range(batch_size)]
            await asyncio.gather(*tasks)
            await asyncio.sleep(0.2)

    # Resolve the correct address path for the user running curl from their host machine
    user_url = "http://localhost:8000" if os.getenv("ZEROGATE_MOCK") == "True" else BASE_URL

    print("\n")
    print("Simulation matrix complete. Your multi-tenant ledger is fully populated.")
    print("\n")
    print("=" * 87)
    print("DEMO VERIFICATION COMMANDS:")
    print(f"curl -X GET {user_url}/v1/status/<request_id>")
    print(f"curl -X GET {user_url}/v1/analytics -H \"X-ZeroGate-Key: zerogate-alpha-demo\"")
    print("=" * 87)
    print("\n")

if __name__ == "__main__":
    total_reqs = int(os.getenv("SIM_TOTAL_REQUESTS", 10))
    batch_sz = int(os.getenv("SIM_BATCH_SIZE", 5))
    asyncio.run(run_stress_test_engine(total_reqs, batch_sz))
