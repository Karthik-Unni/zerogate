import httpx, os
from engine.drivers.base import BaseCloudDriver
from engine.logger import ZeroGateLogger

log = ZeroGateLogger("RUNPOD")

class RunPodDriver(BaseCloudDriver):
    """
    Encapsulates native async RunPod GraphQL mutation execution contracts.
    """

    async def get_client_context(self, redis_client, tenant_id: str) -> tuple:
        api_key = await redis_client.get(f"auth:credentials:{tenant_id}:runpod")
        api_key = api_key or os.getenv("RUNPOD_API_KEY")
        base_url = os.getenv("RUNPOD_API_URL", "https://runpod.io")
        
        if not api_key:
            raise ValueError("RunPod client context error: Missing active authentication token.")
            
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        return httpx.AsyncClient(timeout=30.0), base_url, headers
    
    async def allocate(self, client, base_url, headers, profile, config) -> bool:
        mutation = """
        mutation CreatePod($input: PodInput!) {
            podFindAndCreate(input: $input) { id status }
        }
        """
        payload = {
            "query": mutation,
            "variables": {
                "input": {
                    "name": config["name"],
                    "imageName": config.get("image"),
                    "gpuTypeId": profile,
                    "gpuCount": 1,
                    "ports": "11434/http",
                    "containerDiskInGb": 50
                }
            }
        }
        res = await client.post(base_url, json=payload, headers=headers)
        if not res.is_success or "errors" in res.json():
            log.error(f"RunPod allocation failure: {res.text}")
            return False
        return True

    async def discover_state(self, client, base_url, headers, config) -> tuple:
        # Intercept point for your local FastAPI simulator route
        return "mock-runpod-pod-999", "gateway"

    async def teardown(self, client, base_url, headers, node_id) -> bool:
        return True