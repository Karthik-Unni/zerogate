import os, httpx
from uuid import uuid4
from engine.drivers.base import BaseCloudDriver
from engine.logger import ZeroGateLogger

log = ZeroGateLogger("RUNPOD")

class RunPodDriver(BaseCloudDriver):
    """
    Encapsulates native async RunPod GraphQL mutation and querying contracts
    without relying on proprietary, bloated vendor SDK packages.
    """

    async def get_client_context(self, redis_client, tenant_id: str) -> tuple[httpx.AsyncClient, str, dict]:
        """
        Assembles a fully configured async client mesh, targeting RunPod's 
        centralized production GraphQL gateway routing endpoints.
        """
        api_key = await redis_client.get(f"auth:credentials:{tenant_id}:runpod")
        api_key = api_key or os.getenv("RUNPOD_API_KEY")
        
        base_url = os.getenv("RUNPOD_API_URL")
        
        if not api_key:
            raise ValueError("RunPod client context error: Missing active authentication token.")
            
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        return httpx.AsyncClient(timeout=30.0), base_url, headers

    async def allocate(self, client: httpx.AsyncClient, base_url: str, headers: dict, profile: str, config: dict) -> bool:
        """
        Constructs and dispatches the native GraphQL 'podFindAndCreate' mutation string.
        """
        mutation = """
        mutation DeployPod($input: PodFindAndDeployOnDemandInput!) {
            podFindAndDeployOnDemand(input: $input) {
                id
                desiredStatus
                imageName
            }
        }
        """

        log.info(f"MODEL NAME: {str(config.get('model_name')).strip()}")

        payload = {
            "query": mutation,
            "variables": {
                "input": {
                    "name": f"{config['name']}-{str(uuid4())[:4]}",
                    "imageName": str(config["image"]).strip(),
                    "gpuTypeId": str(profile).strip(),
                    "gpuCount": 1,
                    "ports": "8000/http",
                    "containerDiskInGb": 50,
                    "volumeInGb": 0,
                    "cloudType": "SECURE",
                    "dockerArgs": f"--model {str(config.get('model_name')).strip()} --trust-remote-code --enforce-eager --gpu-memory-utilization 0.90 --max-model-len 4096",
                }
            }
        }

        try:
            res = await client.post(base_url, json=payload, headers=headers)
            res_data = res.json()

            if not res.is_success:
                log.warning(f'DEBUG: {res.text}')
                log.warning(f"RunPod gateway rejected request with HTTP status code: {res.status_code}")
                return False
                
            pod_response = res_data.get("data", {}).get("podFindAndDeployOnDemand", {}) or {}
            pod_id = pod_response.get("id")
            log.info(f"Successfully secured container slot on RunPod. Allocation Tracking Token: {pod_id}")
            if pod_id:
                return pod_id
            
            return None

            
        except Exception as e:
            log.error(f"Fatal exception during RunPod async allocation dispatch loop: {str(e)}")
            return False

    async def discover_state(self, client: httpx.AsyncClient, base_url: str, headers: dict, config: dict) -> tuple[str, str]:
        """
        Queries the live RunPod hypervisor cluster using GraphQL to extract active container IP addresses.
        """
        if os.getenv("ZEROGATE_MOCK") == "True" or "gateway" in base_url:
            return "mock-runpod-pod-999", "gateway"

        query = """
        query GetPods {
            myself {
                pods {
                    id
                    name
                    uptimeSeconds
                    desiredStatus
                    runtime {
                        ports {
                            isIpPublic
                            ip
                            publicPort
                            privatePort
                        }
                    }
                }
            }
        }
        """
        
        try:
            res = await client.post(base_url, json={"query": query}, headers=headers)

            if not res.is_success or "errors" in res.json():
                log.info(f"Upstream API Error Matrix: {res.json().get('errors')}")
                return "NONE", ""

            pods = res.json().get("data", {}).get("myself", {}).get("pods", [])

            active_pods = [
                p for p in pods 
                if str(config["name"]).strip() in str(p.get("name", "")).strip()
                and p.get("desiredStatus", "").upper() not in ["TERMINATED", "DEAD", "EXITED"]
            ]

            if active_pods:
                target_pod = active_pods[-1]
                pod_id = str(target_pod.get("id"))
                pod_status = target_pod.get("desiredStatus", "").upper()

                if pod_id and pod_status == "RUNNING":
                    proxy_domain = f"{pod_id}-8000.proxy.runpod.net"
                    log.info(f"Standardizing endpoint to production proxy link: {proxy_domain}")
                    return pod_id, proxy_domain
                        
        except Exception as e:
            log.error(f"Failed to query live RunPod cluster status matrix: {str(e)}")
            
        return "NONE", ""

    async def teardown(self, client: httpx.AsyncClient, base_url: str, headers: dict, node_id: str) -> bool:
        """
        Dispatches the active RunPod GraphQL 'podTerminate' mutation string 
        to violently flatten idle scale-to-zero container nodes.
        """
        mutation = "mutation TerminatePod($input: PodTerminateInput!) { podTerminate(input: $input) }"
        
        payload = {
            "query": mutation,
            "variables": {
                "input": {
                    "podId": str(node_id).strip()
                }
            }
        }
        
        try:
            res = await client.post(base_url, json=payload, headers=headers)
            
            if not res.is_success:
                log.error(f"[RUNPOD-TEARDOWN] Gateway rejected network transaction with code: {res.status_code}")
                return False
                
            res_data = res.json()
            if "errors" in res_data:
                error_detail = res_data["errors"][0].get("message", "Unknown hypervisor teardown restriction")
                log.error(f"[RUNPOD-TEARDOWN] Schema validation rejected deletion payload: {error_detail}")
                return False
                
            log.info(f"Infrastructure flattened. RunPod container slot {node_id} terminated successfully.")
            return True
            
        except Exception as e:
            log.error(f"Fatal exception encountered during active RunPod container erasure track: {str(e)}")
            return False