from uuid import uuid4
import httpx, os
from engine.drivers.base import BaseCloudDriver
from engine.logger import ZeroGateLogger

log = ZeroGateLogger("HYPERSTACK")

class HyperstackDriver(BaseCloudDriver):
    """
    Encapsulates raw Hyperstack REST API hypervisor execution contracts.
    """
    async def get_client_context(self, redis_client, tenant_id: str) -> tuple:
        api_key = await redis_client.get(f"auth:credentials:{tenant_id}:hyperstack")
        api_key = api_key or os.getenv("HYPERSTACK_API_KEY")
        base_url = os.getenv("HYPERSTACK_API_URL")
        
        if not api_key or not base_url:
            raise ValueError("Hyperstack client context error: Missing active credentials or API URL configurations.")
            
        headers = {
            "api_key": api_key,
            "Content-Type": "application/json"
        }
        return httpx.AsyncClient(timeout=30.0), base_url, headers
    
    async def allocate(self, client: httpx.AsyncClient, base_url: str, headers: dict, profile: str, config: dict) -> bool:
        """
        Consumes the raw configuration parameters and constructs the heavy 
        Hyperstack REST payload structure explicitly.
        """
        payload = {
            "name": f"{config['name']}-{str(uuid4())[:4]}",
            "flavor_name": profile,
            "image_name": config["image"],
            "environment_name": os.getenv("HYPERSTACK_ENVIRONMENT_NAME", "Default-Canada"),
            "assign_floating_ip": True,
            "key_name": os.getenv("HYPERSTACK_SSH_KEY_NAME", "ssh-canada"),
            "user_data": "#!/bin/bash\nnuclips docker start zerogate-vllm-core\n",
            "count": 1,
            "security_rules": [
                {
                    "direction": "ingress",
                    "protocol": "tcp",
                    "port_range_min": 11434,
                    "port_range_max": 11434,
                    "remote_ip_prefix": "0.0.0.0/0",
                    "ethertype": "IPv4"
                }
            ]
        }
        
        res = await client.post(f"{base_url}/core/virtual-machines", json=payload, headers=headers)
        if res.is_success:
            vm_id = str(res.json().get("virtual_machine", {}).get("id", ""))
            log.info(f"Secured slot footprint for flavor {profile}. Tracking token: {vm_id}")
            return True
        else:
            try:
                error_detail = res.json().get("message", res.text)
            except Exception:
                error_detail = res.text
                
            log.warning(f"[OUT-OF-STOCK] Provider rejected allocation for {profile}: {error_detail}. Shifting to fallback lane...")
            return False
        

    async def discover_state(self, client: httpx.AsyncClient, base_url: str, headers: dict, config: dict) -> tuple[str, str]:
            """
            Queries live cloud environments to discover active virtual instances and extract IPs.
            """
            try:
                res = await client.get(f"{base_url}/core/virtual-machines", headers=headers)
                if not res.is_success or res.text in ["NONE", "None", None]:
                    return "NONE", ""
                    
                vms = [
                    vm for vm in res.json().get("instances", [])
                    if config["name"] in vm.get("name", "")
                    and vm.get("status", "").upper() not in ["DELETING", "DELETED", "TEARING_DOWN", "SHUTDOWN", "SHUTOFF"]
                ]
                
                if vms:
                    last_vm = vms[-1]
                    vm_id = str(last_vm.get("id"))
                    public_ip = str(last_vm.get("floating_ip") or last_vm.get("public_ip", "")).strip()

                    if not public_ip or public_ip == "-":
                        return "NONE", ""
                    
                    return vm_id, f"{public_ip}:11434"
                    
            except Exception as e:
                log.error(f"Failed to parse Hyperstack hypervisor registry: {str(e)}")
                
            return "NONE", ""

    async def teardown(self, client: httpx.AsyncClient, base_url: str, headers: dict, node_id: str) -> bool:
        """
        Issues an explicit REST erasure mandate to destroy the instance.
        """
        res = await client.delete(f"{base_url}/core/virtual-machines/{node_id}", headers=headers)
        return res.is_success