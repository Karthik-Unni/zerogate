"""
ZeroGate Hardware Allocation and Telemetry Provisioning Engine.

This module acts as the explicit resource orchestration abstraction layer,
managing cloud hardware lifecycles, evaluating hardware fallback priorities,
and executing automated pool expansion or scale-to-zero teardown sequences.
"""
import os, httpx, json, asyncio
from uuid import uuid4
from engine.logger import ZeroGateLogger
from engine.configs import DEFAULTS

log = ZeroGateLogger("AUTONOMIC")

async def get_api_context(redis_client, tenant_id: str):
    """
    Assembles authorized HTTP client sessions for third-party hypervisor controllers.
    
    Extracts underlying credentials from encrypted tenant rows or returns isolated 
    offline credential targets when running under ZEROGATE_MOCK conditions.
    """
    if os.getenv("ZEROGATE_MOCK") == "True":
        return "MOCK_CLIENT", "http://zerogate-gateway:8000", {"Authorization": "Bearer mock-key"}

    api_key = await redis_client.get(f"auth:credentials:{tenant_id}:api_key") or os.getenv("HYPERSTACK_API_KEY")
    base_url = os.getenv("HYPERSTACK_API_URL")
    headers = {
        "api_key": api_key,
        "Content-Type": "application/json"
    }
    return httpx.AsyncClient(timeout=30.0), base_url, headers

async def discover_cluster_state(client, base_url, headers, config):
    """
    Queries live cloud environments to discover and filter active virtual instances.
    
    Filters out terminating or unroutable machines, extracting the youngest public 
    or floating IP footprint to keep network routing matrices up to date.
    """
    if client == "MOCK_CLIENT":
        return "mock-vm-969X", "zerogate-gateway"
    try:
        res = await client.get(f"{base_url}/core/virtual-machines", headers=headers)
        if not res.is_success or res.text in ["NONE", "None", None]:
            return "NONE", None
            
        vms = [
            vm for vm in res.json().get("instances", [])
            if config["name"] in vm.get("name", "") and 
            vm.get("status", "").upper() not in ["DELETING", "DELETED", "TEARING_DOWN", "SHUTDOWN", "SHUTOFF"]
        ]
        if vms:
            last_vm = vms[-1]
            ip = last_vm.get("floating_ip") or last_vm.get("public_ip")
            return str(last_vm.get("id")), str(ip).strip() if ip else None
        return "NONE", None
    except Exception as e:
        log.error(f"Failed to parse hypervisor registry: {e}")
        return "NONE", None

async def load_workspace_blueprint(redis_client, tenant_id: str, pool_name: str) -> dict:
    """
    Dynamically loads the infrastructure hardware footprint blueprint for a tenant.
    
    Queries hot-path Redis configurations for custom profiles, falling back to a 
    hardcoded multi-tier defaults dictionary if no custom override matrix is found.
    """
    if os.getenv("ZEROGATE_MOCK") == "True":
        pool_name = "mock"

    blueprint_raw = await redis_client.get(f"workspace:blueprints:{tenant_id}:{pool_name}")
    if blueprint_raw:
        try:
            return json.loads(blueprint_raw)
        except Exception:
            pass

    defaults = DEFAULTS

    return defaults.get(pool_name, defaults["base"])

async def manage_hyperstack_lifecycle(redis_client, action: str, tenant_id: str, pool_name: str = "base") -> str:
    """
    Orchestrates live cloud hardware allocations and resource teardowns.

    Handles hardware profile fallback scheduling arrays, evaluates inventory stock levels,
    polls infrastructure fabric providers until instance environments report
    healthy network status, and cleanly destroys instances during scale-down requests.
    """
    log.info(f"Lifecycle triggered | Tenant: {tenant_id} | Action: {action} | Pool: {pool_name}")
        
    client, base_url, headers = await get_api_context(redis_client, tenant_id)
    config = await load_workspace_blueprint(redis_client, tenant_id, pool_name)

    if os.getenv("ZEROGATE_MOCK") == "True":
        if action == "start":
            log.info("Intercepting infrastructure provisioning request.")
            log.info("Emulating cloud hardware initialization timeline...")
            await asyncio.sleep(2.5)
            log.info("Mock container node hot and registered successfully.")
            return "zerogate-gateway" 
            
        elif action in ["delete", "teardown"]:
            log.info(f"Intercepting infrastructure erasure for pool: {pool_name}")
            await asyncio.sleep(1.0)
            return "SUCCESS"

    try:
        if action == "start":

            raw_profiles = config.get("profiles", ["n3-RTX-A6000x1-spot"])

            profile_fallback_list = raw_profiles if isinstance(raw_profiles, list) else [raw_profiles]
            
            log.info(f"Initiating hardware allocation sweeps across priority lanes: {profile_fallback_list}")
            
            allocated_successfully = False
            
            for flavor_profile in profile_fallback_list:
                log.info(f"Attempting compute pool allocation on flavor profile: {flavor_profile}")
                
                payload = {
                    "name": f"{config['name']}-{str(uuid4())[:4]}",
                    "flavor_name": flavor_profile,
                    "image_name": config["image"],
                    "environment_name": os.getenv("HYPERSTACK_ENVIRONMENT_NAME", "Default-Canada"),
                    "assign_floating_ip": True,
                    "key_name": os.getenv("HYPERSTACK_SSH_KEY_NAME", "ssh-canada"),
                    "user_data": "#!/bin/bash\nuclips docker start zerogate-vllm-core\n",
                    "count": 1,
                    "security_rules": [{"direction": "ingress", "protocol": "tcp", "port_range_min": 11434, "port_range_max": 11434, "remote_ip_prefix": "0.0.0.0/0", "ethertype": "IPv4"}]
                }
                
                res = await client.post(f"{base_url}/core/virtual-machines", json=payload, headers=headers)
                
                if res.is_success:
                    vm_id = str(res.json().get("virtual_machine", {}).get("id", ""))
                    log.info(f"Secured slot footprint for flavor {flavor_profile}. Tracking token: {vm_id}")
                    allocated_successfully = True
                    break 
                else:
                    try:
                        error_detail = res.json().get("message", res.text)
                    except Exception:
                        error_detail = res.text
                    log.warning(f"[OUT-OF-STOCK] Provider rejected allocation for {flavor_profile}: {error_detail}. Shifting to fallback lane...")
            
            if not allocated_successfully:
                log.critical("[ORCHESTRATOR-FATAL] Mid level cloud gpus are completely out of stock.")
                return ""
            
            log.info("Awaiting cloud hardware allocation slots. Tracking hypervisor initialization sequences...")
            for attempt in range(1, 91):
                await asyncio.sleep(4)
                
                polled_id, ip = await discover_cluster_state(client, base_url, headers, config)
                
                if not ip:
                    log.warning(f"Synchronizing with cloud hardware fabric managers (Polling check {attempt}/90)...")
                
                if ip and ip not in ["ERROR", "FAILED", "SHUTOFF"]:
                    log.info(f"Primary Hyperstack instance live and routable at: {ip}")
                    return str(ip).strip()
                    
            log.info("Hardware node boot sequence exceeded 6-minute window.")
            return ""

        elif action in ["delete", "teardown"]:

            # We only need to discover the cluster state when tearing down to find the VM target ID
            vm_id, public_ip = await discover_cluster_state(client, base_url, headers, config)
            
            if vm_id == "NONE":
                log.info(f"No active infrastructure footprint found for tenant: {tenant_id}")
                return "SUCCESS"
                
            res = await client.delete(f"{base_url}/core/virtual-machines/{vm_id}", headers=headers)
            if res.is_success:
                log.info(f"Infrastructure flattened. Node token {vm_id} unlinked.")
                return "SUCCESS"
                
            log.info(f"Hypervisor refused erasure: {res.text}")
            return "FAILED"
            
    except Exception as e:
        log.critical(f"Critical infrastructure pipeline failure: {e}")
    finally:
        await client.aclose()
    return ""


async def scale_to_zero(redis_client):
    """
    A continuous background daemon loop tracking workspace idleness thresholds.
    
    Sweeps active Redis cluster states, monitors multi-tenant request activity, 
    and automatically triggers hypervisor resource teardown workflows when a 
    workspace remains completely idle past the configured timeout limits.
    """
    log.info("Multi-Tenant Scale-to-Zero background daemon engaged.")
    IDLE_TIMEOUT = int(os.getenv("SCALE_TO_ZERO_TIMEOUT", 10))
    WARN_WINDOW = int(os.getenv("SCALE_TO_ZERO_WARN_WINDOW", 5))
    SLEEP_INTERVAL = 2
    WARN_THRESHOLD = IDLE_TIMEOUT - WARN_WINDOW

    # Run the self-healing cloud sweep once on container boot up, ignore if mock = true
    try:
        if os.getenv("ZEROGATE_MOCK") != "True":
            system_tenant = "zerogate-master-key"
            client, base_url, headers = await get_api_context(redis_client, system_tenant)
            config = await load_workspace_blueprint(redis_client, system_tenant, "burst")
            
            log.info("Sweeping cloud provider for orphan instances...")
            vm_id, public_ip = await discover_cluster_state(client, base_url, headers, config)
            if isinstance(client, httpx.AsyncClient):
                await client.aclose()
            
            if vm_id != "NONE" and public_ip:
                log.info(f"Detected orphan VM {vm_id} at {public_ip}. Healing cache...")
                cluster_key = f"cluster_state:{system_tenant}:burst"
                await redis_client.hset(cluster_key, mapping={"status": "active", "ip": public_ip})
                await redis_client.set(f"active_jobs:{system_tenant}", "0")

            log.info("Cloud provider cleared of orphan instances.")

        
    except Exception as boot_sync_err:
        log.info(f"Startup cloud provider sweep failed: {boot_sync_err}")

    while True:
        await asyncio.sleep(SLEEP_INTERVAL)
        try:
            # Non-blocking async scanning over distributed keyspace strings
            async for cluster_key in redis_client.scan_iter(match="cluster_state:*:burst"):
                parts = cluster_key.split(":")
                if len(parts) < 3:
                    continue
                tenant_id = parts[1]

                idle_ticks_key = f"tenant_idle_ticks:{tenant_id}"
                warned_key = f"tenant_warned_state:{tenant_id}"

                current_ticks_raw = await redis_client.get(idle_ticks_key)
                current_ticks = int(current_ticks_raw) if current_ticks_raw else 0
                tenant_jobs = int(await redis_client.get(f"active_jobs:{tenant_id}") or 0)
                
                cluster = await redis_client.hgetall(cluster_key)
                gpu_status = cluster.get("status", "cold")

                # Reset tracker bypass guardrails across the shared Redis cluster state
                if gpu_status in ["cold", "tearing_down"] or tenant_jobs > 0:
                    await redis_client.delete(idle_ticks_key, warned_key)
                    continue

                # Clock Progression Metrics
                current_ticks += SLEEP_INTERVAL
                await redis_client.set(idle_ticks_key, current_ticks)
                log.info(f"Tenant: {tenant_id} | Idle Time: {current_ticks}s / {IDLE_TIMEOUT}s")

                # Evaluate Warning and Timeout Threshold Boundaries
                if current_ticks >= IDLE_TIMEOUT:
                    log.info(f"Tenant {tenant_id} timed out. Processing infrastructure erasure...")
                    await redis_client.hset(cluster_key, "status", "tearing_down")

                    if await manage_hyperstack_lifecycle(redis_client, "delete", tenant_id, "burst") == "SUCCESS":
                        await redis_client.delete(cluster_key, idle_ticks_key, warned_key)
                    else:
                        await redis_client.hset(cluster_key, "status", "active")
                        
                elif current_ticks >= WARN_THRESHOLD:
                    is_warned = await redis_client.get(warned_key) == "True"
                    if not is_warned:
                        log.info(f"Warning: Tenant {tenant_id} cluster idle for {current_ticks}s! Teardown in {IDLE_TIMEOUT - current_ticks}s!")
                        await redis_client.set(warned_key, "True")

        except Exception as loop_crash_error:
            log.info(f"The background loop caught an exception frame: {loop_crash_error}")
