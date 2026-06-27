"""
ZeroGate Hardware Allocation and Telemetry Provisioning Engine.

This module acts as the explicit resource orchestration abstraction layer,
managing cloud hardware lifecycles, evaluating hardware fallback priorities,
and executing automated pool expansion or scale-to-zero teardown sequences.
"""
import os, json, asyncio
from uuid import uuid4
from engine.logger import ZeroGateLogger
from engine.configs import DEFAULTS
from engine.drivers.hyperstack import HyperstackDriver
from engine.drivers.runpod import RunPodDriver
from engine.drivers.mock import MockDriver

log = ZeroGateLogger("AUTONOMIC")

PROVIDER_REGISTRY = {
    "hyperstack": HyperstackDriver(),
    "runpod": RunPodDriver(),
    "mock": MockDriver()
}

async def load_workspace_blueprint(redis_client, tenant_id: str, pool_name: str) -> dict:
    """
    Dynamically loads the infrastructure hardware footprint blueprint for a tenant.
    
    Queries hot-path Redis configurations for custom profiles, falling back to a 
    hardcoded multi-tier defaults dictionary if no custom override matrix is found.
    """
    if os.getenv("ZEROGATE_MOCK") == "True":
        pool_name = "mock"

    # For preconfigured defaults
    blueprint_raw = await redis_client.get(f"workspace:blueprints:{tenant_id}:{pool_name}")
    if blueprint_raw:
        try:
            return json.loads(blueprint_raw)
        except Exception:
            pass

    defaults = DEFAULTS

    return defaults.get(pool_name, defaults["base"])

async def manage_infrastructure_lifecycle(redis_client, action: str, tenant_id: str, pool_name: str) -> str:
    """
    Orchestrates live cloud hardware allocations and resource teardowns.

    Handles hardware profile fallback scheduling arrays, evaluates inventory stock levels,
    polls infrastructure fabric providers until instance environments report
    healthy network status, and cleanly destroys instances during scale-down requests.
    """
    log.info(f"Lifecycle triggered | Tenant: {tenant_id} | Action: {action} | Pool: {pool_name}")
    config = await load_workspace_blueprint(redis_client, tenant_id, pool_name)
    provider = config.get("provider")
    
    if not provider:
        raise ValueError(f"Infrastructure transaction rejected: 'provider' key is missing for pool [{pool_name}].")
        
    driver = PROVIDER_REGISTRY.get(provider.lower().strip())
    if not driver:
        raise ValueError(f"Infrastructure transaction rejected: Cloud vendor '{provider}' is not registered.")

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

    driver = PROVIDER_REGISTRY.get(provider.lower().strip())
    if not driver:
        raise ValueError(f"Infrastructure transaction rejected: Cloud vendor '{provider}' is not registered.")

    client, base_url, headers = await driver.get_client_context(redis_client, tenant_id)

    try:
        if action == "start":

            raw_profiles = config.get("profiles")
            profile_fallback_list = raw_profiles if isinstance(raw_profiles, list) else [raw_profiles]
            log.info(f"Initiating hardware allocation sweeps across priority lanes: {profile_fallback_list}")
            allocated_successfully = False
            for flavor_profile in profile_fallback_list:
                raw_profiles = config.get("profiles")
                profile_fallback_list = raw_profiles if isinstance(raw_profiles, list) else [raw_profiles]
                
                log.info(f"Initiating hardware allocation sweeps across priority lanes: {profile_fallback_list}")
                allocated_successfully = False
                for flavor_profile in profile_fallback_list:
                    log.info(f"Routing allocation payload to [{provider}] on profile: {flavor_profile}")
                    
                    allocated_successfully = await driver.allocate(client, base_url, headers, flavor_profile, config)
                    if allocated_successfully:
                        break
                        
                if not allocated_successfully:
                    log.critical(f"[ORCHESTRATOR-FATAL] All configured priority profiles are completely out of stock on {provider}.")
                    return ""
            
            log.info(f"Awaiting active routing configurations from [{provider}] hypervisor cluster...")
            for attempt in range(1, 91):
                await asyncio.sleep(4)
                polled_id, ip = await driver.discover_state(client, base_url, headers, config)
                if ip:
                    log.info(f"Primary cluster node live and routable via [{provider}] at: {ip}")
                    return str(ip).strip()
                    
                log.warning(f"Synchronizing with cloud hardware fabric managers (Polling check {attempt}/90)...")
                
            log.info(f"Hardware node boot sequence exceeded 6-minute window on {provider}.")
            return ""


        elif action in ["delete", "teardown"]:
            node_id, public_ip = await driver.discover_state(client, base_url, headers, config)
            if node_id == "NONE":
                log.info(f"No active infrastructure footprint found for tenant: {tenant_id}")
                return "SUCCESS"
                
            log.info(f"Dispatching erasure mandate to [{provider}] for resource token: {node_id}")
            
            if await driver.teardown(client, base_url, headers, node_id):
                log.info(f"Infrastructure flattened. Node token {node_id} unlinked successfully.")
                return "SUCCESS"
                
            log.info(f"Hypervisor refused erasure request on [{provider}] for token: {node_id}")
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
            config = await load_workspace_blueprint(redis_client, system_tenant, "burst")
            provider = config.get("provider")
            if provider:
                driver = PROVIDER_REGISTRY.get(provider.lower().strip())
                if driver:
                    log.info(f"Sweeping cloud provider [{provider}] for orphan instances...")
                    client, base_url, headers = await driver.get_client_context(redis_client, system_tenant)
                    try:
                        vm_id, public_ip = await driver.discover_state(client, base_url, headers, config)
                        
                        if vm_id != "NONE" and public_ip:
                            log.info(f"Detected orphan VM {vm_id} at {public_ip}. Healing cache...")
                            cluster_key = f"cluster_state:{system_tenant}:burst"
                            await redis_client.hset(cluster_key, mapping={"status": "active", "ip": public_ip})
                            await redis_client.set(f"active_jobs:{system_tenant}", "0")
                    finally:
                        await client.aclose()
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
                
                if gpu_status in ["cold", "tearing_down"] or tenant_jobs > 0:
                    await redis_client.delete(idle_ticks_key, warned_key)
                    continue
                    
                # Clock Progression Metrics
                current_ticks += SLEEP_INTERVAL
                await redis_client.set(idle_ticks_key, current_ticks)
                log.info(f"Tenant: {tenant_id} | Idle Time: {current_ticks}s / {IDLE_TIMEOUT}s")
                
                if current_ticks >= IDLE_TIMEOUT:
                    log.info(f"Tenant {tenant_id} timed out. Processing infrastructure erasure...")
                    await redis_client.hset(cluster_key, "status", "tearing_down")
                    
                    if await manage_infrastructure_lifecycle(redis_client, "delete", tenant_id, "burst") == "SUCCESS":
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
