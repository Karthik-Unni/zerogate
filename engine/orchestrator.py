"""
ZeroGate Hardware Allocation and Telemetry Provisioning Engine.

This module acts as the explicit resource orchestration abstraction layer,
managing cloud hardware lifecycles, evaluating hardware fallback priorities,
and executing automated pool expansion or scale-to-zero teardown sequences.
"""
import os, json, asyncio
from uuid import uuid4
from engine.logger import ZeroGateLogger
from engine.configs import WORKSPACE_CONFIGS
from engine.drivers.hyperstack import HyperstackDriver
from engine.drivers.runpod import RunPodDriver
from engine.drivers.mock import MockDriver
from engine.configs import MODEL_IMAGE_MATRIX

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

    configs = WORKSPACE_CONFIGS

    return configs.get(pool_name, configs["base"])

async def manage_infrastructure_lifecycle(redis_client, action: str, tenant_id: str, pool_name: str, model_name: str = None) -> str:
    """
    Orchestrates live cloud hardware allocations and resource teardowns.

    Handles hardware profile fallback scheduling arrays, evaluates inventory stock levels,
    polls infrastructure fabric providers until instance environments report
    healthy network status, and cleanly destroys instances during scale-down requests.
    model_name is optional because teardown or prewarm lifecycles don't require a model
    """
    log.info(f"Lifecycle triggered | Tenant: {tenant_id} | Action: {action} | Pool: {pool_name}")
    config = await load_workspace_blueprint(redis_client, tenant_id, pool_name)
    provider = config.get("provider")

    if model_name:
        # Unified Abstraction Provider Pattern
        if provider == "hyperstack":
            matched_image = config.get("image", "ZeroGate-Alpha")
            log.info(f"Hyperstack detected. Using custom image identifier: {matched_image}")
        elif provider == "mock":
            # Handle mock mode cleanly without touching matrix rules
            matched_image = config.get("image")
            log.info(f"Mock mode active. Using local simulation engine snapshot: {matched_image}")
            
        else:
            matched_image = MODEL_IMAGE_MATRIX["default"]
            for signature, image_tag in MODEL_IMAGE_MATRIX.items():
                if signature in model_name:
                    matched_image = image_tag
                    break             
            log.info(f"Runpod detected. Using image identifier: {matched_image}")
   
        # Accessed by our driver during allocation
        config["image"] = matched_image
        config["model_name"] = model_name
        log.info(f"Resolved model '{model_name}' requires runtime engine: {matched_image}")
    
    if not provider:
        raise ValueError(f"Infrastructure transaction rejected: 'provider' key is missing for pool [{pool_name}].")
        
    driver = PROVIDER_REGISTRY.get(provider.lower().strip())
    if not driver:
        raise ValueError(f"Infrastructure transaction rejected: Cloud vendor '{provider}' is not registered.")

    # Must be after confirming driver blueprint configs are valid
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

    client, base_url, headers = await driver.get_client_context(redis_client, tenant_id)

    try:
        if action == "start":
            cluster_key = f"cluster_state:{tenant_id}:{pool_name}"
            node_id, public_ip = await driver.discover_state(client, base_url, headers, config)
            if node_id != "NONE" and public_ip:
                log.info(f"Target pod already exists on ({node_id}) at {public_ip}. Short-circuiting allocation.")
                # Sync the state to Redis so the worker loop can use it immediately
                await redis_client.hset(cluster_key, mapping={"status": "active", "ip": public_ip})
                return public_ip
            
            raw_profiles = config.get("profiles")
            profile_fallback_list = raw_profiles if isinstance(raw_profiles, list) else [raw_profiles]
            log.info(f"Initiating hardware allocation sweeps across priority lanes: {profile_fallback_list}")
            allocated_successfully = False
            for profile in profile_fallback_list:
                log.info(f"Routing allocation payload to [{provider}] on profile: {profile}")
                
                allocated_successfully = await driver.allocate(client, base_url, headers, profile, config)
                
                if allocated_successfully:
                    log.info(f"Successfully secured hardware allocation on target profile: {profile}. Breaking fallback loop.")
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


async def sync_cloud_provider_states(redis_client):
    """
    Executes a single-run initialization sweep on container startup.
    Dynamically scans active multi-tenant workspace blueprints to pre-warm
    baseline anchor infrastructure and tear down leftover orphan burst cards.
    """
    log.info("Initializing dynamic multi-tenant infrastructure sweeps...")
    
    try:
        if os.getenv("ZEROGATE_MOCK") == "True":
            log.info("MOCK mode active. Skipping cloud fabric verification sweeps.")
            return

        async for blueprint_key in redis_client.scan_iter(match="workspace_blueprint:*:*"):
            parts = blueprint_key.split(":")
            if len(parts) < 3:
                continue

            tenant_id = parts[1]
            pool_tier = parts[2]

            # Fetch the active blueprint config map for this pool lane
            config = await load_workspace_blueprint(redis_client, tenant_id, pool_tier)
            provider_token = config.get("provider")
            cluster_key = f"cluster_state:{tenant_id}:{pool_tier}"

            if not provider_token:
                continue

            driver = PROVIDER_REGISTRY.get(provider_token.lower().strip())
            if not driver:
                continue

            client, base_url, headers = await driver.get_client_context(redis_client, tenant_id)
            try:
                node_id, public_ip = await driver.discover_state(client, base_url, headers, config)
                
                if pool_tier == "base":
                    # Track A: Validate, Heal, and Pre-Warm Baseline Pool (Cases 1, 2, 3)
                    if node_id != "NONE":
                        if public_ip:
                            log.info(f"Tenant [{tenant_id}] hot base anchor matched at {public_ip}. Syncing cache...")
                            await redis_client.hset(cluster_key, mapping={"status": "active", "ip": public_ip})
                        else:
                            log.info(f"Tenant [{tenant_id}] hardware found ({node_id}), but network proxy is still initializing. Setting to booting state...")
                            await redis_client.hset(cluster_key, mapping={"status": "booting", "ip": ""})

                    else:
                        log.info(f"Tenant [{tenant_id}] base pool is cold/unaligned. Deploying anchor hardware...")
                        await redis_client.hset(cluster_key, "status", "booting")
                        
                        fresh_base_ip = await manage_infrastructure_lifecycle(redis_client, "start", tenant_id, "base")
                        if fresh_base_ip:
                            await redis_client.hset(cluster_key, mapping={"status": "active", "ip": fresh_base_ip})
                        else:
                            await redis_client.hset(cluster_key, "status", "cold")
                            
                elif pool_tier == "burst":
                    # Track B: Sweep Burst Lane for Leftover Orphan Cards (Case 4)
                    if node_id != "NONE" and public_ip:
                        log.info(f"Tenant [{tenant_id}] orphan burst container caught at {public_ip}. Flagging for scale-to-zero.")
                        await redis_client.hset(cluster_key, mapping={"status": "active", "ip": public_ip})
                        await redis_client.set(f"active_jobs:{tenant_id}", "0")
                        
            except Exception as node_err:
                log.error(f"Failed to synchronize boot state for tenant {tenant_id} on pool {pool_tier}: {node_err}")
            finally:
                await client.aclose()
                
        log.info("Multi-tenant infrastructure verification sweeps successfully completed.")
        
    except Exception as boot_sync_err:
        log.error(f"Startup cloud provider sweep encountered an error: {boot_sync_err}")


async def scale_to_zero_daemon(redis_client):
    """
    A continuous background polling daemon monitoring multi-tenant workspace idleness.
    Resets trackers for hot lanes and flattens burst compute footprints when thresholds expire.
    """
    log.info("Multi-Tenant Scale-to-Zero background daemon engaged.")
    IDLE_TIMEOUT = int(os.getenv("SCALE_TO_ZERO_TIMEOUT", 10))
    WARN_WINDOW = int(os.getenv("SCALE_TO_ZERO_WARN_WINDOW", 5))
    SLEEP_INTERVAL = 2
    WARN_THRESHOLD = IDLE_TIMEOUT - WARN_WINDOW

    while True:
        await asyncio.sleep(SLEEP_INTERVAL)
        try:
            # Scan over any active multi-tenant cluster keyspace across all pools dynamically
            async for cluster_key in redis_client.scan_iter(match="cluster_state:*:*"):
                parts = cluster_key.split(":")
                if len(parts) < 3:
                    continue
                
                tenant_id = parts[1]
                pool_tier = parts[2]
                
                if pool_tier == "base":
                    continue
                
                # Isolate metrics trackers by combining tenant and pool names to avoid cross-contamination
                idle_ticks_key = f"tenant_idle_ticks:{tenant_id}:{pool_tier}"
                warned_key = f"tenant_warned_state:{tenant_id}:{pool_tier}"
                
                current_ticks_raw = await redis_client.get(idle_ticks_key)
                current_ticks = int(current_ticks_raw) if current_ticks_raw else 0
                tenant_jobs = int(await redis_client.get(f"active_jobs:{tenant_id}") or 0)
                
                cluster = await redis_client.hgetall(cluster_key)
                gpu_status = cluster.get("status", "cold")
                
                # Reset guardrail bypass loops if the infrastructure is already inactive or handling traffic
                if gpu_status in ["cold", "tearing_down"] or tenant_jobs > 0:
                    await redis_client.delete(idle_ticks_key, warned_key)
                    continue
                    
                # Clock Metric Progression
                current_ticks += SLEEP_INTERVAL
                await redis_client.set(idle_ticks_key, current_ticks)
                log.info(f"Tenant: {tenant_id} | Tier: {pool_tier} | Idle Time: {current_ticks}s / {IDLE_TIMEOUT}s")
                
                # Evaluate Warning and Timeout Threshold Boundaries
                if current_ticks >= IDLE_TIMEOUT:
                    log.info(f"Tenant {tenant_id} tier [{pool_tier}] timed out. Executing lifecycle erasure...")
                    await redis_client.hset(cluster_key, "status", "tearing_down")
                    
                    if await manage_infrastructure_lifecycle(redis_client, "delete", tenant_id, pool_tier) == "SUCCESS":
                        await redis_client.delete(cluster_key, idle_ticks_key, warned_key)
                    else:
                        await redis_client.hset(cluster_key, "status", "active")
                        
                elif current_ticks >= WARN_THRESHOLD:
                    is_warned = await redis_client.get(warned_key) == "True"
                    if not is_warned:
                        log.warning(f"Warning: Tenant {tenant_id} pool [{pool_tier}] idle for {current_ticks}s! Teardown in {IDLE_TIMEOUT - current_ticks}s!")
                        await redis_client.set(warned_key, "True")
                        
        except Exception as loop_crash_error:
            log.error(f"The scale-to-zero background daemon caught an exception frame: {loop_crash_error}")