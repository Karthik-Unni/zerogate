"""
Validates RunPodDriver allocation, discovery, and teardown logic against
upstream GraphQL edge cases and fallback matrix paths.
"""

import os
import pytest
import httpx
import respx
from unittest.mock import AsyncMock, patch

from engine.drivers.runpod import RunPodDriver

RUNPOD_URL = "https://api.runpod.io/graphql"


def _running_pod_body(pod_id: str = "pod-abc123", status: str = "RUNNING") -> dict:
    return {
        "data": {
            "myself": {
                "pods": [
                    {
                        "id": pod_id,
                        "name": "zerogate-burst-test",
                        "uptimeSeconds": 60,
                        "desiredStatus": status,
                        "runtime": {
                            "ports": [
                                {
                                    "isIpPublic": True,
                                    "ip": "1.2.3.4",
                                    "publicPort": 8000,
                                    "privatePort": 8000,
                                }
                            ]
                        },
                    }
                ]
            }
        }
    }


@pytest.mark.asyncio
async def test_get_client_context_missing_auth_token_raises_value_error():
    """
    When Redis has no stored credentials for the tenant and RUNPOD_API_KEY
    is absent from the environment, get_client_context must raise ValueError
    rather than silently proceeding with no auth.
    """
    driver = RunPodDriver()
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)

    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("RUNPOD_API_KEY", None)
        with pytest.raises(ValueError):
            await driver.get_client_context(redis, "tenant-no-auth")


@pytest.mark.asyncio
@respx.mock
async def test_graphql_out_of_stock_string_in_200_body():
    """
    RunPod returns HTTP 200 with a null pod payload when a profile is out
    of stock. The driver must resolve this to None, not raise or return a
    truthy value.
    """
    driver = RunPodDriver()

    out_of_stock_body = {"data": {"podFindAndDeployOnDemand": None}}
    respx.post(RUNPOD_URL).mock(return_value=httpx.Response(200, json=out_of_stock_body))

    config = {
        "name": "zerogate-burst-test",
        "image": "vllm/vllm-openai:v0.5.4",
        "model_name": "llama-3",
    }

    async with httpx.AsyncClient() as client:
        result = await driver.allocate(client, RUNPOD_URL, {}, "NVIDIA L4", config)

    assert result is None, "Driver must return None when GraphQL body contains no pod ID"


@pytest.mark.asyncio
@respx.mock
async def test_discover_state_ignores_restarting_and_stopped_pods():
    """
    Pods in a transitional RESTARTING or STOPPED state are not routable.
    discover_state must resolve these down to ("NONE", "") rather than
    returning a partial or stale IP.
    """
    driver = RunPodDriver()

    for transitional_status in ["RESTARTING", "STOPPED"]:
        respx.post(RUNPOD_URL).mock(
            return_value=httpx.Response(200, json=_running_pod_body(status=transitional_status))
        )

        config = {"name": "zerogate-burst-test"}
        async with httpx.AsyncClient() as client:
            node_id, ip = await driver.discover_state(client, RUNPOD_URL, {}, config)

        assert (node_id, ip) == ("NONE", ""), (
            f"Pods with desiredStatus={transitional_status} must resolve to NONE, not be treated as routable"
        )


@pytest.mark.asyncio
@respx.mock
async def test_teardown_graphql_error_returns_false():
    """
    When the RunPod GraphQL teardown mutation returns an errors array in
    a 200 response body, teardown() must return False without raising.
    """
    driver = RunPodDriver()

    error_body = {"errors": [{"message": "Pod not found or already terminated."}]}
    respx.post(RUNPOD_URL).mock(return_value=httpx.Response(200, json=error_body))

    async with httpx.AsyncClient() as client:
        result = await driver.teardown(client, RUNPOD_URL, {}, "pod-ghost-999")

    assert result is False, "teardown() must return False on GraphQL error payload"


@pytest.mark.asyncio
@respx.mock
async def test_runpod_driver_complete_scale_lifecycle():
    """
    Exercises the full driver lifecycle in sequence against a single tenant
    session: an empty cluster, a successful allocation, and a successful
    teardown of the resulting pod.
    """
    driver = RunPodDriver()
    config = {
        "name": "zerogate-burst-test",
        "image": "vllm/vllm-openai:v0.5.4",
        "model_name": "llama-3",
    }

    # 1. Discover on an empty cluster — nothing allocated yet
    respx.post(RUNPOD_URL).mock(return_value=httpx.Response(200, json={"data": {"myself": {"pods": []}}}))
    async with httpx.AsyncClient() as client:
        node_id, ip = await driver.discover_state(client, RUNPOD_URL, {}, config)
    assert (node_id, ip) == ("NONE", "")

    # 2. Allocate — captures the new pod's tracking token
    respx.post(RUNPOD_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "data": {
                    "podFindAndDeployOnDemand": {
                        "id": "pod-lifecycle-001",
                        "desiredStatus": "RUNNING",
                        "imageName": config["image"],
                    }
                }
            },
        )
    )
    async with httpx.AsyncClient() as client:
        pod_id = await driver.allocate(client, RUNPOD_URL, {}, "NVIDIA L4", config)
    assert pod_id == "pod-lifecycle-001"

    # 3. Teardown — terminates the allocated pod successfully
    respx.post(RUNPOD_URL).mock(return_value=httpx.Response(200, json={"data": {"podTerminate": True}}))
    async with httpx.AsyncClient() as client:
        result = await driver.teardown(client, RUNPOD_URL, {}, pod_id)
    assert result is True