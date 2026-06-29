from engine.drivers.base import BaseCloudDriver
import httpx

class MockDriver(BaseCloudDriver):
    """
    Emergency sandbox driver to intercept 'mock' config tokens for zero-cost internal testing.
    """
    async def get_client_context(self, redis_client, tenant_id: str) -> tuple:
        return httpx.AsyncClient(timeout=10.0), "http://gateway:8000/mock", {}

    async def allocate(self, client, base_url, headers, profile, config) -> bool:
        return True

    async def discover_state(self, client, base_url, headers, config) -> tuple:
        return "mock-node-12345", "gateway"

    async def teardown(self, client, base_url, headers, node_id) -> bool:
        return True