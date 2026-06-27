import abc
import httpx

class BaseCloudDriver(abc.ABC):
    """
    Unified abstract execution contract for all multi-cloud hypervisor drivers.
    """
    @abc.abstractmethod
    async def get_client_context(self, redis_client, tenant_id: str) -> tuple[httpx.AsyncClient, str, dict]:
        """
        Assembles a fully configured, authorized AsyncClient, base URL, 
        and header dictionary tailored specifically for this cloud provider.
        """
        pass

    @abc.abstractmethod
    async def allocate(self, client: httpx.AsyncClient, base_url: str, headers: dict, profile: str, config: dict) -> bool:
        pass

    @abc.abstractmethod
    async def discover_state(self, client: httpx.AsyncClient, base_url: str, headers: dict, config: dict) -> tuple:
        pass

    @abc.abstractmethod
    async def teardown(self, client: httpx.AsyncClient, base_url: str, headers: dict, node_id: str) -> bool:
        pass
