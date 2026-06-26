import logging

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(name)s] %(message)s", datefmt="%H:%M:%S")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("aioredis").setLevel(logging.WARNING)
logging.getLogger("aiokafka").setLevel(logging.WARNING)

class ZeroGateLogger:
    """
    A worker-safe lazy proxy wrapper for Python's standard logging module.
    
    This class bypasses multi-worker initialization race conditions by 
    preventing logger stream objects from being instantiated globally at 
    the module level during the master process boot/fork phase. 
    
    The live logger stream is fetched on-demand via `logging.getLogger` 
    only when an explicit logging method (e.g., .info(), .error()) is called.
    """
    def __init__(self, tag: str):
        self.tag = tag

    def __getattr__(self, name):
        return getattr(logging.getLogger(self.tag), name)