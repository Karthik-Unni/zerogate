"""
===============================================================================
CONFIGURATION VALUES:
===============================================================================
- main: The primary steady-state node lane used to absorb immediate client payloads.
- burst: The elastic surge node lane that scales up on-demand to handle queue traffic.
- mock: A localized offline development target used for zero-cost routing verification.

Within each workspace track object, the variables execute as follows:
- name: A descriptive string tag injected into cloud metadata headers for identification.
- provider: An explicit vendor lock string determining which outbound API driver to fire.
- image: The specific virtual machine base snapshot or Docker container image to initialize.
- min_nodes: The absolute minimum capacity floor. Setting this to 0 guarantees total scale-
             to-zero cost collapse when idle. Setting to 1 keeps a prewarmed node hot 24/7.
- max_nodes: The scaling ceiling. Acts as an automated brake to clamp max infrastructure 
             expenses and prevent runaway resource allocation surges.
- profiles: A prioritized list of cloud-specific hardware size tokens. If the primary token 
            is out of stock, the engine falls back down the list within that same cloud.
            Leave at 1 for zero fallback.
Note: You can configure 'base' to use Hyperstack bare-metal virtual machines 
      and 'burst' to leverage RunPod containers natively for multi-cloud load-balancing.
===============================================================================
"""
DEFAULTS = {
    "base": {
        "provider": "runpod",
        "name": "zerogate-base",
        "image": "runpod/vllm:latest",
        "profiles": ["gpu-rtx-4090-1"],
        "min_nodes": 0,
        "max_nodes": 1,
    },
    "burst": {
        "provider": "runpod",
        "name": "zerogate-burst",
        "image": "runpod/vllm:latest",
        "profiles": ["gpu-rtx-4090-1"],
        "min_nodes": 0,
        "max_nodes": 5, 
    },
    "mock": {
        "provider": "mock",
        "name": "zerogate-mock",
        "image": "mock-latest",
        "profiles": ["mock-profile"],
        "min_nodes": 0,
        "max_nodes": 1,
    }
}