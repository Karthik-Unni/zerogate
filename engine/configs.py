"""
==============================================================================
CONFIGURATION VALUES:

- base: The primary steady-state node lane used to absorb immediate client payloads.
- burst: The elastic surge node lane that scales up on-demand to handle queue traffic.
- mock: A localized offline deployment target used for zero-cost routing verification.

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
==============================================================================
"""
WORKSPACE_CONFIGS = {
    "base": {
        "provider": "runpod",
        "name": "zerogate-base",
        "image": "vllm/vllm-openai:v0.5.4", 
        "profiles": ["NVIDIA GeForce RTX 4090", "NVIDIA L4", "NVIDIA GeForce RTX 5090"]
    },
    "burst": {
        "provider": "runpod",
        "name": "zerogate-burst",
        "image": "vllm/vllm-openai:stable", 
        "profiles": ["NVIDIA GeForce RTX 4090", "NVIDIA L4", "NVIDIA GeForce RTX 5090"]
    },
    "mock": {
        "provider": "mock",
        "name": "zerogate-mock",
        "image": "mock-latest",
        "profiles": ["mock-profile"]
    }
}
"""
MODEL_IMAGE_MATRIX: Autonomic Runtime Compatibility Registry

Acts as the intelligent decoupling layer between user-requested inference 
models and underlying cloud hypervisor container runtimes. 

Why this exists:
    Open-source LLM weight configurations, tokenizer structures, and attention 
    mechanisms (e.g., RoPE scaling formats) introduce breaking upstream Python/Rust 
    dependency disparities across different version releases of inference engines.
    
How it operates:
    The core orchestration plane interceptor loops query this dictionary matrix 
    at runtime using substring signature matching against incoming user payloads. 
    It dynamically mutates the cloud provider's target allocation image tag, 
    bypassing static tenant blueprint settings.
"""
MODEL_IMAGE_MATRIX = {
    "llama-3.1": "vllm/vllm-openai:v0.6.2",
    "llama-3": "vllm/vllm-openai:v0.5.4",
    "mistral": "vllm/vllm-openai:v0.4.2",
    "default": "vllm/vllm-openai:v0.5.4"
}