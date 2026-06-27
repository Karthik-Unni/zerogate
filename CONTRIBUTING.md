# Contributing to ZeroGate

First off, thank you for checking out the project! ZeroGate is a high-performance, event-driven control plane built to automate multi-tenant cost-arbitrage and scale-to-zero infrastructure loops for generative AI workloads.

Because we operate at the critical intersection of infrastructure security, fast data streams, and raw multi-cloud orchestration, we maintain an uncompromising standard for engineering aesthetics, security, and runtime physics.

To protect our development velocity, maintainer sanity, and code design, all contributors must strictly adhere to the rules of engagement detailed below. **Unsolicited or non-compliant Pull Requests will be closed immediately without review.**

---

If your code breaks any of these four rules, it cannot and will not be merged into our `main` branch under any circumstances:

## 1. The Async Non-Blocking Mandate

ZeroGate's engine relies on absolute concurrent efficiency (`asyncio`). Every single outbound hypervisor API call, internal telemetry database write, or cache layer lookup must remain strictly non-blocking.

* Any Pull Request that introduces blocking synchronous I/O operations or imports blocking code structures will be rejected immediately.
* **All cloud infrastructure handshakes must be handled natively via `httpx.AsyncClient`.**

## 2. Zero-Dependency Footprint & Vendor Isolation

We keep our `requirements.txt` footprint minimal to protect our multi-tenant runtime environment from supply-chain vulnerabilities, bloat, and dependency conflicts.

* **We do not install proprietary cloud vendor SDK packages (e.g., `hyperstack-sdk`, `boto3`, etc.).** 
* All infrastructure providers must be controlled using raw, low-level HTTP REST or GraphQL payload strings dispatched natively over our clean, internal async network utilities.

## 3. Centralized Blueprint Architecture

We explicitly separate workspace tracking configurations from core background execution daemons.

* Do not introduce transient global shell environment variables to pass infrastructure parameters (like disk limits, or container images) into `.env`.
* All infrastructure capacities, VRAM boundaries, and target array fallbacks must be explicitly managed within our centralized, isolated schema inside `configs.py`.

## 4. Zero Tolerance for AI-Generated Boilerplate Farming

Do not use an LLM context window to blindly spit out unoptimized wrapper blocks or patch files against our issue tracker just to farm open-source contribution metrics. If you use AI to assist your development, you are fully expected to manually audit the code line-by-line to ensure it perfectly respects our system naming conventions, directory paths, and async design patterns.

---

## The Rules of Engagement (How to Get Code Merged)

To ensure a smooth, collaborative contribution lifecycle, follow this exact sequence:

### Step 1: Open an Architectural Issue First

Never open a Pull Request out of the blue. You must open a tracking **Issue** first explaining the bug you found or the driver feature you want to build. This allows the maintainers to review, pivot, and approve the systemic approach before any code is written.

### Step 2: Small, Atomic Commits

Keep your Pull Requests hyper-focused and small. A PR should do exactly one thing (e.g., fix a trailing slash bug, or add a single cloud endpoint mapping block). Massive, multi-file PRs that touch multiple independent layers of the stack simultaneously will be rejected without review.

### Step 3: Local Simulation Testing

All code paths must be fully validated locally using our integrated simulation harness (`ZEROGATE_MOCK=True`) before submission. Your pull request description must include raw terminal output log traces proving that your code cleanly runs through our mock daemons and state grid checkpoints without throwing Python `KeyError` or connection exceptions (429 is okay).

## Code Style Guidelines

* **Logging:** Use our centralized async logger primitive (`ZeroGateLogger`). Never drop standard `print()` statements into production execution tracks.
* **Semantic Commits:** We follow structured commit log tags. Ensure your local branch commits are formatted cleanly: `fix: <description>`, `feat: <description>`, or `refactor: <description>`.

Thank you for helping us build the ultimate cost-optimization engine for the open-source AI community!
