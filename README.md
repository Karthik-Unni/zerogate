# ZeroGate

ZeroGate is an open-source, event-driven cross-cloud GPU orchestration fabric. It eliminates unmanaged hardware idle costs in multi-tenant inference (vLLM) pipelines. You no longer have to suffer brutal 5-minute bare-metal cold starts.

Sitting directly between your application gateway and underlying hardware providers, ZeroGate implements a reactive architecture. It securely scales dedicated infrastructure pools to absolute zero the moment tenant demand dries up.


---

## Core Architecture Primitives

* **Automated Scale-to-Zero Daemon**: Continuously evaluates distributed tenant idle-tick registries via background event loops, executing immediate infrastructure erasure to flatten compute bills.
* **Thread-Safe Concurrency Lock Guard**: Implements non-blocking distributed lock coordination over incoming telemetry surges via Redis, cleanly parking requests while underlying hardware scales up.
* **Dynamic Market Arbitrage**: Gracefully intercepts provider spot instance exhaustion events, automatically falling back across priority lanes to standard bare-metal configurations without breaking runtime inference streams.
* **Immutable Relational Billing Ledger**: Features an integrated, real-time relational logging pipeline to calculate token-level utilization metrics and track infrastructure cost savings.

---

## 5-Minute Quick Start (Local Evaluation)

Evaluate ZeroGate's queuing, state boundaries, and automated scaling primitives entirely on your local machine.

By default, the engine boots with an isolated **Mock Mode** turned on (`ZEROGATE_MOCK=True`). This allows you to stress-test the complete orchestration fabric on any hardware (including Apple Silicon or non-GPU laptops) with **zero infrastructure costs, zero provider accounts, and zero local CUDA/NVIDIA driver dependencies**.

### 1. Clone the Fabric Engine

```bash
git clone https://github.com/noah-garner/zerogate
cd zerogate
```

### 2. Configure Your Local Environment

Copy the pre-santized environment template. The default settings are pre-configured to launch the engine in an offline mock layer cleanly:

```bash
cp .env.example .env
```

### 3. Boot the Core Infrastructure Mesh

Launch the ultra-lightweight Alpine service container stack (API Gateway, Kafka event broker streams, Redis state cache, and PostgreSQL billing database):

```bash
docker compose up --build -d
```

*(Verify all services are up and healthy by running `docker compose ps`)*

### 4. Trigger the Multi-Tenant Ingress Stress Test

Fire an automated batch of concurrent prompt streams directly inside the private container network mesh:

```bash
docker compose run --build --rm simulator
```

### 5. Monitor the Telemetry Logs

While the simulator container floods the network, watch the event loops handle the infrastructure expansion, rate-limiting, and scale-down lifecycle phases in real time:

```bash
# Watch the gateway ingest prompts and handle backpressure limits
docker compose logs -f gateway

# Watch the worker lock states, mock compute boot-ups, and SQL ledger commits
docker compose logs -f worker
```

---

## Testing the Automated Scaling & Teardown Loops

To watch ZeroGate handle live cluster expansion and scale-to-zero loops entirely inside the local deployment, you need to overwhelm the default baseline thread pools. Instead of editing source code, you can trigger this directly via your environment configurations:

1. Open your local `.env` file and increase your workload density to breach your burst threshold limit of 15:

    ```ini
    SIM_TOTAL_REQUESTS=20
    SIM_BATCH_SIZE=20
    ```

2. Trigger the automated over-capacity surge container:

    ```bash
    docker compose run --build --rm simulator
    ```

3. Open your second terminal tab and monitor your background worker daemon (`docker compose logs -f worker`). You will watch the engine detect the `Global Pipeline Load: 20`, spin up the simulation cluster drivers, and execute the infrastructure cleanup erasure loop exactly 10 seconds after the batch clears!

> **System Resilience Note:**  
> If you fire a secondary workload surge while a scale-to-zero teardown loop is actively running, the engine will instantly intercept the new traffic metrics, cancel the erasure cycle, and spin up fresh compute pools to process the payload without dropping a single packet.

---

## Core Control Plane API Endpoints

ZeroGate provides high-performance, non-blocking telemetry and state queries over your active workspaces and transaction queues.

### 1. Polling Task Inference Status

Query the real-time lifecycle phase of a specific inference job cached across your distributed state layer.

* **Path**: `Get /v1/status/{request_id}`
* **Authentication**: None (Designed for safe, frictionless frontend/client-side polling without leaking master admin tokens).
* **Verification Command**:

    ```bash
    curl -X GET http://localhost:8000/v1/status/<request_id_from_logs>
    ```

### 2. Fetching Workspace Billing Analytics

Pull real-time relational aggregation directly from the PostgreSQL ledger to track token velocity, queue overhead latencies, and total accumulated dollar-value savings.

* **Path**: `GET /v1/analytics`
* **Required Header**: `X-ZeroGate-Key: {your_workspace_token}`
* **Verification Command**:

    ```bash
    curl -X GET http://localhost:8000/v1/analytics \
        -H "X-ZeroGate-Key: zerogate-alpha-demo"
    ```

> **Systems Engineering Note: Telemetry Aggregation**  
> The `aggregated_idle_tax_saved_usd` metric updates **strictly upon the completion of an infrastructure erasure cycle**. If your workload testing batch does not breach the `BURST_THRESHOLD` parameter (default: 15), the system processes your tasks entirely on the warm baseline buffer layer without provisioning extra burst nodes. Consequently, no cloud waste occurs, and the ledger will accurately report `0.0` until an over-capacity surge actively triggers a spin-up, idle tracking sequence, and a subsequent teardown loop.

---

## API Data Schemas Reference

### Task Status Responses

* **Processing / Scaling Phase**:

    ```json
    {
        "request_id": "73909cd9-3af7-4f20-a209-c5857619c680",
        "status": "processing",
        "infrastructure": { "cluster_slice": "allocated_pool", "vllm_state": "hot_path_stream_engaged" }
    }
    ```

* **Completed Phase**:

    ```json
    {
      "request_id": "73909cd9-3af7-4f20-a209-c5857619c680",
      "status": "completed",
      "metrics": { "execution_duration_seconds": 4.5, "estimated_savings_usd": 0.00022 },
      "prompt": "...",
      "result": "...",
      "message": "Inference lifecycle finished. Idle footprint flattened."
    }
    ```

### Workspace Analytics Response

* **Analytics Phase**:

    ```json
    {
    "workspace_key": "zerogate-alpha-demo",
    "ledger": {
        "total_inferences_processed": 20,
        "total_tokens_generated": 1460,
        "aggregated_idle_tax_saved_usd": 0.00442,
        "average_queue_overhead_ms": 400.1
    },
    "status": "Healthy. Workspace data plane separation verified."
    }
    ```

## Production Infrastructure Tiers

When you are ready to transition past local simulation loops and orchestrate real hardware layers inside your own private cloud setups, toggle `ZEROGATE_MOCK=False` inside your local `.env`.

### Tier 1: Enterprise Bare-Metal Orchestration (Hyperstack Provider)

For high-performance, non-virtualized production workloads. To bypass heavy runtime package installation delays and secure low cold-start latency, this tier utilizes optimized hardware snapshots.

1. Save your pre-configured vLLM execution environment as a **Custom Snapshot Image** inside your Hyperstack console.
2. Update your credentials inside your `.env` configuration file:

    ```ini
    ZEROGATE_MOCK=False
    HYPERSTACK_API_KEY=your_secret_api_key
    HYPERSTACK_ENVIRONMENT_NAME=Default-Canada
    HYPERSTACK_BASE_URL=https://nexgencloud.com
    ```

### Tier 2: Lightweight Container Bursting (RunPod Provider - Coming This Week)

Our active engineering sprint is focused on launching native RunPod container driver hooks. This will allow deploying public vLLM images straight from a standard API request with **zero custom snapshot configurations required**, slashing hypervisor cold-start latency down from 5 minutes to **<40 seconds**. Follow along with our active development tracking inside Issue #1!

---

## Operational Engineering Roadmap

Deep-tech infrastructure is built iteratively. We publish our engineering milestones openly to cultivate transparent collaboration with our core alpha developer network.

* **v0.1.0-alpha (Current)**: Full event-driven kafka consumer gateway,  distributed locks, automated scale-to-zero background daemons, and local evaluation engine.

* **v0.2.0 (Active Sprint)**: Implement fluid cross-cloud pod drivers (RunPod) to leverage container-based GPU scaling, dropping cold starts under 90 seconds (and sub-40 seconds on cached container nodes).

* **v0.3.0 (Production Enterprise Milestone)**: Transition from a push-based proxy router to a pull-based late-binding work-stealing consumer mesh to optimize multi-node execution throughput.

---

## Join as an Alpha Developer

We are selecting **5-10 Core Alpha Developers** building production-grade AI platforms who need to optimize infrastructure utilization, secure multi-cloud fault tolerance, and eliminate unmanaged GPU idle tax.

* **File an Issue**: Found an edge case in our async locking primitives? Open a detailed GitHub issue with your simulator log output.
* **Get Early Enterprise Access**: Reach out directly if you require custom private-cloud deployment scripts or dedicated queue isolation.

Licensed under the [Apache 2.0 License](LICENSE).
