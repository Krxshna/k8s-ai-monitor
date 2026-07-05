# K8s Monitor — AI-Powered Kubernetes Observability

Ask plain English questions about your cluster. The agent talks to the Kubernetes API,
pulls live data, and replies in human language.

```
Ask: Are all my pods running in production?

✓ api-server      [production]
   Phase:    Running
   Ready:    3/3
   Restarts: 0
   Node:     node-1
   Age:      5d

✗ worker-job      [production]
   Phase:    CrashLoopBackOff
   Ready:    0/1
   Restarts: 14
   Node:     node-2
   Age:      2h
```

---

## What it can do

| Question you might ask | Tool used |
|---|---|
| "List pods in the staging namespace" | `list_pods` |
| "Is nginx healthy?" | `check_pod_health` |
| "Show me the last 200 lines from api-server" | `get_pod_logs` |
| "Restart the worker pod in production" | `restart_pod` |
| "How many replicas does web-deployment have?" | `get_deployment_status` |
| "Are all nodes ready?" | `list_nodes` |

**Restart behaviour:** Kubernetes doesn't restart pods in place. The agent deletes the
pod and Kubernetes brings a fresh one up via its controller (Deployment / StatefulSet /
ReplicaSet). This is the correct, idiomatic way.

---

## Two modes

### Simple agent (`k8s_agent.py`)

The Strands agent calls tools directly. Fast to run, great for development and ad-hoc queries.

```
User → Agent → Kubernetes API → Answer
```

### Temporal agent (`k8s_temporal_agent.py`)

Every operation runs as a Temporal workflow. You get retries, a full audit trail,
and fault tolerance — if the worker crashes mid-run, Temporal replays from where it stopped.

```
User → Client → Temporal Server → Worker → Activities → Kubernetes API → Answer
                                      ↑
                               AI orchestrator
                              (Ollama / Bedrock)
```

---

## Setup

### Prerequisites

- Python 3.10+
- A running Kubernetes cluster (local: `minikube start` or `kind create cluster`)
- `kubectl` configured (`~/.kube/config` pointing at your cluster)
- [Ollama](https://ollama.ai) running locally with a model pulled
- [Temporal CLI](https://docs.temporal.io/cli) for the Temporal agent

### Install

```bash
cd k8s-monitor
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Pull an Ollama model

```bash
ollama pull mistral:7b            # default — ~4 GB
# or
ollama pull llama3.2:latest       # lighter option
```

---

## Running

### Simple agent

```bash
python k8s_agent.py
```

Example queries to try:

```
Ask: List all pods
Ask: Check health of coredns in kube-system
Ask: Show logs from my-app
Ask: Restart broken-pod in staging
Ask: Status of web deployment
Ask: How are the nodes?
```

### Temporal agent

**Terminal 1 — Temporal server**
```bash
temporal server start-dev
```

**Terminal 2 — Worker**
```bash
python k8s_temporal_agent.py worker
```

**Terminal 3 — Interactive client**
```bash
python k8s_temporal_agent.py
```

**Browser — Workflow dashboard**
```
http://localhost:8233
```

The UI shows every workflow run: which activities fired, how long each took,
retry counts, and the exact input/output at every step.

---

## Configuration

All config lives in `config.py` and can be overridden with environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `OLLAMA_HOST` | `http://localhost:11434` | Ollama endpoint |
| `OLLAMA_MODEL` | `mistral:7b` | Model to use |
| `TEMPORAL_HOST` | `localhost:7233` | Temporal server address |
| `K8S_DEFAULT_NAMESPACE` | `default` | Fallback namespace |
| `POD_RESTART_THRESHOLD` | `5` | Restart count that marks a pod unhealthy |

### Switching to AWS Bedrock

1. Set up AWS credentials (`aws configure` or env vars)
2. In `k8s_agent.py`, swap the model:

```python
# Comment out Ollama:
# model = OllamaModel(host=OLLAMA_HOST, model_id=OLLAMA_MODEL)

# Uncomment Bedrock:
from strands.models import BedrockModel
model = BedrockModel(
    model_id="us.anthropic.claude-sonnet-4-20250514-v1:0",
    region_name="us-east-1"
)
```

Same change in `k8s_temporal_agent.py` inside `ai_orchestrator_activity`.

---

## Project layout

```
k8s-monitor/
├── config.py                  # All tunable settings
├── k8s_client.py              # Kubernetes SDK wrapper + data models
├── k8s_agent.py               # Simple Strands agent (direct execution)
├── k8s_temporal_agent.py      # Temporal workflow, activities, worker & client
├── requirements.txt
└── README.md
```

### How the code is layered

```
k8s_agent.py / k8s_temporal_agent.py
        │  calls
        ▼
   k8s_client.py          ← only file that touches the Kubernetes SDK
        │  uses
        ▼
  kubernetes Python SDK   ← talks to your cluster's API server
```

`k8s_client.py` is intentionally kept separate so you can test it without
importing Strands or Temporal at all.

---

## Retry policies (Temporal agent)

Each operation type gets a retry policy matched to its risk level:

| Operation | Max attempts | Backoff |
|---|---|---|
| List pods | 3 | 1 s → 5 s |
| Health check | 3 | 2 s → 10 s |
| Get logs | 2 | 1 s → 5 s |
| Restart pod | 5 | 3 s → 30 s |
| Deployment status | 3 | 1 s → 5 s |
| List nodes | 3 | 1 s → 5 s |
| AI orchestrator | 2 | 1 s → 2 s |

Restarts get the most retries because a transient API blip during a delete
would leave the pod untouched — we want to be sure the call got through.
The orchestrator gets only 2 because a failed LLM call falls back to listing
pods rather than burning quota on retries.

---

## Troubleshooting

**`K8sConnectionError` on startup**
```bash
# Verify kubectl can reach the cluster
kubectl cluster-info
kubectl get pods --all-namespaces
```

**Ollama not responding**
```bash
ollama list                        # confirm model is available
ollama serve                       # start if not running
```

**Temporal worker won't start**
```bash
# Make sure Temporal is running first
temporal server start-dev

# Then start the worker
python k8s_temporal_agent.py worker
```

**`No pods found` even though pods exist**

The agent defaults to the `default` namespace. Be explicit:
```
Ask: List pods in kube-system
Ask: Show pods in production namespace
```

---

## Local cluster quickstart (no real cluster needed)

```bash
# Option A — minikube
brew install minikube
minikube start
kubectl get pods -A            # verify

# Option B — kind
brew install kind
kind create cluster
kubectl get pods -A
```

Then run the agent — it will automatically pick up your kubeconfig.

> Built for learning purposes. Demonstrates AI agents, Temporal workflows, and Kubernetes operations as a hands-on reference.
