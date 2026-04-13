# Coldpress Code Walkthrough

This document provides a comprehensive walkthrough of the Coldpress codebase, explaining how the code works and flows through the system.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Package Structure](#package-structure)
3. [Setup Flow (coldpress-setup)](#setup-flow-coldpress-setup)
4. [Job Generation Flow (coldpress)](#job-generation-flow-coldpress)
5. [Key Components Deep Dive](#key-components-deep-dive)
6. [Complete Example Walkthrough](#complete-example-walkthrough)
7. [Data Flow Diagrams](#data-flow-diagrams)

---

## Architecture Overview

Coldpress is a two-piece system for orchestrating AI/HPC workloads on Kubernetes:

1. **`coldpress-setup`** - One-time cluster configuration (runs once per cluster/project)
2. **`coldpress`** - Local job manifest generator (runs for each workload)

**Key Design Principles:**
- **Transparent**: All manifests generated locally for inspection before applying
- **Kubernetes-native**: Uses standard `kubectl`/`oc` commands
- **Validated**: Pydantic models catch configuration errors early
- **Stateless**: No server-side components; pure manifest generation

**Technology Stack:**
- Kubernetes operators: Kueue (job queueing), JobSet (multi-task jobs)
- Python 3.9+ with Click (CLI), PyYAML (config parsing), Pydantic (validation)
- Package manager: `uv` for fast installation

---

## Package Structure

```
coldpress/
├── coldpress/              # Main CLI: Job manifest generator
│   ├── cli.py             # CLI entry point and orchestration
│   ├── generator.py       # JobSet/Service YAML generation
│   ├── allocator.py       # GPU node allocation logic
│   └── script_gen.py      # Bash helper scripts generation
│
├── coldpress_setup/        # Setup CLI: Cluster configuration
│   ├── cli.py             # Setup CLI entry point
│   └── generator.py       # Cluster manifest generation
│
├── coldpress_common/       # Shared validation models
│   └── model.py           # Pydantic models for YAML validation
│
├── discovery/              # Hardware discovery pod templates
├── projects/               # Example project configs
├── examples/               # Example workloads
├── cluster/                # Example cluster configs
└── users/                  # Example user RBAC configs
```

**Entry Points** (defined in [pyproject.toml](pyproject.toml) (lines 19-21)):
- `coldpress` → `coldpress.cli:cli`
- `coldpress-setup` → `coldpress_setup.cli:cli`

---

## Setup Flow (coldpress-setup)

The setup tool configures the cluster in three stages: cluster → project → user.

### 1. Cluster Setup

**Command:** `coldpress-setup apply cluster cluster/ocp-test-nerc-mghpcc.yaml`

**File:** [coldpress_setup/cli.py](coldpress_setup/cli.py) (lines 73-91)

**What it does:**
1. Loads cluster config YAML (contains operator CRDs and Kueue resources)
2. Applies directly to cluster using `oc apply -f <file>`
3. Creates:
   - Kueue operator instance
   - JobSet operator instance
   - ResourceFlavors (one per node with `coldpress.node` label)
   - ClusterQueue (aggregates all node resources)

**Key Code:**
```python
# coldpress_setup/cli.py:209-241
def apply_cluster_config(config_file, output, dry_run):
    kubectl_cmd = _get_kubectl_cmd()  # Detects oc/kubectl
    cmd = [kubectl_cmd, "apply", "-f", config_file]
    result = subprocess.run(cmd, ...)
```

**Example cluster config:** [cluster/ocp-test-nerc-mghpcc.yaml](cluster/ocp-test-nerc-mghpcc.yaml) (lines 1-90)
- Defines 2 nodes (node0, node1) with 4 GPUs each
- Creates ResourceFlavors with node labels
- Creates ClusterQueue with GPU quotas

### 2. Project Setup

**Command:** `coldpress-setup apply project projects/researcher-a.yaml`

**File:** [coldpress_setup/cli.py](coldpress_setup/cli.py) (lines 94-133)

**What it does:**
1. Loads project config ([projects/researcher-a.yaml](projects/researcher-a.yaml) (lines 1-12))
2. Validates with Pydantic ([coldpress_common/model.py](coldpress_common/model.py) (lines 222-239))
3. Generates manifests ([coldpress_setup/generator.py](coldpress_setup/generator.py) (lines 298-343)):
   - **Namespace** with `kueue.openshift.io/managed` label
   - **LocalQueue** pointing to ClusterQueue
   - **PVC** for results storage (500Gi by default)
   - **RBAC** (ServiceAccount, Role, RoleBinding) for pod permissions
4. Applies to cluster via `oc apply -f -` (stdin)

**Key Code:**
```python
# coldpress_setup/generator.py:298-343
def generate_project_manifests(config):
    namespace = config.get("namespace")
    manifests = {"namespaces": [], "kueue": [], "storage": [], "rbac": []}
    
    # Create Namespace
    manifests["namespaces"].append(namespace_manifest)
    
    # Create LocalQueue
    manifests["kueue"].append(generate_local_queue(...))
    
    # Create PVC for results
    manifests["storage"].append(generate_pvc(...))
    
    # Create RBAC (SA, Role, RoleBinding)
    manifests["rbac"].extend(generate_rbac(namespace))
```

**RBAC Permissions** ([coldpress_setup/generator.py](coldpress_setup/generator.py) (lines 148-177)):
- JobSets: create, get, list, watch, delete
- Jobs/Pods: get, list, watch
- Pods: create, delete (for helper pods)
- Pod logs/exec: view and execute
- Services/ConfigMaps: full CRUD

### 3. User Setup

**Command:** `coldpress-setup apply user users/coldpress-user.yaml`

**File:** [coldpress_setup/cli.py](coldpress_setup/cli.py) (lines 136-175)

**What it does:**
1. Loads user config (`users/coldpress-user.yaml`)
2. Validates username and namespaces list ([coldpress_common/model.py](coldpress_common/model.py) (lines 241-254))
3. Generates RoleBindings ([coldpress_setup/generator.py](coldpress_setup/generator.py) (lines 199-231))
4. Grants user access to `coldpress-user-role` in each namespace

**Key Code:**
```python
# coldpress_setup/generator.py:199-231
def generate_user_rbac(username, namespaces):
    rbac = []
    for namespace in namespaces:
        binding = {
            "kind": "RoleBinding",
            "roleRef": {"name": "coldpress-user-role"},
            "subjects": [{"kind": "User", "name": username}]
        }
        rbac.append(binding)
    return rbac
```

---

## Job Generation Flow (coldpress)

The main workflow for generating and running jobs.

### Entry Point: `coldpress generate`

**Command:** `coldpress generate --config examples/pytorch_ddp_training/config.yaml`

**File:** [coldpress/cli.py](coldpress/cli.py) (lines 224-375)

**Complete Flow:**

#### Step 1: Load and Validate Config
**Location:** [coldpress/cli.py](coldpress/cli.py) (lines 29-51)

```python
def _load_and_validate_config(config_path, ...):
    # Load config.yaml
    with open(config_path, "r") as f:
        config_data = yaml.safe_load(f)
    
    # Validate with Pydantic
    validated_config = validate_config(config_data)  # coldpress_common/model.py:199-211
    
    # Extract values
    project = project_override or validated_config.project
    discovery = discovery_override or validated_config.discovery
    output_name = output_override or validated_config.output
```

**Example config:** [examples/pytorch_ddp_training/config.yaml](examples/pytorch_ddp_training/config.yaml) (lines 1-9)
- `project: researcher-a` - which namespace to use
- `discovery: user_snapshot` - hardware discovery template
- `output: ddp-training-job` - output directory name
- `files: [train.py, model_config.json]` - files to mount via ConfigMap

#### Step 2: Load and Validate Task Specs
**Location:** [coldpress/cli.py](coldpress/cli.py) (lines 53-75)

```python
def _load_task_specs(config_dir):
    # Auto-discover job-spec.yaml in same directory as config
    job_spec_path = os.path.join(config_dir, "job-spec.yaml")
    
    # Load all documents (supports multi-task workflows)
    task_specs_data = list(yaml.safe_load_all(f))
    
    # Validate each task
    validated_tasks = validate_task_specs(task_specs_data)  # coldpress_common/model.py:302-323
```

**Example job-spec:** [examples/pytorch_ddp_training/job-spec.yaml](examples/pytorch_ddp_training/job-spec.yaml) (lines 1-42)
- Container image, command, args
- Resource requests (GPUs, memory, CPU)
- Volumes (results PVC, shared memory)
- Environment variables

**Validation** ([coldpress_common/model.py](coldpress_common/model.py) (lines 139-194)):
- Ensures `containers` array exists
- Validates resource specs
- Checks endpoint blocking has health_check or readinessProbe
- Allows both new nested format and legacy flat format

#### Step 3: Load Project Config
**Location:** [coldpress/cli.py](coldpress/cli.py) (lines 84-104)

```python
def _load_project_config(project_name):
    # Load project config from standard directory
    project_config_file = os.path.join(PROJECT_DIR, f"{project_name}.yaml")
    
    # Validate
    validated_project = validate_project_config(project_config_data)
    
    # Extract namespace
    namespace = validated_project.namespace
```

#### Step 4: Prepare ConfigMap Files
**Location:** [coldpress/cli.py](coldpress/cli.py) (lines 106-125)

```python
def _prepare_configmap_files(config_dir, files_list, job_name):
    configmap_name = f"{job_name}-files"
    file_data = {}
    
    # Read each file content
    for file_name in files_list:
        file_path = os.path.join(config_dir, file_name)
        with open(file_path, "r") as f:
            file_data[file_name] = f.read()
    
    return {"name": configmap_name, "files": [...], "data": {...}}
```

Files are mounted at `/workspace/{filename}` in containers ([coldpress/generator.py](coldpress/generator.py) (lines 410-425)).

#### Step 5: Allocate Nodes
**Location:** [coldpress/cli.py](coldpress/cli.py) (lines 127-151)

**Manual Allocation:**
```bash
coldpress generate --config config.yaml --node 0 --node 1
```

**Automatic Allocation:**
```python
def _allocate_nodes_for_tasks(task_specs, manual_nodes):
    for task_id, task in enumerate(task_specs):
        # Sum GPU requests across all containers
        req_gpus = sum(container["resources"]["requests"]["nvidia.com/gpu"])
        req_nics = task.get("roce_nics", 0)
        
        # Find best node
        allocated_node = allocate_node(req_gpus, req_nics)  # allocator.py:260-293
        node_assignments[task_id] = int(allocated_node)
```

**Node Allocation Algorithm** ([coldpress/allocator.py](coldpress/allocator.py) (lines 260-293)):

1. **Get Available Nodes** ([allocator.py](allocator.py) (lines 30-72)):
   - Query all nodes with `coldpress.node` label
   - Extract GPU count from allocatable resources

2. **Calculate Demand Score** ([allocator.py](allocator.py) (lines 225-258)):
   - Fetch Kueue ClusterQueue status
   - Get actual GPU usage from running pods ([allocator.py](allocator.py) (lines 74-136))
   - Score = (used_gpus / required_gpus) × 10 + other factors
   - Lower score = less loaded = better choice

3. **Select Best Node**:
   - Filter nodes with enough GPUs
   - Pick node with lowest demand score

**Example:**
```
Task requires 2 GPUs
Node 0: 4 GPUs total, 2 in use → score = (2/2)*10 = 10
Node 1: 4 GPUs total, 0 in use → score = (0/2)*10 = 0
→ Allocates to Node 1
```

#### Step 6: Generate JobSet Manifest
**Location:** [coldpress/generator.py](coldpress/generator.py) (lines 192-275)

This is the core manifest generation logic.

```python
def generate_jobset(job_spec, node_assignments):
    # 1. Generate unique base directory for results
    base_dir = generate_base_dir(namespace, job_id)  # e.g., "researcher-a/coldpress_results/ddp-training-a1b2c3d4-20260413_143022"
    
    # 2. Preprocess tasks
    for task in tasks:
        _infer_blocking_type_and_health_check(task, ...)  # generator.py:52-70
    _substitute_dns_in_args(tasks, ...)  # generator.py:72-91
    
    # 3. Build initialization jobs
    init_jobs = [
        build_mkdir_job(...),         # Creates base directory in PVC
        build_discovery_job(...)      # Optional: runs hardware discovery
    ]
    
    # 4. Build task jobs
    for task_id, task in enumerate(tasks):
        container_spec = build_container_spec(...)   # generator.py:369-408
        pod_spec = build_pod_spec(...)              # generator.py:488-528
        
        replicated_job = {
            "name": f"task-{task_id}",
            "replicas": 1,
            "template": {"spec": {"template": pod_spec}},
            "dependsOn": [...]  # Dependencies on previous jobs
        }
    
    # 5. Assemble final JobSet
    jobset = {
        "apiVersion": "jobset.x-k8s.io/v1alpha2",
        "kind": "JobSet",
        "metadata": {
            "labels": {"kueue.x-k8s.io/queue-name": f"local-queue-{namespace}"},
            "annotations": {"coldpress.io/base-dir": base_dir}
        },
        "spec": {
            "suspend": True,  # Kueue will unsuspend when resources available
            "replicatedJobs": init_jobs + task_jobs
        }
    }
```

**Task Dependencies** ([generator.py](generator.py) (lines 176-188)):
- **Completion blocking**: Wait for job to complete (status: Complete)
- **Endpoint blocking**: Wait for service to be ready (status: Ready)

**Example dependency chain:**
```
mkdir (Complete) → discovery (Complete) → task-0 (endpoint/Ready) → task-1 (completion/Complete)
```

**Service Generation** ([generator.py](generator.py) (lines 531-561)):
When a task has endpoint blocking, a Kubernetes Service is created:
```python
service = {
    "metadata": {"name": f"s-{job_id}-{task_id}"},
    "spec": {
        "selector": {"app": f"task-{task_id}"},
        "ports": [{"port": port}]
    }
}
```

**DNS Substitution** ([generator.py](generator.py) (lines 72-91)):
Task args can reference other tasks by name:
```yaml
args:
  - --server-url=http://vllm-server:8000
```
Gets rewritten to:
```
--server-url=http://s-benchmark-job-0.researcher-a.svc:8000
```

#### Step 7: Generate Bash Helper Scripts
**Location:** [coldpress/script_gen.py](coldpress/script_gen.py) (lines 1-390)

Five scripts are generated:

1. **`run.sh`** ([script_gen.py](script_gen.py) (lines 7-64)):
   ```bash
   oc create configmap {job}-files --from-file=train.py --from-file=config.json
   oc apply -f jobset.yaml
   oc apply -f services.yaml
   oc wait --for=condition=complete jobset/{job} --timeout=1h
   ```

2. **`monitor.sh`** ([script_gen.py](script_gen.py) (lines 116-140)):
   ```bash
   watch -n 2 "oc get jobset,job,pod -l coldpress/gid={job}"
   ```

3. **`logs.sh`** ([script_gen.py](script_gen.py) (lines 142-242)):
   - Creates helper pod with PVC mounted
   - Fetches logs from all job pods
   - Saves to `{base_dir}/logs/` in PVC
   - Creates `combined.log` with all logs

4. **`explore.sh`** ([script_gen.py](script_gen.py) (lines 245-337)):
   - Creates interactive helper pod
   - Opens shell at `/data/{base_dir}`
   - Auto-cleans up pod on exit

5. **`cleanup.sh`** ([script_gen.py](script_gen.py) (lines 67-113)):
   ```bash
   oc delete jobset/{job}
   oc delete services -l coldpress/gid={job}
   oc delete configmap/{job}-files
   oc delete pods -l app=coldpress-explorer
   ```

#### Step 8: Write Output Files
**Location:** [coldpress/cli.py](coldpress/cli.py) (lines 153-222)

```python
def _write_output_files(output_dir, jobset, services, ...):
    os.makedirs(output_dir, exist_ok=True)
    
    # Write JobSet YAML
    with open(f"{output_dir}/jobset.yaml", "w") as f:
        f.write(jobset_to_yaml(jobset))
    
    # Write Services YAML (if any)
    if services:
        with open(f"{output_dir}/services.yaml", "w") as f:
            f.write(services_to_yaml(services))
    
    # Copy ConfigMap files
    shutil.copy(src_path, dst_path)
    
    # Write metadata.json
    json.dump(metadata, f, indent=2)
    
    # Generate bash scripts
    write_scripts(output_dir, job_name, namespace, ...)
```

**Output Structure:**
```
output/ddp-training-job/
├── jobset.yaml           # Main JobSet manifest
├── services.yaml         # Services (if endpoint blocking used)
├── metadata.json         # Generation metadata
├── train.py              # ConfigMap files (copied)
├── model_config.json
├── run.sh                # Apply and wait
├── monitor.sh            # Watch progress
├── logs.sh               # Capture logs
├── explore.sh            # Interactive shell
└── cleanup.sh            # Delete resources
```

---

## Key Components Deep Dive

### Container Spec Builder

**Location:** [coldpress/generator.py](coldpress/generator.py) (lines 369-408)

Builds the container spec for each task:

```python
def build_container_spec(task, task_id, job_id, namespace, ...):
    # Extract container config (supports both new and legacy formats)
    config = _extract_container_config(task)  # generator.py:277-306
    
    container = {
        "name": "main",
        "image": config["image"],
        "volumeMounts": _build_volume_mounts(task, task_id, base_dir),  # generator.py:325-347
        "resources": _build_container_resources(config["resources"], config["gpus"])  # generator.py:308-323
    }
    
    # Add optional fields
    if config["working_dir"]: container["workingDir"] = ...
    if config["command"]: container["command"] = shlex.split(...)
    if config["args"]: container["args"] = ...
    if config["env"]: container["env"] = ...
    
    # Add readiness probe for endpoint blocking
    if task.get("blocking") == "endpoint":
        container["readinessProbe"] = _build_readiness_probe(task)  # generator.py:350-367
```

**Volume Mounts:**
- Always mounts results PVC at `/mnt/coldpress-data`
- If task has `volumes: [{name: results, mount: /results}]`, adds subPath mount
- If task has `volumes: [{name: dshm, type: emptyDir, mount: /dev/shm}]`, creates emptyDir volume

### Pod Spec Builder

**Location:** [coldpress/generator.py](coldpress/generator.py) (lines 488-528)

```python
def build_pod_spec(task, task_id, job_id, node_id, container_spec, ...):
    volumes = [{"name": "coldpress-data", "persistentVolumeClaim": {...}}]
    
    # Add ConfigMap volume if files specified
    _add_configmap_volume(volumes, container_spec, configmap_info)  # generator.py:410-425
    
    # Add task volumes (results, emptyDir, etc.)
    _add_task_volumes(volumes, container_spec, task, base_dir)  # generator.py:428-456
    
    # Add host path mounts if specified
    _add_sys_mounts(volumes, container_spec, task)  # generator.py:458-474
    
    pod_spec = {
        "metadata": {
            "labels": {
                "app": f"task-{task_id}",
                "coldpress/gid": job_id  # Used for service selectors and cleanup
            }
        },
        "spec": {
            "nodeSelector": {"coldpress.node": node_id},  # Pin to allocated node
            "restartPolicy": "Never",
            "volumes": volumes,
            "containers": [container_spec]
        }
    }
    
    # Apply pod options
    if task.get("tolerate_all"): pod_spec["spec"]["tolerations"] = [{"operator": "Exists"}]
    if task.get("network_mode") == "host": pod_spec["spec"]["hostNetwork"] = True
    if task.get("privileged"): container_spec["securityContext"] = {"privileged": True}
```

### Discovery Job

**Location:** [coldpress/generator.py](coldpress/generator.py) (lines 614-692)

Discovery jobs run hardware benchmarks and save results to PVC:

```python
def build_discovery_job(template_path, base_dir, data_pvc_name, node_id):
    # Load discovery template (e.g., discovery/user_snapshot.yaml)
    with open(template_path, "r") as f:
        template = yaml.safe_load(f)
    
    container = template["spec"]["containers"][0].copy()
    
    # Update volume mounts to write to base_dir
    container["volumeMounts"] = [
        {"name": "storage", "mountPath": "/tmp/result", "subPath": base_dir}
    ]
    
    # Rename output file to discovery_{template_name}.json
    template_name = os.path.basename(template_path).replace(".yaml", "")
    rename_cmd = f"\nmv /tmp/result/discovery.json /tmp/result/discovery_{template_name}.json"
    container["args"][0] += rename_cmd
    
    return {
        "name": "discovery",
        "template": {
            "spec": {
                "nodeSelector": {"coldpress.node": str(node_id)},  # Run on same node as task
                "containers": [container],
                "volumes": [{"name": "storage", "persistentVolumeClaim": {...}}]
            }
        }
    }
```

**Result Location:**
```
/data/researcher-a/coldpress_results/ddp-training-a1b2c3d4-20260413_143022/discovery_user_snapshot.json
```

### Base Directory Generation

**Location:** [coldpress/generator.py](coldpress/generator.py) (lines 32-50)

Each job run gets a unique directory in the PVC:

```python
def generate_base_dir(namespace, job_name):
    # Generate 8-char hex uid from timestamp
    now = datetime.now(timezone.utc)
    uid = hashlib.md5(f"{job_name}{now.isoformat()}".encode()).hexdigest()[:8]
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    
    return f"{namespace}/coldpress_results/{job_name}-{uid}-{timestamp}"
    # Example: "researcher-a/coldpress_results/ddp-training-a1b2c3d4-20260413_143022"
```

This ensures:
- Multiple runs don't overwrite each other
- Results are organized by namespace and job name
- Easy to identify when a run happened

---

## Complete Example Walkthrough

Let's trace a complete PyTorch DDP training job from start to finish.

### Setup Phase (One-time)

#### 1. Cluster Admin: Apply Cluster Config

```bash
coldpress-setup apply cluster cluster/ocp-test-nerc-mghpcc.yaml
```

**What happens:**
- [coldpress_setup/cli.py](coldpress_setup/cli.py) (lines 73-91) loads the cluster config
- Config is applied directly to cluster ([cli.py](cli.py) (lines 224))
- Creates Kueue/JobSet operators, ResourceFlavors (node0, node1), ClusterQueue

**Verification:**
```bash
oc get resourceflavors
oc get clusterqueues
```

#### 2. Cluster Admin: Create Project

```bash
coldpress-setup apply project projects/researcher-a.yaml
```

**Config (`projects/researcher-a.yaml`):**
```yaml
namespace: researcher-a
cluster_queue: cluster-queue-test
storage_class: nfs-csi
storage:
  results: researcher-a-storage
  size: 500Gi
```

**What happens:**
- [coldpress_setup/cli.py](coldpress_setup/cli.py) (lines 94-133) loads and validates project config
- [coldpress_setup/generator.py](coldpress_setup/generator.py) (lines 298-343) generates manifests:
  - Namespace `researcher-a`
  - LocalQueue pointing to ClusterQueue
  - PVC `researcher-a-storage` (500Gi NFS)
  - ServiceAccount, Role, RoleBinding
- Applied to cluster via `oc apply -f -`

**Created Resources:**
```bash
oc get namespace researcher-a
oc get localqueue -n researcher-a
oc get pvc -n researcher-a
oc get role,rolebinding -n researcher-a
```

#### 3. Cluster Admin: Grant User Access

```bash
coldpress-setup apply user users/coldpress-user.yaml
```

**Config (`users/coldpress-user.yaml`):**
```yaml
username: asanaullah@bu.edu
namespaces:
  - researcher-a
```

**What happens:**
- [coldpress_setup/generator.py](coldpress_setup/generator.py) (lines 199-231) generates RoleBinding
- Grants `asanaullah@bu.edu` access to `coldpress-user-role` in `researcher-a` namespace

### Job Generation Phase

#### 4. User: Generate Job Manifest

```bash
coldpress generate --config examples/pytorch_ddp_training/config.yaml
```

**Config Files:**

`examples/pytorch_ddp_training/config.yaml`:
```yaml
project: researcher-a
discovery: user_snapshot
output: ddp-training-job
files:
  - train.py
  - model_config.json
```

`examples/pytorch_ddp_training/job-spec.yaml`:
```yaml
name: ddp-training
tolerate_all: true
containers:
  - name: training
    image: pytorch/pytorch:2.2.0-cuda12.1-cudnn8-runtime
    workingDir: /workspace
    command: ["python", "-m", "torch.distributed.run"]
    args:
      - --nproc_per_node=2
      - --nnodes=1
      - train.py
      - --epochs=50
      - --output-dir=/results/checkpoints
    resources:
      requests:
        nvidia.com/gpu: "2"
        memory: "16Gi"
        cpu: "8"
volumes:
  - name: results
    mount: /results
  - name: dshm
    type: emptyDir
    medium: Memory
    sizeLimit: 16Gi
    mount: /dev/shm
```

**Execution Flow:**

1. **Load config** ([coldpress/cli.py](coldpress/cli.py) (lines 277-279)):
   - Reads `config.yaml`
   - Validates with [coldpress_common/model.py](coldpress_common/model.py) (lines 270-283)
   - Extracts: project=researcher-a, discovery=user_snapshot, output=ddp-training-job

2. **Load task specs** ([coldpress/cli.py](coldpress/cli.py) (lines 280)):
   - Reads `job-spec.yaml` from same directory
   - Validates with [coldpress_common/model.py](coldpress_common/model.py) (lines 302-323)
   - Creates TaskSpec object

3. **Load project config** ([coldpress/cli.py](coldpress/cli.py) (lines 284)):
   - Reads `projects/researcher-a.yaml`
   - Extracts namespace and storage settings

4. **Prepare ConfigMap** ([coldpress/cli.py](coldpress/cli.py) (lines 304-307)):
   - Reads `train.py` and `model_config.json`
   - Prepares data dict for ConfigMap

5. **Allocate node** ([coldpress/cli.py](coldpress/cli.py) (lines 322-336)):
   - Task requires 2 GPUs
   - Queries cluster for nodes with `coldpress.node` label
   - Checks GPU usage via Kueue and actual pods
   - Selects node with lowest demand score
   - **Output:** `Task 0 (ddp-training) → Node 1 (GPUs: 2)`

6. **Generate JobSet** ([coldpress/generator.py](coldpress/generator.py) (lines 192-275)):
   - Creates base_dir: `researcher-a/coldpress_results/ddp-training-a1b2c3d4-20260413_143022`
   - Builds init jobs: mkdir, discovery
   - Builds task job with container spec
   - Adds dependencies: mkdir → discovery → task-0

7. **Write output** ([coldpress/cli.py](coldpress/cli.py) (lines 342-351)):
   ```
   output/ddp-training-job/
   ├── jobset.yaml
   ├── metadata.json
   ├── train.py
   ├── model_config.json
   ├── run.sh
   ├── monitor.sh
   ├── logs.sh
   ├── explore.sh
   └── cleanup.sh
   ```

**Generated JobSet Structure:**
```yaml
apiVersion: jobset.x-k8s.io/v1alpha2
kind: JobSet
metadata:
  name: ddp-training
  namespace: researcher-a
  labels:
    kueue.x-k8s.io/queue-name: local-queue-researcher-a
  annotations:
    coldpress.io/base-dir: researcher-a/coldpress_results/ddp-training-a1b2c3d4-20260413_143022
spec:
  suspend: true  # Kueue will unsuspend
  replicatedJobs:
    - name: mkdir  # Creates base directory
      template:
        spec:
          template:
            spec:
              containers:
                - name: mkdir
                  image: ubi9/ubi-minimal
                  command: ["sh", "-c", "mkdir -p /data/{base_dir}"]
    
    - name: discovery  # Runs hardware benchmark
      dependsOn:
        - name: mkdir
          status: Complete
      template:
        spec:
          template:
            spec:
              nodeSelector:
                coldpress.node: "1"
              containers:
                - name: discovery
                  volumeMounts:
                    - name: storage
                      mountPath: /tmp/result
                      subPath: researcher-a/coldpress_results/ddp-training-a1b2c3d4-20260413_143022
    
    - name: task-0  # Main training task
      dependsOn:
        - name: discovery
          status: Complete
      template:
        spec:
          template:
            metadata:
              labels:
                app: task-0
                coldpress/gid: ddp-training
            spec:
              nodeSelector:
                coldpress.node: "1"  # Allocated node
              tolerations:
                - operator: Exists
              containers:
                - name: main
                  image: pytorch/pytorch:2.2.0-cuda12.1-cudnn8-runtime
                  workingDir: /workspace
                  command: ["python", "-m", "torch.distributed.run"]
                  args:
                    - --nproc_per_node=2
                    - --nnodes=1
                    - train.py
                    - --epochs=50
                    - --output-dir=/results/checkpoints
                  resources:
                    requests:
                      nvidia.com/gpu: "2"
                      memory: "16Gi"
                      cpu: "8"
                    limits:
                      nvidia.com/gpu: "2"
                      memory: "16Gi"
                  env:
                    - name: NCCL_DEBUG
                      value: INFO
                  volumeMounts:
                    - name: coldpress-data
                      mountPath: /mnt/coldpress-data
                    - name: coldpress-data
                      mountPath: /results
                      subPath: researcher-a/coldpress_results/ddp-training-a1b2c3d4-20260413_143022
                    - name: configmap-files
                      mountPath: /workspace/train.py
                      subPath: train.py
                    - name: configmap-files
                      mountPath: /workspace/model_config.json
                      subPath: model_config.json
                    - name: dshm
                      mountPath: /dev/shm
              volumes:
                - name: coldpress-data
                  persistentVolumeClaim:
                    claimName: researcher-a-storage
                - name: configmap-files
                  configMap:
                    name: ddp-training-files
                - name: dshm
                  emptyDir:
                    medium: Memory
                    sizeLimit: 16Gi
```

#### 5. User: Run Job

```bash
cd output/ddp-training-job/
./run.sh
```

**Script Execution:**

1. Creates ConfigMap:
   ```bash
   oc create configmap ddp-training-files \
     --from-file=train.py \
     --from-file=model_config.json \
     -n researcher-a
   ```

2. Applies JobSet:
   ```bash
   oc apply -f jobset.yaml
   ```

3. Waits for completion:
   ```bash
   oc wait --for=condition=complete jobset/ddp-training \
     -n researcher-a --timeout=1h
   ```

**What happens in the cluster:**

1. **Kueue receives JobSet** (suspended)
2. **Kueue checks resources** against ClusterQueue quotas
3. **Kueue unsuspends JobSet** when resources available
4. **JobSet controller creates Jobs** (mkdir, discovery, task-0)
5. **Job controller creates Pods**
6. **Kubernetes scheduler places pods** on nodes with `coldpress.node` label
7. **Pods run in sequence** (mkdir → discovery → task-0)

**Job Execution:**
- **mkdir pod**: Creates directory, completes
- **discovery pod**: Runs GPU benchmark, saves JSON, completes
- **task-0 pod**: 
  - Pulls PyTorch image
  - Mounts ConfigMap files to /workspace
  - Mounts PVC results volume to /results
  - Runs distributed training
  - Saves checkpoints to /results/checkpoints
  - Completes

#### 6. User: Monitor Progress

```bash
./monitor.sh
```

**Shows live view:**
```
NAME                        STATUS
jobset/ddp-training        Running

NAME                        COMPLETIONS
job/ddp-training-mkdir-0   1/1
job/ddp-training-discovery-0  1/1
job/ddp-training-task-0-0  0/1

NAME                              READY   STATUS
pod/ddp-training-mkdir-0-abc123   0/1     Completed
pod/ddp-training-discovery-0-def456  0/1  Completed
pod/ddp-training-task-0-0-ghi789     1/1  Running
```

#### 7. User: Capture Logs

```bash
./logs.sh
```

**What happens** ([coldpress/script_gen.py](coldpress/script_gen.py) (lines 142-242)):

1. Creates helper pod with PVC mounted
2. Fetches logs from all job pods
3. Saves to PVC at `{base_dir}/logs/`
4. Creates combined.log
5. Cleans up helper pod

**Result:**
```
/data/researcher-a/coldpress_results/ddp-training-a1b2c3d4-20260413_143022/logs/
├── ddp-training-task-0-0-ghi789.log
└── combined.log
```

#### 8. User: Explore Results

```bash
./explore.sh
```

**What happens** ([coldpress/script_gen.py](coldpress/script_gen.py) (lines 245-337)):

1. Creates interactive helper pod
2. Mounts PVC at /data
3. Opens shell in pod
4. User can browse results
5. Cleanup on exit

**Session:**
```bash
sh-5.1$ cd /data/researcher-a/coldpress_results/ddp-training-a1b2c3d4-20260413_143022
sh-5.1$ ls -lh
total 4.0K
-rw-r--r-- 1 root root 2.3K Apr 13 14:31 discovery_user_snapshot.json
drwxr-xr-x 2 root root   64 Apr 13 14:35 checkpoints
drwxr-xr-x 2 root root   96 Apr 13 14:40 logs

sh-5.1$ cat checkpoints/training_stats.json
{"epoch": 50, "loss": 0.023, "accuracy": 0.98}

sh-5.1$ exit
```

#### 9. User: Cleanup

```bash
./cleanup.sh
```

**What happens:**
- Deletes JobSet (cascades to Jobs and Pods)
- Deletes Services (if any)
- Deletes ConfigMap
- Deletes helper pods
- **Results remain in PVC**

---

## Data Flow Diagrams

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        User's Machine                        │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  coldpress-setup                    coldpress                │
│  ┌────────────┐                    ┌──────────────┐         │
│  │ Config YAML│                    │  Config YAML │         │
│  └─────┬──────┘                    └──────┬───────┘         │
│        │                                   │                  │
│        ▼                                   ▼                  │
│  ┌────────────┐                    ┌──────────────┐         │
│  │  Validate  │                    │   Validate   │         │
│  │  (Pydantic)│                    │  (Pydantic)  │         │
│  └─────┬──────┘                    └──────┬───────┘         │
│        │                                   │                  │
│        ▼                                   ▼                  │
│  ┌────────────┐                    ┌──────────────┐         │
│  │ Generate   │                    │   Allocate   │         │
│  │ Manifests  │                    │    Nodes     │         │
│  └─────┬──────┘                    └──────┬───────┘         │
│        │                                   │                  │
│        │                                   ▼                  │
│        │                            ┌──────────────┐         │
│        │                            │   Generate   │         │
│        │                            │   JobSet     │         │
│        │                            └──────┬───────┘         │
│        │                                   │                  │
│        │                                   ▼                  │
│        │                            ┌──────────────┐         │
│        │                            │   Generate   │         │
│        │                            │   Scripts    │         │
│        │                            └──────┬───────┘         │
│        │                                   │                  │
└────────┼───────────────────────────────────┼─────────────────┘
         │ oc apply                          │ ./run.sh
         ▼                                   ▼
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                        │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   Kueue      │    │   JobSet     │    │  Kubernetes  │  │
│  │  Operator    │◄───┤   Operator   │◄───┤  Scheduler   │  │
│  └──────┬───────┘    └──────┬───────┘    └──────────────┘  │
│         │                   │                                │
│         │ Queues jobs       │ Creates Jobs                   │
│         ▼                   ▼                                │
│  ┌──────────────────────────────────────┐                   │
│  │              Pods                    │                   │
│  ├──────────────────────────────────────┤                   │
│  │ ┌────┐ ┌────┐ ┌────┐                │                   │
│  │ │Node│ │Node│ │Node│                │                   │
│  │ │ 0  │ │ 1  │ │ 2  │                │                   │
│  │ └────┘ └────┘ └────┘                │                   │
│  └──────────────────────────────────────┘                   │
│                    │                                          │
│                    │ Writes results                          │
│                    ▼                                          │
│         ┌──────────────────┐                                 │
│         │       PVC        │                                 │
│         │  (NFS Storage)   │                                 │
│         └──────────────────┘                                 │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### Job Generation Flow

```
config.yaml + job-spec.yaml
         │
         ▼
┌────────────────────┐
│ Load & Validate    │  coldpress/cli.py:277-280
│ - Config           │  coldpress_common/model.py:270-283
│ - Task Specs       │  coldpress_common/model.py:302-323
│ - Project Config   │  coldpress_common/model.py:286-300
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ Prepare ConfigMap  │  coldpress/cli.py:304-307
│ - Read files       │
│ - Build data dict  │
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ Allocate Nodes     │  coldpress/allocator.py:260-293
│ - Query cluster    │  coldpress/allocator.py:30-72
│ - Calculate scores │  coldpress/allocator.py:225-258
│ - Select best node │
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ Generate JobSet    │  coldpress/generator.py:192-275
│ - Build base_dir   │  coldpress/generator.py:32-50
│ - Build init jobs  │  coldpress/generator.py:93-122
│ - Build task jobs  │  coldpress/generator.py:123-189
│ - Build services   │  coldpress/generator.py:531-561
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ Generate Scripts   │  coldpress/script_gen.py
│ - run.sh           │  script_gen.py:7-64
│ - monitor.sh       │  script_gen.py:116-140
│ - logs.sh          │  script_gen.py:142-242
│ - explore.sh       │  script_gen.py:245-337
│ - cleanup.sh       │  script_gen.py:67-113
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ Write Output       │  coldpress/cli.py:153-222
│ - jobset.yaml      │
│ - services.yaml    │
│ - metadata.json    │
│ - bash scripts     │
│ - ConfigMap files  │
└────────────────────┘
```

### JobSet Execution Flow

```
./run.sh
  │
  ├─► oc create configmap (files)
  │
  ├─► oc apply -f jobset.yaml
  │                │
  │                ▼
  │         ┌──────────────┐
  │         │    Kueue     │
  │         │ (Queue Job)  │
  │         └──────┬───────┘
  │                │ Checks resources
  │                │ Unsuspends when available
  │                ▼
  │         ┌──────────────┐
  │         │   JobSet     │
  │         │  Controller  │
  │         └──────┬───────┘
  │                │
  │                ├─► Job: mkdir
  │                │     │
  │                │     ▼
  │                │   Pod: mkdir
  │                │     │ Creates base_dir
  │                │     ▼
  │                │   Complete
  │                │
  │                ├─► Job: discovery (depends: mkdir Complete)
  │                │     │
  │                │     ▼
  │                │   Pod: discovery
  │                │     │ Runs GPU benchmark
  │                │     │ Saves discovery.json
  │                │     ▼
  │                │   Complete
  │                │
  │                └─► Job: task-0 (depends: discovery Complete)
  │                      │
  │                      ▼
  │                    Pod: task-0
  │                      │ Mounts ConfigMap files
  │                      │ Mounts PVC at /results
  │                      │ Runs training
  │                      │ Saves checkpoints
  │                      ▼
  │                    Complete
  │                      │
  └─► oc wait --for=condition=complete
                        │
                        ▼
                    Job Done!
```

### Results Storage Structure

```
PVC: researcher-a-storage
└── researcher-a/
    └── coldpress_results/
        ├── ddp-training-a1b2c3d4-20260413_143022/  (Run 1)
        │   ├── discovery_user_snapshot.json
        │   ├── checkpoints/
        │   │   ├── model_weights.pth
        │   │   └── training_stats.json
        │   └── logs/
        │       ├── ddp-training-task-0-0-abc123.log
        │       └── combined.log
        │
        ├── ddp-training-e5f6g7h8-20260414_092145/  (Run 2)
        │   ├── discovery_user_snapshot.json
        │   ├── checkpoints/
        │   └── logs/
        │
        └── vllm-benchmark-i9j0k1l2-20260415_164532/  (Different job)
            ├── discovery_user_snapshot.json
            ├── benchmark_results.json
            └── logs/
```

---

## Key Design Decisions

### 1. Why Local Generation?

**Decision:** Generate all manifests locally before applying to cluster

**Rationale:**
- **Transparency**: Users can inspect YAML before applying
- **Version Control**: JobSet manifests can be committed to git
- **No Server**: Simpler architecture, no server-side component
- **Security**: No cluster access needed for generation

**Implementation:** [coldpress/cli.py](coldpress/cli.py) (lines 342-362) writes all files before suggesting `./run.sh`

### 2. Why Two CLIs?

**Decision:** Separate `coldpress-setup` for cluster config, `coldpress` for jobs

**Rationale:**
- **Separation of Concerns**: Cluster admin vs user workflows
- **Permissions**: Setup needs cluster-admin, generate needs none
- **Frequency**: Setup once, generate many times
- **Validation**: Different Pydantic models for different configs

**Implementation:** [pyproject.toml](pyproject.toml) (lines 19-21) defines two entry points

### 3. Why Auto Node Allocation?

**Decision:** Automatically select least-loaded node for each task

**Rationale:**
- **Load Balancing**: Distributes work across cluster
- **Simplicity**: Users don't need to know cluster topology
- **Flexibility**: Can override with `--node` flag if needed

**Implementation:** [coldpress/allocator.py](coldpress/allocator.py) (lines 260-293) scores nodes by demand

### 4. Why Base Directory Per Run?

**Decision:** Each job run gets unique timestamped directory

**Rationale:**
- **No Overwrites**: Multiple runs don't conflict
- **History**: Can compare results across runs
- **Debugging**: Failed runs remain accessible

**Implementation:** [coldpress/generator.py](coldpress/generator.py) (lines 32-50) generates unique base_dir

### 5. Why Bash Scripts?

**Decision:** Generate bash helper scripts instead of Python wrappers

**Rationale:**
- **Simplicity**: Users familiar with shell scripts
- **Transparency**: Easy to read and modify
- **No Dependencies**: Just needs `oc`/`kubectl`
- **Composability**: Can be integrated into other scripts

**Implementation:** [coldpress/script_gen.py](coldpress/script_gen.py) (lines 1-390) generates all scripts

---

## Common Patterns

### Pattern 1: Validation Before Execution

Every config file is validated with Pydantic before use:

```python
# Config validation
validated_config = validate_config(config_data)  # coldpress_common/model.py:270-283

# Task spec validation
validated_tasks = validate_task_specs(task_specs_data)  # coldpress_common/model.py:302-323

# Project config validation
validated_project = validate_project_config(project_data)  # coldpress_common/model.py:286-300
```

This catches errors early with clear error messages.

### Pattern 2: Builder Pattern for Manifests

Complex manifests built step-by-step:

```python
# Container spec
container_spec = build_container_spec(...)  # generator.py:369-408

# Pod spec
pod_spec = build_pod_spec(..., container_spec, ...)  # generator.py:488-528

# Job spec
job = {"template": {"spec": {"template": pod_spec}}}

# JobSet spec
jobset = {"spec": {"replicatedJobs": [job, ...]}}
```

Each builder function focuses on one level of abstraction.

### Pattern 3: Environment Variable Overrides

Default directories can be overridden:

```python
DISCOVERY_DIR = os.getenv("COLDPRESS_DISCOVERY_DIR", "discovery")
PROJECT_DIR = os.getenv("COLDPRESS_PROJECT_DIR", "projects")
OUTPUT_DIR = os.getenv("COLDPRESS_OUTPUT_DIR", "output")
```

Allows customization without changing code.

### Pattern 4: CLI Overrides Config

CLI flags override config file values:

```python
project = project_override or validated_config.project
discovery = discovery_override or validated_config.discovery
output_name = output_override or validated_config.output
```

Provides flexibility for different contexts.

---

## Conclusion

This walkthrough covered:

✅ **Architecture**: Two-piece design (setup vs generate)  
✅ **Setup Flow**: Cluster → Project → User configuration  
✅ **Generation Flow**: Config → Validation → Allocation → JobSet → Scripts  
✅ **Key Components**: Container builder, pod builder, node allocator, script generator  
✅ **Complete Example**: PyTorch DDP training from setup to cleanup  
✅ **Design Decisions**: Local generation, bash scripts, auto allocation  

The codebase is well-structured with clear separation of concerns, comprehensive validation, and transparent workflows that give users full control over their Kubernetes workloads.
