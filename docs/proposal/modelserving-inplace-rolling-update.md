## ModelServing In-Place Rolling Update

This document is the implementation design for [LFX issue #1420](https://github.com/volcano-sh/kthena/issues/1420). It builds on the direction proposed in [PR #1372](https://github.com/volcano-sh/kthena/pull/1372), resolves the review feedback on readiness gates, rollout configuration, and cache safety, and defines the work needed to make the feature production-ready.

### Summary

ModelServing currently updates an image by deleting Pods and creating replacements. That behavior is correct for a broad template change, but it is expensive for an image-only change. A replacement Pod must be scheduled, attached to the network, initialized, and rediscovered by the router. For large model-serving Pods, this increases update time and can consume scarce accelerator capacity while old and new replicas overlap.

This proposal introduces <code>InPlaceRollingUpdate</code>, an explicit ModelServing rollout strategy for changing images of existing regular containers. The controller changes a Pod image in place, keeps the Pod identity, and uses a controller-owned readiness gate to keep the Role out of Kthena routing while kubelet restarts the changed container.

The feature has a strict safety contract:

- It supports image changes only. Any other rendered Pod-template change is rejected under this strategy.
- A Role instance is the availability unit. Its entry Pod and all worker Pods move through the update together.
- A Role-level <code>partition</code> and <code>maxUnavailable</code> control rollout order and parallelism.
- Pod readiness remains false from before the image patch until the target containers are running and ready.
- A durable Pod annotation records the operation, restart baselines, and target images. The controller can reconstruct progress after a restart.
- Planned image restarts do not trigger the existing generic recovery path. Unrelated failures still do.
- Existing Pods without the required readiness gate receive a controlled, one-time Role recreation before they are eligible for in-place updates.
- Router cache state is invalidated or fenced so a same-named restarted Pod is never selected for a stale KV-cache hit.

The feature deliberately does not promise that existing HTTP streams survive a container restart. It prevents new Kthena-router selections after readiness propagation. Applications that require connection draining need a later router-coordinated drain protocol.

### Motivation

#### Problem statement

Kthena represents an inference deployment as a ModelServing. A ModelServing contains ServingGroups, and every ServingGroup contains one or more Roles. A Role instance can contain an entry Pod and worker Pods. In prefill-decode serving, a Role often represents one side of the inference pipeline.

Today, changing an image changes the Role template hash. The ModelServing controller treats the Role as outdated, deletes it, and recreates its Pods. This is safe, but it has operational cost:

- A new Pod waits for scheduling and image pull.
- GPU, network, storage, and PodGroup setup happen again.
- Pod UID, IP, and container IDs change.
- The router must rediscover the replacement.
- Prefix and runtime-cache state are lost or become stale.
- A Role with several workers is rebuilt even when only one container image changed.

Kubernetes permits updating the image of an existing regular container. Kubelet then reconciles the new image and restarts that container without replacing the Pod object. This preserves the Pod name, UID, assigned node, Pod IP, mounted volumes, and network attachment.

An image patch alone is not sufficient for Kthena. The controller must prevent traffic from reaching a restarting Role, distinguish a planned restart from a real failure, preserve rollout budgets, and recover correctly after a controller restart. Without those rules, an in-place update can either route traffic to a restarting backend or trigger the existing recovery policy and recreate the Pod anyway.

#### Current implementation facts

The design uses the existing control-plane boundaries rather than introducing a second controller.

| Current behavior | Why it matters for this design |
| --- | --- |
| [ModelServing revision calculation](../../pkg/model-serving-controller/utils/revision_util.go) hashes only Role templates and intentionally excludes replica and rolling-update configuration. | Changing rollout strategy alone does not create a new revision. Gate bootstrap needs explicit reconciliation state. |
| [Pod generation](../../pkg/model-serving-controller/utils/utils.go) creates entry and worker Pods from a Role template. | This is the single place to inject a managed readiness gate and cache-freshness annotation for newly created Pods. |
| [Pod event handling](../../pkg/model-serving-controller/controller/model_serving_controller.go) handles Ready Pods before handling any Pod with a non-zero container restart count. | The in-place handler must run before the generic Ready and restart paths. |
| [Generic recovery](../../pkg/model-serving-controller/controller/model_serving_controller.go) can delete a Pod after the restart grace period. | A planned kubelet restart must not enter this path. |
| [Role rolling update](../../pkg/model-serving-controller/controller/model_serving_controller.go) already uses Role partition and maxUnavailable semantics. | In-place update should reuse the same Role identity and ordinal model, with a safer default budget. |
| [Router Pod synchronization](../../pkg/kthena-router/controller/modelserver_controller.go) removes a NotReady Pod from the router datastore. | A readiness gate naturally removes an updating backend from normal router selection. |
| [Prefix-cache store](../../pkg/kthena-router/scheduler/plugins/cache/prefix_store.go) evicts state on router Pod deletion events. | A NotReady transition already clears in-memory prefix-cache entries. |
| [KV-cache-aware plugin](../../pkg/kthena-router/scheduler/plugins/kvcache_aware.go) identifies cache owners as pod-name.namespace and retains Redis fields for 24 hours. | A same-named in-place restarted Pod can otherwise receive a stale cache score. This must be fenced. |

#### Goals

- Add an explicit <code>InPlaceRollingUpdate</code> rollout strategy to ModelServing.
- Support in-place changes to images of existing regular containers in entry and worker templates.
- Preserve Pod identity during a successful in-place update.
- Keep a whole Role instance unavailable while any of its Pods is being updated.
- Reuse Role-level <code>partition</code> and <code>maxUnavailable</code> with exact, deterministic semantics.
- Keep updates safe across controller-manager restarts and informer resync.
- Separate planned image restarts from real container failures.
- Prevent router and KV-cache-aware scheduling from using stale cache state after a same-named Pod restarts.
- Provide observable status, events, metrics, unit tests, integration tests, and Kind E2E coverage.

#### Non-goals

- Updating environment variables, commands, arguments, resources, volumes, probes, security settings, metadata, or Pod topology in place.
- Updating init containers or ephemeral containers in place.
- Preserving existing client connections or streaming responses across a container restart.
- Adding maxSurge semantics.
- Automatically rolling back an image after an image-pull or startup failure.
- Replacing Kthena ModelServing ownership with OpenKruise workload CRDs.
- Changing the behavior of existing ServingGroupRollingUpdate or RoleRollingUpdate users.

### Proposal

#### Design decisions

| Topic | Decision |
| --- | --- |
| New strategy | Add <code>InPlaceRollingUpdate</code> to <code>RolloutStrategyType</code>. |
| Eligible change | Images of existing regular containers only. Every other rendered template difference is rejected. |
| Availability unit | One Role instance, meaning its entry Pod plus all worker Pods with the same Role ID. |
| Availability control | Reuse Role <code>partition</code> and <code>maxUnavailable</code>. An omitted Role maxUnavailable means one for this strategy only. |
| Traffic isolation | Inject and manage a controller-owned Pod readiness gate. |
| Durable truth | Pod annotations and Pod status conditions are durable truth. The controller datastore is a reconstructed cache. |
| Legacy Pods | Recreate ungated Role instances before in-place update is allowed. Do not use an annotation-only fallback. |
| Failure handling | Planned image restarts are suppressed. Unrelated failures use the existing RoleRecreate path. An invalid target image stays blocked and visible until the user corrects it or explicitly exits the strategy. |
| Router cache | Existing prefix-cache eviction is reused. KV-cache-aware scores are fenced by a persistent per-Pod cache-valid-after timestamp. |
| OpenKruise | Perform a short compatibility spike. Keep a Kthena-owned adapter boundary unless the upstream utility fits without importing controller ownership or unsafe semantics. |

#### Terms and invariants

| Term | Meaning in this proposal |
| --- | --- |
| ServingGroup | The existing ModelServing unit that groups Roles for one inference workload. |
| Role instance | One entry Pod plus its worker Pods sharing the same ServingGroup, Role name, and Role ID. It is the in-place availability unit. |
| Gated Pod | A Pod whose spec contains the Kthena readiness gate. A false or missing gate condition makes Kubernetes report the Pod NotReady. |
| Active update | A Pod annotation in Preparing or WaitingForRuntime phase, or a Role reconstructed as Updating from that state. |
| Committed Role | A Role whose Pods have target images, runtime completion evidence, Ready conditions, and target revision labels. |
| Cache incarnation | The period beginning at a Pod creation or image-update attempt. Cache records from an older incarnation are not eligible for KV-cache-aware scoring. |

The implementation must preserve these invariants:

- A Role is either fully available or unavailable for Kthena Router. The controller never intentionally returns only a subset of a Role to routing.
- No target Role label or datastore revision is committed before all Pods in the Role pass completion checks.
- No active in-place update proceeds on a Pod without the controller-owned readiness gate.
- A Pod with a false Kthena gate must not enter generic restart recovery merely because the expected image restart increments its cumulative restart count.
- A cache record written before the current Pod cache incarnation must not increase that Pod's KV-cache-aware score.

#### User stories

##### Update a decode image without replacing its Pods

A platform operator runs a ModelServing with four decode Role instances. The operator changes the decode server image from a known v1 digest to a known v2 digest.

The controller removes one decode Role from readiness, patches the entry and worker Pod images, waits until all target containers are running and ready, marks the Role ready, and proceeds to the next Role. The updated Pods retain their UID, name, node, Pod IP, and mounted state.

##### Upgrade only prefill in a prefill-decode deployment

The operator changes only the prefill Role image. Decode Role instances do not restart. Each selected prefill Role becomes unavailable as one unit, so Kthena does not send a request to an entry Pod whose workers are still restarting.

##### Correct a bad image

The operator sets an image that cannot be pulled. The affected Role remains NotReady, the ModelServing reports an in-place update failure, and the controller does not repeatedly delete and recreate the same bad image. The operator replaces the image with a valid reference. The controller starts a new in-place attempt while the Role remains isolated.

##### Enable the strategy on an existing ModelServing

An existing ModelServing has Pods without the Kthena in-place readiness gate. The operator changes only the rollout strategy to <code>InPlaceRollingUpdate</code>. Kthena performs a one-time, Role-by-Role bootstrap recreation to create gated Pods. After every unprotected Role is gated and Ready, a later image-only edit uses true in-place update.

This intentionally takes two user operations. Combining a strategy switch and an image change is rejected. It avoids claiming an in-place update for legacy Pods that cannot be gated safely.

#### API and validation

The public API adds a new rollout type:

~~~go
type RolloutStrategyType string

const (
    ServingGroupRollingUpdate RolloutStrategyType = "ServingGroupRollingUpdate"
    RoleRollingUpdate         RolloutStrategyType = "RoleRollingUpdate"
    InPlaceRollingUpdate      RolloutStrategyType = "InPlaceRollingUpdate"
)
~~~

The kubebuilder enum must include the new value. The implementation must run <code>make generate</code> and commit the generated CRD and chart artifacts. Without the enum update, the API server rejects the new strategy before the controller sees it.

Example configuration:

~~~yaml
apiVersion: workload.serving.volcano.sh/v1alpha1
kind: ModelServing
metadata:
  name: llama-serving
spec:
  recoveryPolicy: RoleRecreate
  rolloutStrategy:
    type: InPlaceRollingUpdate
  template:
    roles:
    - name: decode
      replicas: 4
      partition: 1
      maxUnavailable: 1
      entryTemplate:
        spec:
          containers:
          - name: server
            image: registry.example.com/llama@sha256:target-digest
      workerReplicas: 1
      workerTemplate:
        spec:
          containers:
          - name: worker
            image: registry.example.com/llama-worker@sha256:target-digest
~~~

The two existing configuration levels have distinct meaning:

| Field | Meaning with InPlaceRollingUpdate |
| --- | --- |
| <code>spec.rolloutStrategy.rollingUpdateConfiguration</code> | Forbidden. It controls ServingGroupRollingUpdate only. |
| <code>spec.template.roles[].partition</code> | Allowed. Role instances with ordinal lower than the effective partition are protected. |
| <code>spec.template.roles[].maxUnavailable</code> | Allowed. It limits unavailable Role instances of that Role in one ServingGroup. |
| omitted Role <code>maxUnavailable</code> | Treated as one for InPlaceRollingUpdate. This controller-only default does not alter RoleRollingUpdate behavior. |
| <code>spec.recoveryPolicy</code> | Must resolve to RoleRecreate. This keeps bootstrap and unexpected-failure recovery at the same Role granularity. |

The validating webhook must read both the new object and <code>AdmissionRequest.OldObject</code> on UPDATE. The current helper decodes only the new object, so the implementation needs a small admission parsing extension.

The webhook applies the following update contract when the effective strategy is InPlaceRollingUpdate:

| Update | Result | Reason |
| --- | --- | --- |
| Create a new ModelServing with this strategy | Allowed | New Pods receive the gate at creation. |
| Switch to this strategy with no template image change | Allowed | Starts the controlled gate-bootstrap migration when needed. |
| Change images of existing regular containers after bootstrap | Allowed | Eligible for in-place update. |
| Add, remove, rename, or reorder containers | Rejected | The rendered Pod shape changed. |
| Change an init-container or ephemeral-container image | Rejected | It is outside the image-only runtime contract. |
| Change a regular container field other than image | Rejected | The Pod update is not safe under this strategy. |
| Change entry or worker template metadata | Rejected | Labels and annotations can alter routing, selection, and injected behavior. |
| Change Role name, workerReplicas, or template structure with an image edit | Rejected | This changes Role shape and update scope. |
| Change ModelServing or Role replica count with an image edit | Rejected | Scaling and image update must be separate operations. |
| Change partition or maxUnavailable with an image edit | Rejected | The rollout policy must be stable for one image operation. |
| Switch from InPlaceRollingUpdate to an existing strategy | Allowed as an explicit abort path | The controller safely delegates to the selected recreate strategy. |

The template comparison is semantic. It maps regular containers by name, verifies that both templates contain the same ordered set of containers, clears each allowed image field in copies of old and new objects, and compares the remaining rendered Role templates. The comparison covers both entry and worker templates.

The controller owns the readiness-gate condition type. The webhook rejects a user-supplied readiness gate with that exact type so another controller cannot change the in-place lifecycle.

#### Rendered Pod and plugin contract

ModelServing plugins can mutate a Pod in the existing OnPodCreate hook. The in-place implementation must preserve that extension point:

- The allowed-image set is derived from the declared entry and worker Role templates.
- A container injected by a plugin is never selected for an in-place image patch unless it is also declared by the Role template.
- A restart in an injected or otherwise unchanged container remains an unrelated failure and uses generic recovery.
- The controller appends the Kthena readiness gate and cache-valid-after annotation after OnPodCreate hooks run, immediately before Pod creation.
- A plugin must not remove or replace a controller-owned gate, state annotation, cache-freshness annotation, revision label, or Role template-hash label.

This keeps template ownership clear. It also prevents a sidecar injected for telemetry, storage, or networking from silently becoming part of an image-only update contract.

#### Role scope, partition, and maxUnavailable

The unit selected by this rollout is a Role instance:

~~~text
ModelServing
  ServingGroup llama-serving-2
    Role decode-3
      entry Pod  llama-serving-2-decode-3-0
      worker Pod llama-serving-2-decode-3-1
      worker Pod llama-serving-2-decode-3-2
~~~

All Pods in a selected Role instance become NotReady before any of their images are patched. Kthena patches the Role Pods concurrently after isolation. The Role is not returned to traffic until every Pod in that Role reaches the target image and Ready state.

The budget is evaluated independently for each pair of ServingGroup and Role name. This preserves the current RoleRollingUpdate scope. For example, two ServingGroups that each contain a decode Role with <code>maxUnavailable: 1</code> can each have one unavailable decode Role at the same time.

For a Role with four instances and <code>partition: 1</code>:

| Role ordinal | Eligible for update |
| --- | --- |
| 0 | No. Protected by partition. |
| 1 | Yes. |
| 2 | Yes. |
| 3 | Yes. |

Partition is evaluated from the parsed Role ordinal, not from the position of a Role in a slice. This remains correct if role IDs are sparse after failure recovery or scale operations.

For every ServingGroup and Role name, reconciliation uses this algorithm:

1. Read the target Role template hash.
2. List Role instances and parse their ordinals.
3. Exclude partition-protected, deleting, malformed, and missing-Pod Role instances.
4. Count unavailable instances. A Role is unavailable if it is not fully Ready or has an active in-place operation.
5. Sort candidates deterministically:
   - already unavailable outdated candidates first, because updating them does not reduce available capacity further;
   - then Role ordinal in descending order;
   - then Role ID as a stable final tie-breaker.
6. Start every already-unavailable eligible candidate, then start Ready candidates only while doing so stays within the effective maxUnavailable budget.

An omitted <code>maxUnavailable</code> has an effective value of one for this strategy. Existing RoleRollingUpdate currently treats an omitted role budget differently, so the new default must live in an InPlace-specific helper rather than a shared defaulting change.

#### Readiness-gate lifecycle

The controller-managed condition type is:

~~~text
modelserving.volcano.sh/InPlaceUpdateReady
~~~

Kubernetes calculates Pod Ready from both container readiness and any declared readiness gates. A missing custom condition is treated as false. The controller must write this custom condition through the Pod status subresource. The controller therefore needs <code>get</code>, <code>patch</code>, and <code>update</code> permission for both <code>pods</code> and <code>pods/status</code>.

For a Pod created under InPlaceRollingUpdate:

1. Pod generation preserves user readiness gates and appends the Kthena gate once.
2. The Pod starts NotReady because the Kthena gate condition is absent or false.
3. When normal container readiness is true and no in-place operation is active, the controller writes the Kthena condition as true.
4. Kubelet then computes Pod Ready as true.

For an active update:

1. The controller records a durable Preparing state.
2. The controller sets the Kthena gate to false.
3. The router observes the NotReady Pod and removes it from scheduling candidates.
4. The controller patches the target container images.
5. The controller waits for runtime completion evidence.
6. The controller sets the Kthena gate true only after every changed container is running at the target image and all normal containers are ready.

The gate is created with the Pod. Kubernetes rejects adding readiness gates to an existing Pod, so a controller annotation alone cannot provide the same traffic-isolation guarantee.

#### Legacy Pod gate bootstrap

Pods created before this feature do not have the Kthena readiness gate. The controller must never run an in-place update on them.

When a ModelServing changes from another rollout strategy to InPlaceRollingUpdate:

1. The webhook allows the strategy-only update.
2. The controller scans every unprotected Role instance for the Kthena readiness gate on every entry and worker Pod.
3. A Role that lacks the gate enters <code>GateBootstrap</code>.
4. The controller recreates that Role through the existing Role-level deletion and creation mechanism, respecting the Role partition and maxUnavailable budget.
5. The newly created Pods receive the gate at creation and become Ready through the normal gate initialization path.
6. The controller marks the Role bootstrap-complete only after all its Pods are Ready and gated.

The bootstrap changes Pod UID and may change Pod IP. It is an intentional migration, not an in-place image update.

If partition protects a legacy Role, bootstrap does not bypass partition. ModelServing status remains in progress with reason <code>LegacyPodsProtected</code>. The user must lower the partition or use a recreate strategy before every Role can become in-place eligible.

The controller does not add rollout strategy to <code>ModelServingRevision</code>. The existing revision intentionally hashes only Role templates. Instead, the durable gate state and ModelServing in-place status drive bootstrap reconciliation. This avoids triggering an unrelated rollout merely because an operator changes rollout policy.

#### Persistent state and datastore contract

The controller adds a new Role cache state:

~~~text
Creating -> Updating -> Running
~~~

The in-memory datastore also gains an atomic operation that advances a Role revision and Role template hash together after successful completion. The datastore is not authoritative. It is rebuilt from Pod labels, readiness, gate conditions, and annotations after controller restart.

The durable source of truth is a compact annotation on each participating Pod:

~~~text
modelserving.volcano.sh/inplace-update-state
~~~

Conceptual state:

~~~json
{
  "version": 1,
  "operationID": "pod-uid:modelserving-generation:role-template-hash",
  "phase": "Preparing",
  "targetRevision": "target-serving-group-revision",
  "targetRoleTemplateHash": "target-role-hash",
  "targetImages": {
    "server": "registry.example.com/server@sha256:target"
  },
  "startedAt": "2026-08-02T12:00:00Z",
  "containers": {
    "server": {
      "baselineRestartCount": 3,
      "baselineImageID": "old-image-id",
      "baselineContainerID": "old-container-id",
      "expectedRestartAllowance": 1,
      "stableRestartCount": 3
    }
  }
}
~~~

The exact serialization can evolve with a versioned schema. It must satisfy these rules:

- Record only changed regular containers.
- Never store credentials, pull secrets, request bodies, or other sensitive values.
- Keep the last completed baseline on the Pod so a later informer event does not mistake a cumulative restart count for a new failure.
- Use Pod UID and resource version checks to reject stale state.
- Treat an unreadable or unsupported state annotation as a blocked update, emit a warning Event, and do not patch the image.

The controller also stores a simple cache-freshness annotation:

~~~text
modelserving.volcano.sh/kv-cache-valid-after
~~~

It is set on all generated ModelServing Pods and increased before each in-place image patch. The value is an integer Unix second rounded up from the update start. It is not a cache invalidation command. It is a persistent lower bound used by the router to reject Redis cache records older than the current Pod incarnation.

#### Update protocol

The update protocol prioritizes traffic isolation over speed. Each mutation is idempotent and retried on resource-version conflict.

~~~mermaid
stateDiagram-v2
    [*] --> Ready
    Ready --> Preparing: Role selected
    Preparing --> Isolated: state recorded and gate false
    Isolated --> WaitingForRuntime: image patch accepted
    WaitingForRuntime --> WaitingForRuntime: target not ready
    WaitingForRuntime --> Committing: target images and containers ready
    Committing --> Ready: gate true and role hash committed
    Preparing --> GenericRecovery: unrelated failure before image patch
    WaitingForRuntime --> Failed: target image cannot become healthy
    Failed --> Preparing: user supplies a new valid image
    Failed --> RecreateFallback: user switches rollout strategy
    RecreateFallback --> [*]
~~~

For one selected Role instance, the controller follows this sequence:

1. Re-read every entry and worker Pod by name. Verify ModelServing ownership, Role labels, expected Pod count, absence of deletion timestamp, and the Kthena gate.
2. Write an annotation with phase <code>Preparing</code>, target images, and pre-update container baselines. No image changes occur in this step.
3. Set the Kthena readiness condition to false through the Pod status subresource.
4. Bump <code>kv-cache-valid-after</code>.
5. Patch Pod metadata and the named regular-container image fields together. Update state phase to <code>WaitingForRuntime</code>.
6. Observe Pod updates. Confirm target images, target runtime identity, running state, and ContainersReady.
7. Patch the target revision and Role template-hash labels only after every Pod in the Role has completed.
8. Atomically update the datastore Role revision and Role template hash, set Role state to Running, and update ModelServing status.
9. Set the Kthena readiness condition to true on every Role Pod.
10. Wait until Kubernetes reports the whole Role Ready. Then select the next Role.

The state is written before the readiness condition is set false. If the controller crashes after recording state but before isolation, it can resume or clear a safe Preparing attempt. If it crashes after isolation, the annotation tells the new controller why the Pod is NotReady.

The update handler always checks persistent in-place state before the generic Ready and restart handlers:

- An active state routes the event to in-place progress evaluation.
- A completed state retains enough restart baseline to suppress the one expected image restart.
- An unrelated restart or a restart count beyond the recorded allowance returns control to the generic RoleRecreate recovery path.
- A lower restart count than the stored baseline means the Pod was recreated or the state is stale. The controller discards the stale record and handles the new Pod normally.

#### Completion and restart semantics

Patching <code>PodSpec.Containers[].Image</code> is not completion. A Role commits only after every Pod in the Role satisfies all of the following:

1. The desired image string is present for every changed regular container.
2. The matching container status exists and is running.
3. The container provides proof of a new runtime generation. A changed image ID is preferred. A changed container ID is an acceptable fallback for image references that resolve to the same image ID.
4. Normal container readiness is true.
5. The Kthena readiness condition has been set true and Kubernetes reports Pod Ready.

The expected restart accounting is per container:

| Container case | Allowed during active image update | Result after completion |
| --- | --- | --- |
| Changed regular container with a new runtime image or container ID | One kubelet-driven restart allowance | Preserve the completed restart baseline. A later increase invokes normal recovery. |
| Changed regular container with repeated restart count before reaching Ready | No unlimited allowance | Mark the in-place operation Failed and keep the Role isolated. |
| Unchanged regular container | No allowance | A restart is unrelated and follows generic recovery. |
| Init container | No allowance and not eligible for image patch | A restart follows existing generic recovery. |
| Restart count lower than recorded baseline | Treat state as stale or Pod recreated | Reconstruct from the current Pod. |

An invalid image or an image-pull failure is not a reason to repeatedly recreate the Role with the same bad template. The controller sets an <code>InPlaceUpdateFailed</code> condition and Event, keeps the Kthena gate false, and waits for one of these operator actions:

- update the image to a new valid target; or
- switch to an existing recreate rollout strategy as an explicit abort and fallback.

When a user supplies a new image while a Role is blocked, the controller starts a fresh attempt from current container status and restart baselines. The gate remains false throughout the transition. This avoids a brief return to traffic between failed and corrected images.

When a user exits InPlaceRollingUpdate while an operation is active, the controller emits <code>InPlaceUpdateAborted</code>, does not reopen the gate for the incomplete Role, and delegates to the selected existing rollout strategy. This is the escape hatch for an operator who prefers a recreate fallback.

The fallback update must contain a valid existing rollout-strategy and recovery-policy pair. For example, a RoleRollingUpdate fallback uses RoleRecreate. The controller marks the old in-place state Aborted before allowing the selected recreate path to delete the isolated Pod. A newly created replacement has no stale in-place state.

| Observation | Controller action | Traffic state |
| --- | --- | --- |
| Conflict before the image patch | Retry from durable Preparing state. | Gate stays false once isolation began. |
| Controller restart during update | Reconstruct RoleUpdating from Pod annotation and status. | Gate condition remains authoritative. |
| Target image pull fails | Mark Failed, emit Event, wait for corrected image or fallback. | Isolated. |
| Changed container restarts once and reaches target Ready | Commit normally. | Isolated until every Role Pod completes. |
| Changed container repeatedly restarts before target Ready | Mark Failed. Do not recreate the same bad target automatically. | Isolated. |
| Unchanged container restarts | Hand off to existing RoleRecreate recovery. | Existing recovery behavior. |
| Pod is externally deleted | Clear stale state and let existing recovery create a replacement. | Existing recovery behavior. |

#### Revisions, labels, and status

The controller must not report an update as complete merely because the Pod spec patch succeeded.

For each Role:

- The Role template hash and revision labels stay old while the Role is Preparing, WaitingForRuntime, or Failed.
- The labels change only after all entry and worker Pods in the Role meet the completion criteria.
- The datastore advances the Role revision and Role template hash in one locked operation after labels are updated.

For each ServingGroup:

- Its revision advances only when no unprotected Role instance remains outdated or Updating.
- Partition-protected Roles retain their old desired revision semantics.
- An active gate-bootstrap migration also counts as update work even though it does not change the ModelServing revision hash.

Existing <code>currentRevision</code>, <code>updateRevision</code>, replica counters, and conditions retain their current serving-group meaning. To make partial Role progress visible, this proposal adds an optional read-only status block:

~~~go
type InPlaceUpdateStatus struct {
    ObservedGeneration int64
    TargetRevision     string
    Phase              string
    TotalRoles         int32
    UpdatedRoles       int32
    UpdatingRoles      int32
    UnavailableRoles   int32
    Message            string
}
~~~

The final implementation includes normal JSON tags and kubebuilder documentation. The status block is populated only for this strategy. It gives operators a concise answer to these questions:

- Is Kthena bootstrapping readiness gates, updating images, blocked, or complete?
- Which ModelServing generation is being reconciled?
- How many Role instances are target, updated, currently updating, or unavailable?
- Why is progress blocked?

The existing ModelServing conditions use these reasons:

| Condition | True while | Representative reason |
| --- | --- | --- |
| UpdateInProgress | Gate bootstrap, any active in-place operation, or any unprotected outdated Role exists | InPlaceUpdateProgressing |
| Progressing | A Role is creating, updating, or recovering | RoleUpdating |
| Available | Existing availability calculation says sufficient ServingGroups are ready | Existing behavior is preserved |
| UpdateInProgress | A legacy gate is blocked by partition | LegacyPodsProtected |
| UpdateInProgress | A target image cannot become healthy | InPlaceUpdateFailed |

The status-update implementation must not create a new revision or count a partially patched Role as updated.

#### Router, traffic, and cache safety

The readiness gate integrates with the current router behavior:

1. Kthena sets the Pod readiness gate false.
2. The router ModelServer controller sees the Pod as NotReady.
3. It calls <code>DeletePod</code> on its datastore.
4. Scheduler plugins receive no candidate for that Pod.
5. The prefix-cache store receives the Pod deletion callback and evicts in-memory prefix mappings.

This protects future requests after informer propagation. It does not terminate or drain a request that was already proxied to the Pod, and it does not protect clients that bypass Kthena Router and address a Pod directly.

The current headless Services use <code>PublishNotReadyAddresses: true</code>. Therefore, a readiness gate is not an internal Service-level connection drain for entry-worker traffic. The update treats a Role as unavailable to Kthena Router, but it does not claim to drain arbitrary internal TCP connections. This limit belongs in the user guide and release notes.

The KV-cache-aware plugin needs extra work. It stores cache owners as pod-name.namespace in Redis and currently accepts a cached owner by name. An in-place restart keeps the same name, so a Redis record from the previous container process can look valid for up to the existing 24-hour freshness window.

The implementation adds a persistent freshness fence:

1. Every generated ModelServing Pod receives <code>modelserving.volcano.sh/kv-cache-valid-after</code> at creation.
2. The controller increases it before each in-place image patch.
3. KV-cache-aware reads Redis hash fields and their timestamp values, rather than only field names.
4. For each candidate Pod, KV-cache-aware ignores a record whose timestamp is older than the Pod cache-valid-after value.
5. A restarted runtime writes new cache records with fresh timestamps. Those records become eligible naturally.

This design avoids a cluster-wide Redis scan and delete during an update. It works across router restarts because the threshold lives on the Pod. It also makes normal replacement Pods safer when they reuse a name.

New router metrics:

- <code>kthena_router_kvcache_stale_entries_filtered_total</code>
- <code>kthena_router_kvcache_cache_valid_after_seconds</code>, exposed only as bounded aggregate metrics without Pod-name labels

#### Controller integration points

| Area | Planned change |
| --- | --- |
| <code>pkg/apis/workload/v1alpha1</code> | Add strategy enum value and optional in-place status type. |
| CRD generation and Helm CRDs | Regenerate enum and status schema with <code>make generate</code>. |
| <code>pkg/model-serving-controller/webhook</code> | Decode old object, enforce image-only diff, validate Role fields and recovery-policy pairing. |
| <code>pkg/model-serving-controller/utils</code> | Add readiness-gate, in-place-state, image comparison, Pod condition, and cache-freshness helpers. |
| <code>pkg/model-serving-controller/controller</code> | Inject gates, select Role targets, drive in-place state machine, patch Pod status and image fields, update status. |
| <code>pkg/model-serving-controller/datastore</code> | Add RoleUpdating and atomic Role revision and hash transition. Rebuild it from persistent Pod state. |
| Controller RBAC | Add Pod patch and Pod status patch or update permissions. |
| <code>pkg/kthena-router/scheduler/plugins/kvcache_aware.go</code> | Read field timestamps and apply Pod cache-valid-after filtering. |
| Documentation and examples | Add rollout example, user guide, troubleshooting guide, and metric reference. |

The in-place update handler runs before the current generic <code>updatePod</code> switch. It owns all Pods with a Kthena in-place state annotation or Kthena readiness gate. Once a Pod is fully completed and its baseline is stable, normal Ready processing continues, but restart handling first compares against the durable baseline.

#### OpenKruise compatibility spike

OpenKruise has mature in-place update logic, including readiness-gate handling and persisted state. Its [public in-place utility](https://github.com/openkruise/kruise/blob/master/pkg/util/inplaceupdate/inplace_update.go) exposes generic-looking update methods, but it also depends on OpenKruise ControllerRevision adapters, Pod adapters, and OpenKruise API types.

The project starts with a one-week compatibility spike:

| Question | Acceptance criterion |
| --- | --- |
| Can Kthena build an image-only update specification from its Role templates without importing CloneSet ownership behavior? | A small prototype updates a fake Pod and preserves Kthena labels and annotations. |
| Can the utility use Kthena ControllerRevision data without changing Kthena revision semantics? | A prototype completes a successful and a failed image update using Kthena revisions. |
| Can Kthena keep its own readiness gate condition, state schema, and cache-valid-after fence? | No OpenKruise controller, webhook, or CRD becomes required at runtime. |
| Is the dependency size and upgrade risk acceptable? | Dependency review passes and the implementation remains understandable to Kthena maintainers. |

The default architecture uses a narrow internal interface:

~~~go
type PodInPlaceUpdater interface {
    Prepare(ctx context.Context, pod *corev1.Pod, target Target) error
    PatchImages(ctx context.Context, pod *corev1.Pod, target Target) error
    Evaluate(ctx context.Context, pod *corev1.Pod, state State) Result
}
~~~

If the spike shows that OpenKruise can implement this interface cleanly, Kthena can wrap its utility behind the adapter. If it cannot, Kthena implements the small image-only adapter locally. The rollout policy, Role coordination, status, router cache fence, and recovery semantics remain Kthena-owned in either case.

#### Observability and operations

The controller adds Events at ModelServing and Role scope:

- InPlaceGateBootstrapStarted
- InPlaceGateBootstrapCompleted
- InPlaceUpdateStarted
- InPlaceUpdateImagePatched
- InPlaceUpdateCompleted
- InPlaceUpdateFailed
- InPlaceUpdateAborted

Controller metrics use bounded labels such as namespace, ModelServing, Role name, and outcome. They never use Pod name or image reference as a metric label.

| Metric | Meaning |
| --- | --- |
| <code>kthena_modelserving_inplace_update_attempts_total</code> | Attempts by outcome. |
| <code>kthena_modelserving_inplace_update_duration_seconds</code> | Time from isolation to Role completion. |
| <code>kthena_modelserving_inplace_update_active_roles</code> | Current Role instances in Preparing or WaitingForRuntime. |
| <code>kthena_modelserving_inplace_update_failures_total</code> | Failed attempts by reason such as image-pull, readiness, or unexpected restart. |
| <code>kthena_modelserving_inplace_gate_bootstrap_roles_total</code> | Legacy Roles recreated to become gated. |
| <code>kthena_modelserving_inplace_update_unavailable_roles</code> | Current unavailable Role instances by ModelServing and Role. |

The user guide includes these diagnostics:

~~~text
kubectl describe modelserving <name>
kubectl get pod -l modelserving.volcano.sh/name=<name> -o yaml
kubectl get events --field-selector involvedObject.kind=ModelServing
~~~

Operators should use immutable image digests for production updates. Tags remain supported because Kubernetes supports image references, but a mutable tag makes it difficult to prove which artifact reached a restarted container.

### Risks and mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| A legacy Pod lacks the readiness gate. | Traffic can reach a restarting container. | Gate-bootstrap recreation is mandatory. No annotation-only fallback. |
| The controller crashes midway through an update. | A Pod can remain NotReady or be interpreted as failed. | Durable per-Pod state, idempotent phases, and reconstruction from annotations and status. |
| A planned restart triggers current recovery logic. | Kthena deletes a Pod, defeating in-place behavior. | Persistent baseline check runs before generic restart handling. |
| A bad image never becomes ready. | The rollout stalls. | Clear Failed condition and Event. Keep Role isolated. Require a corrected image or explicit fallback. |
| A previous container cache record is selected after restart. | Wrong cache score and avoidable latency or backend error. | Persistent cache-valid-after fence in KV-cache-aware scoring. |
| Router informer propagation is delayed. | A short window can still accept a new request. | State the limit clearly. This release does not promise stream draining. A future router acknowledgement protocol is separate work. |
| A headless Service publishes NotReady addresses. | Internal callers might still discover the Pod. | Scope the guarantee to Kthena Router. Document the service behavior. |
| A user updates image and template shape together. | Update unit becomes ambiguous. | Webhook rejects mixed changes. |
| Controller-managed readiness condition conflicts with another controller. | Pod can remain blocked or become Ready early. | Reserve the exact condition type and reject it in user templates. |
| Metrics have high cardinality. | Monitoring cost and instability. | Never label metrics with Pod names or image strings. |

The design does not introduce secrets, external endpoints, or new privilege beyond Pod patch and Pod status patch. RBAC review must confirm that the controller writes only Pods owned by the same ModelServing UID and only Kthena-reserved annotations, labels, image fields, and condition types.

### Implementation plan

The project is intentionally split into reviewable phases. Each phase has a demonstration gate before the next phase begins.

| Weeks | Milestone | Deliverable and acceptance gate |
| --- | --- | --- |
| 1 | Design and compatibility spike | Confirm OpenKruise adapter feasibility, finalize API names, and record the dependency decision. |
| 2 | API and admission | New enum, generated CRDs, old-object parsing, image-only validator tests, and API documentation. |
| 3 to 4 | Pod lifecycle primitives | Gate injection, status-condition helper, durable state schema, RoleUpdating datastore state, RBAC changes, and controller restart reconstruction tests. |
| 5 to 6 | Core image update | Deterministic Role selection, partition and budget accounting, image patch protocol, runtime completion checks, and label and revision commit. |
| 7 | Failure semantics | Planned versus unrelated restarts, invalid-image behavior, explicit fallback, Events, conditions, and metrics. |
| 8 | Legacy migration | Gate-bootstrap rollout, protected legacy Role status, migration E2E tests. |
| 9 | Router cache safety | Cache-valid-after annotation, KV-cache-aware timestamp filtering, router unit and integration tests. |
| 10 | End-to-end behavior | Kind E2E tests with real kubelet image restart behavior, controller-manager restart, and router exclusion checks. |
| 11 | Performance and hardening | Reconcile conflict tests, race tests, benchmark update latency, and documentation review. |
| 12 | Final review | User guide, examples, upgrade notes, release note, and maintainer feedback fixes. |

### Test plan

The unit test suite cannot prove kubelet image-restart behavior by itself. The implementation needs a layered test plan.

#### Unit tests

- API enum and generated CRD schema accept InPlaceRollingUpdate.
- Validator accepts a regular-container image-only change.
- Validator rejects every other rendered template change, including init images, resources, probes, metadata, container order, and worker count.
- Validator accepts strategy-only bootstrap and allows explicit fallback.
- Role target selection honors sparse ordinals, partition, maxUnavailable, active updates, and stable tie-breaking.
- A Role contains exactly the expected entry and worker Pod set before it is selected.
- Gate injection is idempotent and preserves unrelated user gates.
- State transitions recover after retryable conflict and do not patch images while Preparing state is incomplete.
- Completion requires the target image, new runtime identity, ContainersReady, and gate readiness.
- Changed-container restart allowance is accepted once. Unchanged-container and repeated restarts reach generic recovery.
- A lower restart count invalidates stale state.
- Datastore reconstruction restores Updating from an active Pod annotation and commits revision plus Role template hash atomically.
- KV-cache-aware filters a Redis record older than cache-valid-after and accepts a newer record.

#### Controller integration tests

- Use a fake Kubernetes client with reactors to assert the order: state annotation, gate false, image patch, completion, gate true.
- Assert image patch requests modify only named regular-container image fields and controller-owned metadata.
- Assert generic Pod recovery is not called for an expected active image restart.
- Assert generic RoleRecreate is called for an unrelated container restart.
- Assert a role never commits target labels before all its Pods complete.
- Assert status stays UpdateInProgress while any unprotected Role is Updating or blocked.

#### Kind end-to-end tests

Each E2E test uses a real kubelet because only a cluster proves that changing a Pod image produces the expected container transition while retaining Pod identity.

| Scenario | Assertions |
| --- | --- |
| Single entry Pod image update | Pod name, UID, node, and IP stay stable. Container ID changes. Image reaches target. Gate becomes false then true. |
| Role with entry and workers | All Role Pods isolate together. The Role returns Ready only after every Pod is healthy. |
| Partition and maxUnavailable | Protected ordinals do not change. The number of unavailable Role instances never exceeds budget. |
| Prefill-only update | Decode Pods retain image, UID, and readiness. |
| Controller-manager restart | An active update resumes from annotations and does not duplicate or recreate a successful Pod. |
| Invalid image then correction | Failed Role stays isolated and is not deleted. A corrected image completes. |
| Explicit fallback | Switching to RoleRollingUpdate abandons in-place state and recreates the Role through existing behavior. |
| Router integration | A false-gate Pod disappears from router candidates before a new request is scheduled. |
| Prefix cache and KV cache | Prefix mappings are evicted on NotReady. KV cache records older than cache-valid-after do not influence score. |
| No deletion on successful in-place update | Watch events and verify no Pod deletion occurred for the selected Role. |

The final E2E report records tested Kubernetes versions supported by Kthena and the image pull policy used in the test. It also reports update duration separately from application model-load time.

#### Verification commands

Before opening the implementation PR:

~~~text
make fmt
make lint
make test
make gen-check
go test -race ./pkg/model-serving-controller/... ./pkg/kthena-router/...
make test-e2e-controller-manager
make test-e2e-router
~~~

### Alternatives

#### Keep recreate-only rolling updates

This is the current behavior. It is simple and broadly safe, but it recreates Pod identity and repeats scheduling and initialization work for an image-only update. It does not meet the LFX issue objective.

#### Patch Pod images without a readiness gate

This is rejected. A Pod can remain routable while kubelet stops and restarts the changed container. It also gives the controller no reliable traffic-isolation signal.

#### Use an annotation-only legacy fallback

This is rejected. An annotation can help the Kthena controller recognize a planned update, but it cannot change Kubernetes Pod Ready. It would avoid accidental recovery while leaving a legacy Pod visible to traffic.

#### Inject the readiness gate into every ModelServing Pod

This would make later strategy switches easier, but all ModelServing Pods would depend on controller condition management even when users never request in-place update. A controller outage or condition bug could block unrelated workloads. The proposal scopes the gate to new InPlaceRollingUpdate Pods and explicit bootstrap replacements.

#### Add a universal in-flight request drain before image patch

The current controller has no acknowledgement protocol with every router replica, and a readiness transition does not terminate existing streams. A fixed sleep would not prove that requests are drained. This is valuable future work, but it is not presented as a guarantee in the first in-place update release.

#### Fully adopt OpenKruise CloneSet or Advanced StatefulSet

OpenKruise provides mature in-place mechanisms, but replacing ModelServing ownership would change Kthena APIs, Role semantics, PodGroup handling, and controller responsibilities. The compatibility spike evaluates reuse of a narrow utility, not migration to a separate workload owner.

#### Delete all Redis cache records for a restarted Pod

The existing KV-cache schema does not maintain an efficient reverse index from Pod to all block keys. A Redis-wide scan during each update is expensive and races with runtime writes. The cache-valid-after timestamp fence is bounded, persistent, and safe across router restart.

### References

- [LFX issue #1420](https://github.com/volcano-sh/kthena/issues/1420)
- [Original in-place update proposal PR #1372](https://github.com/volcano-sh/kthena/pull/1372)
- [Kubernetes Pod conditions and readiness gates](https://kubernetes.io/docs/concepts/workloads/pods/pod-condition/)
- [Kubernetes Pod API reference](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/)
- [OpenKruise in-place update concept](https://openkruise.io/docs/core-concepts/inplace-update)
- [OpenKruise in-place update utility source](https://github.com/openkruise/kruise/blob/master/pkg/util/inplaceupdate/inplace_update.go)
