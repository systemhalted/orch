# Agent Orchestrator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a working Rust MVP that plans, persists, routes, schedules, observes, and explains local coding-agent work through Herdr while protecting capped-provider budgets.

**Architecture:** The `orch-core` library owns domain rules, persistence, adaptive routing, provider normalization, Herdr communication, Git isolation, scheduling, and orchestration. The `orch` binary talks to a local daemon over a versioned Unix-socket JSON protocol and provides the public CLI and terminal control view. SQLite stores current state and audit events; large artifacts stay in a content-addressed filesystem store.

**Tech Stack:** Rust 1.98 with edition 2024, Tokio, Clap, Serde, TOML, rusqlite with bundled SQLite, UUID, Chrono, SHA-256, Reqwest, Ratatui, Crossterm, tempfile, assert_cmd, and predicates.

**Spec:** `docs/superpowers/specs/2026-09-30-agent-orchestrator-design.md`

## Global Constraints

- The MVP is single-user and single-machine.
- Herdr is the terminal and execution substrate; `orch` does not implement terminal multiplexing.
- Provider credentials remain in native CLI stores or environment variables.
- The target Git branch is never modified without an explicit integration approval.
- Dispatch, result ingestion, event recording, and recovery are idempotent.
- Every provider assignment and next action has a persisted explanation.
- The default planner may produce a deterministic four-stage graph; model planners plug into the same `Plan` schema.
- The existing empty `.git` directory is not removed or replaced. Commit steps are recorded but skipped until this workspace becomes a valid Git repository.

## Review Focus

- Cyclic or missing dependencies must be rejected before dispatch; Task 1 tests both cases.
- Unknown provider quota must still enforce turn, concurrency, and reserve limits; Task 3 tests the fallback ledger.
- Restart reconciliation must not duplicate work or silently relaunch unknown processes; Tasks 2 and 6 test lease recovery.
- Malformed provider JSON must return a typed error while retaining the raw line; Task 4 tests this boundary.
- Internal composition must not update the configured target branch without approval; Task 5 tests target-branch protection.

---

### Task 1: Rust workspace, domain contracts, and configuration

**Files:**
- Create: `Cargo.toml`
- Create: `src/lib.rs`
- Create: `src/domain.rs`
- Create: `src/config.rs`
- Create: `tests/domain_plan.rs`

**Interfaces:**
- Produces: `Plan`, `TaskSpec`, `TaskPacket`, `TaskResult`, `TaskState`, `TaskKind`, `RiskClass`, `PermissionProfile`, `ProviderProfile`, `ProviderCapabilities`, `ProviderObservation`, `Budget`, `ApprovalGate`, `GateKind`, `DecisionRecord`, `EventRecord`, `DomainError`, and `AppConfig`.
- Produces: `Plan::new(goal, base_commit, target_branch, tasks)`, `Plan::validate()`, `Plan::refresh_ready_tasks()`, `TaskSpec::transition(next)`, and `AppConfig::load(user_path, repo_path)`.
- Consumes: none.

- [ ] **Step 1: Create the manifest and failing domain tests**

Create `Cargo.toml` with package name `orch-core`, library name `orch_core`, binary name `orch`, edition 2024, and the dependencies listed in the header. Create `src/lib.rs` that declares `pub mod config; pub mod domain;` without creating those modules yet.

Create `tests/domain_plan.rs` with literal fixtures and these cases:

```rust
#[test]
fn releases_only_tasks_with_succeeded_dependencies() {
    let mut plan = plan_with_tasks(vec![
        task("inspect", &[], TaskState::Planned),
        task("implement", &["inspect"], TaskState::Planned),
    ]);
    plan.refresh_ready_tasks().unwrap();
    assert_eq!(plan.task("inspect").unwrap().state, TaskState::Ready);
    assert_eq!(plan.task("implement").unwrap().state, TaskState::Planned);

    let inspect = plan.task_mut("inspect").unwrap();
    inspect.transition(TaskState::Running).unwrap();
    inspect.transition(TaskState::Validating).unwrap();
    inspect.transition(TaskState::Succeeded).unwrap();
    plan.refresh_ready_tasks().unwrap();
    assert_eq!(plan.task("implement").unwrap().state, TaskState::Ready);
}

#[test]
fn rejects_cycles_before_dispatch() {
    let plan = plan_with_tasks(vec![
        task("a", &["b"], TaskState::Planned),
        task("b", &["a"], TaskState::Planned),
    ]);
    assert!(matches!(plan.validate(), Err(DomainError::DependencyCycle(_))));
}

#[test]
fn rejects_missing_dependencies() {
    let plan = plan_with_tasks(vec![task("a", &["missing"], TaskState::Planned)]);
    assert_eq!(
        plan.validate().unwrap_err(),
        DomainError::MissingDependency {
            task_id: "a".into(),
            dependency_id: "missing".into(),
        }
    );
}

#[test]
fn prevents_running_a_planned_task() {
    let mut task = task("a", &[], TaskState::Planned);
    assert!(matches!(
        task.transition(TaskState::Running),
        Err(DomainError::InvalidTransition { .. })
    ));
}
```

Add a configuration test that writes user TOML with `max_parallel = 4` and repository TOML with `max_parallel = 2`, then asserts `AppConfig::load` returns `2` while preserving user-only provider paths.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test domain_plan`

Expected: compilation fails because `src/domain.rs` and `src/config.rs` do not exist.

- [ ] **Step 3: Implement the minimum domain and configuration behavior**

Use UUID-backed newtypes for plan, run, task, approval, decision, and event identifiers. Make public contracts serializable with Serde. Store dependency and task IDs as stable strings in editable plans. Implement DAG validation with missing-dependency detection followed by depth-first cycle detection. Encode allowed transitions in `TaskSpec::transition`; direct state mutation remains crate-private outside test builders.

`AppConfig::load` merges defaults, user configuration, then repository configuration. Repository values override user policy values. Provider executable paths and endpoints remain user-only and cannot be overridden by repository configuration.

- [ ] **Step 4: Verify GREEN and quality gates**

Run: `cargo test --test domain_plan`

Expected: all domain and configuration tests pass.

Run: `cargo test`

Run: `cargo fmt --check`

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Expected: every command exits successfully without warnings.

- [ ] **Step 5: Commit**

When Git is available: `git add Cargo.toml src/lib.rs src/domain.rs src/config.rs tests/domain_plan.rs && git commit -m "feat: add orchestration domain model"`

### Task 2: SQLite state, event log, artifacts, and leases

**Files:**
- Create: `src/store.rs`
- Create: `src/artifacts.rs`
- Create: `src/redaction.rs`
- Modify: `src/lib.rs`
- Create: `tests/store_recovery.rs`

**Interfaces:**
- Consumes: Task 1 domain identifiers and serializable contracts.
- Produces: `Store::open(path)`, `save_plan`, `load_plan`, `append_event`, `claim_task`, `complete_task`, `active_leases`, `reconcile_unknown_leases`, `record_decision`, `Redactor::apply`, `ArtifactStore::put`, `ArtifactStore::put_transcript`, and retention pruning.
- `Store::claim_task(plan_id, task_id, worker_id, now) -> Result<ClaimOutcome, StoreError>` returns `Claimed(Lease)` or `AlreadyClaimed(Lease)`.
- `ArtifactStore::put(bytes) -> Result<ArtifactRef, ArtifactError>` returns SHA-256, size, and canonical path.

- [ ] **Step 1: Write failing persistence tests**

Create `tests/store_recovery.rs` with these cases:

```rust
#[test]
fn persists_and_loads_a_plan() {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::open(temp.path().join("orch.db")).unwrap();
    let plan = example_plan();
    store.save_plan(&plan).unwrap();
    assert_eq!(store.load_plan(plan.id).unwrap(), Some(plan));
}

#[test]
fn appending_the_same_event_id_is_idempotent() {
    let store = test_store();
    let event = example_event("evt-fixed");
    assert_eq!(store.append_event(&event).unwrap(), AppendOutcome::Inserted);
    assert_eq!(store.append_event(&event).unwrap(), AppendOutcome::Duplicate);
    assert_eq!(store.events_for(event.run_id).unwrap().len(), 1);
}

#[test]
fn only_one_worker_can_claim_a_task() {
    let store = store_with_ready_task();
    let first = store.claim_task(plan_id(), "task", "worker-a", fixed_time()).unwrap();
    let second = store.claim_task(plan_id(), "task", "worker-b", fixed_time()).unwrap();
    assert!(matches!(first, ClaimOutcome::Claimed(_)));
    assert!(matches!(second, ClaimOutcome::AlreadyClaimed(lease) if lease.worker_id == "worker-a"));
}

#[test]
fn restart_marks_unconfirmed_running_leases_for_attention() {
    let store = store_with_running_lease();
    let changed = store.reconcile_unknown_leases(&HashSet::new(), fixed_time()).unwrap();
    assert_eq!(changed, vec![task_id()]);
    assert_eq!(store.task_state(plan_id(), task_id()).unwrap(), TaskState::NeedsAttention);
}
```

Add an artifact test that stores `b"same payload"` twice, receives the same lowercase SHA-256 reference, and observes only one file. Add transcript tests that replace configured literal and regular-expression secrets before disk writes, reject invalid redaction expressions during configuration loading, and prune expired transcript artifacts without pruning referenced patches or validation reports.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test store_recovery`

Expected: compilation fails because `orch_core::store` and `orch_core::artifacts` do not exist.

- [ ] **Step 3: Implement schema and transactional operations**

Create schema version 1 with tables for plans, tasks, results, events, leases, decisions, provider observations, budgets, and approvals. Store editable contracts as JSON alongside indexed state columns. Use `BEGIN IMMEDIATE` for claims and result ingestion. Make event IDs and active `(plan_id, task_id)` leases unique. Persist migration version in `schema_migrations`.

Write artifacts to `<root>/<first-two-hash-bytes>/<full-hash>` using a temporary file and atomic rename. Verify existing content length before reuse. Apply `Redactor` before hashing transcript bytes so secret-bearing content never reaches the artifact directory. Store artifact kind and retention deadline in SQLite; pruning deletes only expired, unreferenced transcripts.

- [ ] **Step 4: Verify GREEN and quality gates**

Run: `cargo test --test store_recovery`

Run: `cargo test`

Run: `cargo fmt --check`

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Expected: every command exits successfully.

- [ ] **Step 5: Commit**

When Git is available: `git add src/lib.rs src/store.rs src/artifacts.rs src/redaction.rs tests/store_recovery.rs && git commit -m "feat: persist runs and recovery state"`

### Task 3: Adaptive routing and conservative budgets

**Files:**
- Create: `src/routing.rs`
- Modify: `src/lib.rs`
- Create: `tests/routing_policy.rs`

**Interfaces:**
- Consumes: `TaskSpec`, `TaskKind`, `ProviderProfile`, `ProviderCapabilities`, `Budget`, and stored `ProviderObservation` values.
- Produces: `BudgetLedger::can_dispatch(profile, estimate)`, `Router::rank(task, candidates, context)`, `Router::select(task, candidates, context)`, and `RoutingDecision { selected, ranked, factors, exploratory }`.
- `RoutingContext` contains current time, run budget, active counts, observations, deterministic exploration seed, and deadline pressure.

- [ ] **Step 1: Write failing routing tests**

Create tests for capability filtering and hard budgets:

```rust
#[test]
fn excludes_providers_without_required_write_capability() {
    let task = implementation_task_requiring_workspace_write();
    let local = provider("local", capabilities(false), uncapped_budget());
    let codex = provider("codex", capabilities(true), capped_budget(10, 2));
    let ranked = Router::default().rank(&task, &[local, codex], &context()).unwrap();
    assert_eq!(
        ranked.iter().map(|r| r.provider_id.as_str()).collect::<Vec<_>>(),
        vec!["codex"]
    );
}

#[test]
fn protects_the_capped_reserve_when_usage_is_unknown() {
    let ledger = BudgetLedger::new(capped_budget(10, 2)).with_consumed_turns(8);
    let decision = ledger.can_dispatch(&UsageEstimate { turns: 1, active_seconds: 60 });
    assert_eq!(decision, BudgetDecision::Denied(BudgetDenial::ProtectedReserve));
}
```

Create the required adaptive test with literal observations: Big Pickle has three accepted reviews averaging 300 seconds and three accepted implementations averaging 3600 seconds; Local Fast has two accepted implementations averaging 240 seconds and one rejected review. Assert Big Pickle ranks first for `TaskKind::Review`, Local Fast ranks first for time-sensitive `TaskKind::Implement`, and both decisions cite the relevant acceptance and latency observations.

Add tests for max concurrency, hard per-run turn ceilings, per-task ceilings, reset-window rollover, cold-start priors, recency weighting, and deterministic exploration disabled when reserve headroom is absent.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test routing_policy`

Expected: compilation fails because routing APIs do not exist.

- [ ] **Step 3: Implement deterministic adaptive scoring**

Filter candidates by required capabilities, risk allowance, active concurrency, and budget before scoring. Compute recency-weighted acceptance, normalized latency, rework, intervention, and capped-turn estimates per task archetype. Use configured priors when fewer than three comparable observations exist. Prefer an uncapped candidate when its predicted acceptance is within 0.10 of the highest eligible candidate. Allow deterministic exploration only when hard limits and reserve remain satisfied. Persist factor names and source observation counts in the decision.

- [ ] **Step 4: Verify GREEN and quality gates**

Run: `cargo test --test routing_policy`

Run: `cargo test`

Run: `cargo fmt --check`

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Expected: every command exits successfully.

- [ ] **Step 5: Commit**

When Git is available: `git add src/lib.rs src/routing.rs tests/routing_policy.rs && git commit -m "feat: add adaptive provider routing"`

### Task 4: Provider adapters and Herdr client

**Files:**
- Create: `src/providers/mod.rs`
- Create: `src/providers/command.rs`
- Create: `src/herdr.rs`
- Modify: `src/lib.rs`
- Create: `tests/provider_contracts.rs`
- Create: `tests/fixtures/providers/claude_success.jsonl`
- Create: `tests/fixtures/providers/codex_success.jsonl`
- Create: `tests/fixtures/providers/agy_success.jsonl`
- Create: `tests/fixtures/providers/opencode_success.jsonl`

**Interfaces:**
- Consumes: `TaskPacket`, `ProviderProfile`, `ArtifactRef`, and normalized usage/result contracts.
- Produces: `ProviderAdapter`, `ProviderKind`, `Invocation`, `ProviderEvent`, `ProviderError`, `built_in_adapter(kind)`, and `HerdrClient`.
- `ProviderAdapter::build_invocation(&TaskPacket, &ProviderProfile) -> Result<Invocation, ProviderError>`.
- `ProviderAdapter::parse_line(&str) -> Result<Option<ProviderEvent>, ProviderError>` preserves malformed input in `ProviderError::MalformedEvent { raw, source }`.
- `HerdrClient` exposes `snapshot`, `start_agent`, `prompt_and_wait`, `read_agent`, and `subscribe` through an injected `CommandExecutor`.

- [ ] **Step 1: Capture supported native CLI surfaces**

Run and save the relevant output in the task ledger, not the repository:

```bash
herdr agent --help
herdr api --help
claude --help
codex exec --help
agy --help
opencode run --help
```

Use only flags shown by the installed versions. If a documented structured flag is absent, mark that capability false and use the adapter's text fallback rather than inventing an argument.

- [ ] **Step 2: Write failing adapter tests**

Create fixture-based tests that assert:

```rust
#[test]
fn codex_invocation_is_structured_and_scoped_to_the_worktree() {
    let invocation = built_in_adapter(ProviderKind::Codex)
        .build_invocation(&packet("Fix parser"), &profile_with_path("/usr/bin/codex"))
        .unwrap();
    assert_eq!(invocation.program, PathBuf::from("/usr/bin/codex"));
    assert!(invocation.args.windows(2).any(|v| v == ["--cd", "/tmp/task-worktree"]));
    assert!(invocation.args.iter().any(|v| v == "--json"));
}

#[test]
fn malformed_json_retains_the_raw_line() {
    let error = built_in_adapter(ProviderKind::Claude)
        .parse_line("{bad json")
        .unwrap_err();
    assert!(matches!(error, ProviderError::MalformedEvent { raw, .. } if raw == "{bad json"));
}
```

Add one fixture test per provider for completed, failed, blocked, usage, and resumable-session events when the native CLI exposes them. Add Herdr decoding tests for `session.snapshot`, `pane.agent_status_changed`, `pane.exited`, `events_lost`, and an occupant mismatch during a wait.

- [ ] **Step 3: Verify RED**

Run: `cargo test --test provider_contracts`

Expected: compilation fails because provider and Herdr modules do not exist.

- [ ] **Step 4: Implement adapters and transport boundaries**

Represent process arguments as `OsString` arrays and never concatenate shell commands. Implement Claude, Codex, Agy, and OpenCode as native CLI adapters. Implement the local endpoint adapter with Reqwest against an OpenAI-compatible chat-completions endpoint. Keep raw native events in artifacts while emitting the normalized event vocabulary.

Prefer Herdr CLI wrappers for individual commands and use its socket protocol for subscriptions. Treat `events_lost` as a recovery signal that requires a fresh snapshot before resubscription.

- [ ] **Step 5: Verify GREEN and quality gates**

Run: `cargo test --test provider_contracts`

Run: `cargo test`

Run: `cargo fmt --check`

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Expected: every command exits successfully.

- [ ] **Step 6: Commit**

When Git is available: `git add src/lib.rs src/providers src/herdr.rs tests/provider_contracts.rs tests/fixtures/providers && git commit -m "feat: add provider and herdr adapters"`

### Task 5: Scheduler, next-action policy, and Git isolation

**Files:**
- Create: `src/scheduler.rs`
- Create: `src/git.rs`
- Create: `src/orchestrator.rs`
- Modify: `src/lib.rs`
- Create: `tests/orchestration_flow.rs`

**Interfaces:**
- Consumes: Tasks 1-4 domain, store, routing, provider, artifact, and Herdr interfaces.
- Produces: `Scheduler::ready_tasks`, `NextActionPolicy::after_result`, `GitWorkspace`, `Orchestrator::tick`, and `OrchestrationAction`.
- `OrchestrationAction` variants are `Dispatch`, `Validate`, `Repair`, `Reroute`, `ComposeInternal`, `CreateGate`, `StopWorker`, and `None`.
- `GitBackend` and `ExecutionBackend` traits isolate real Git and Herdr/provider side effects from policy tests.

- [ ] **Step 1: Write failing orchestration tests**

Use a real temporary SQLite store with in-memory fake backends. Write these cases:

```rust
#[test]
fn dispatches_independent_tasks_but_serializes_overlapping_write_scopes() {
    let mut harness = Harness::new(plan_with_three_ready_tasks());
    let actions = harness.tick();
    assert_dispatches(&actions, &["api", "docs"]);
    assert_not_dispatched(&actions, "api-overlap");
}

#[test]
fn repeated_validation_failure_reroutes_after_one_same_session_repair() {
    let mut harness = Harness::new(plan_with_ready_task());
    harness.complete_with_failed_check("task", "cargo test");
    assert!(matches!(
        harness.tick().as_slice(),
        [OrchestrationAction::Repair { same_session: true, .. }]
    ));
    harness.complete_with_failed_check("task", "cargo test");
    assert!(matches!(
        harness.tick().as_slice(),
        [OrchestrationAction::Reroute { .. }]
    ));
}

#[test]
fn internal_composition_never_updates_target_branch_without_approval() {
    let mut harness = Harness::new(validated_task_with_commit("abc123"));
    harness.tick();
    assert_eq!(harness.git.internal_commits(), vec!["abc123"]);
    assert!(harness.git.target_updates().is_empty());
    assert!(harness.store.pending_gate(GateKind::FinalIntegration).is_some());
}
```

Add tests for dependency release, provider-budget exhaustion, blocked permission prompts, cancellation ordering, composition conflict gates, idempotent duplicate results, stale approval rejection, and target integration after a matching approval ID. Add permission-policy cases proving that an out-of-worktree write, undeclared network requirement, credential request, destructive Git command, or scope-changing graph revision creates an approval gate without calling the execution backend.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test orchestration_flow`

Expected: compilation fails because scheduler, Git, and orchestrator APIs do not exist.

- [ ] **Step 3: Implement pure policy before side effects**

Implement path-overlap detection with normalized repository-relative paths. Read-only tasks may share a checkout; mutating tasks use task worktrees. `Scheduler::ready_tasks` is deterministic by priority then task ID. `NextActionPolicy` allows one same-session repair per failure signature, then reroutes; high-risk, permission-sensitive, scope-changing, or exhausted-budget actions create gates. `Orchestrator::tick` persists a decision before executing its side effect and records the side-effect outcome under the same decision ID.

`GitWorkspace` creates an internal branch namespace `orch/run/<run-id>` and task worktrees under the configured state directory. Validate the pinned base commit before each creation. Only `integrate_target(approval_id)` may update the target branch, and it must consume an unexpired matching approval.

- [ ] **Step 4: Verify GREEN and quality gates**

Run: `cargo test --test orchestration_flow`

Run: `cargo test`

Run: `cargo fmt --check`

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Expected: every command exits successfully.

- [ ] **Step 5: Commit**

When Git is available: `git add src/lib.rs src/scheduler.rs src/git.rs src/orchestrator.rs tests/orchestration_flow.rs && git commit -m "feat: orchestrate task lifecycle"`

### Task 6: Local daemon, IPC, and crash reconciliation

**Files:**
- Create: `src/ipc.rs`
- Create: `src/daemon.rs`
- Modify: `src/lib.rs`
- Create: `tests/daemon_recovery.rs`

**Interfaces:**
- Consumes: `Store`, `Orchestrator`, Herdr snapshots/events, and public domain views.
- Produces: versioned newline-delimited JSON request/response envelopes, `Daemon::serve`, `DaemonClient`, `RunView`, single-instance locking, event streaming, and startup reconciliation.
- Requests include `Ping`, `SubmitPlan`, `StartRun`, `GetRun`, `ListRuns`, `Approve`, `Reject`, `Pause`, `Resume`, `Cancel`, `Retry`, `Reroute`, `Explain`, `Integrate`, and `SubscribeRun`.

- [ ] **Step 1: Write failing daemon tests**

Create asynchronous tests that bind a socket under a temporary directory:

```rust
#[tokio::test]
async fn duplicate_start_run_requests_do_not_duplicate_dispatch() {
    let daemon = TestDaemon::start().await;
    let request_id = "req-fixed";
    let first = daemon.client.start_run(request_id, daemon.plan_id).await.unwrap();
    let second = daemon.client.start_run(request_id, daemon.plan_id).await.unwrap();
    assert_eq!(first, second);
    assert_eq!(daemon.backend.dispatch_count(), 1);
}

#[tokio::test]
async fn startup_requires_attention_for_unmatched_running_leases() {
    let daemon = TestDaemon::start_with_running_lease_and_empty_herdr().await;
    let run = daemon.client.get_run(daemon.run_id).await.unwrap();
    assert_eq!(run.task("task").state, TaskState::NeedsAttention);
    assert_eq!(daemon.backend.dispatch_count(), 0);
}
```

Add tests for protocol-version mismatch, stale socket cleanup only when no live owner responds, event replay from a SQLite sequence number, `events_lost` snapshot reconciliation, and graceful shutdown preserving active leases.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test daemon_recovery`

Expected: compilation fails because IPC and daemon APIs do not exist.

- [ ] **Step 3: Implement daemon and protocol**

Use a Unix domain socket on Unix and a loopback TCP fallback behind the same transport trait for future Windows support. Every mutating request carries a client-generated request ID persisted with its response. The daemon acquires an exclusive state-directory lock, loads SQLite, subscribes to Herdr, snapshots authoritative state, reconciles leases, and only then accepts dispatch-capable requests.

Stream run events with monotonic SQLite sequence numbers. A reconnecting client supplies its last sequence and receives stored events before live events. Bound each request and event to 1 MiB and reject larger frames.

- [ ] **Step 4: Verify GREEN and quality gates**

Run: `cargo test --test daemon_recovery`

Run: `cargo test`

Run: `cargo fmt --check`

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Expected: every command exits successfully.

- [ ] **Step 5: Commit**

When Git is available: `git add src/lib.rs src/ipc.rs src/daemon.rs tests/daemon_recovery.rs && git commit -m "feat: add durable orchestration daemon"`

### Task 7: Public CLI, planning flow, diagnostics, and terminal view

**Files:**
- Create: `src/main.rs`
- Create: `src/cli.rs`
- Create: `src/tui.rs`
- Modify: `src/lib.rs`
- Create: `tests/cli_e2e.rs`
- Create: `README.md`
- Create: `.gitignore`

**Interfaces:**
- Consumes: daemon client and all read-only run views.
- Produces: `orch init`, `doctor`, `plan`, `run`, `status`, `attach`, `approve`, `reject`, `pause`, `resume`, `cancel`, `retry`, `reroute`, `explain`, and `integrate`.
- All read commands accept `--json`. Mutating commands print the persisted decision or gate ID.
- `plan` writes `.orch/plans/<slug>-<plan-id>.toml`; `run --dry-run` performs validation and routing without starting workers.

- [ ] **Step 1: Write failing CLI tests**

Use `assert_cmd` against the real binary:

```rust
#[test]
fn plan_writes_an_editable_four_stage_fallback_graph() {
    let repo = TestRepo::new();
    orch(&repo)
        .args(["plan", "add parser validation", "--no-model-planner"])
        .assert()
        .success();
    let plan = repo.only_plan();
    assert_eq!(plan.goal, "add parser validation");
    assert_eq!(plan.task_ids(), vec!["inspect", "implement", "validate", "review"]);
    assert_eq!(plan.task("implement").dependencies, vec!["inspect"]);
}

#[test]
fn integrate_refuses_to_touch_target_without_matching_approval() {
    let repo = TestRepo::with_integration_ready_run();
    orch(&repo)
        .args(["integrate", &repo.run_id()])
        .assert()
        .failure()
        .stderr(predicates::str::contains("final integration approval required"));
    assert_eq!(repo.target_head(), repo.original_head());
}
```

Add tests for `init` configuration creation without credentials, `doctor --json` provider/Herdr capability output, invalid plan rejection, `run --dry-run` explanations, `status --json`, `explain`, approve/reject, pause/resume/cancel, retry/reroute, and daemon auto-start failure messages. Test the TUI through a pure `render_dashboard(model, area) -> Buffer` function for running, blocked, budget-exhausted, and completed views.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test cli_e2e`

Expected: compilation fails because the binary and CLI modules do not exist.

- [ ] **Step 3: Implement the command surface**

Use Clap derive types. `init` discovers executables with `PATH`, invokes version/help probes without starting agents, and writes only repository-safe policy. `doctor` reports `ok`, `degraded`, or `unavailable` per provider capability. `plan` uses a configured planner adapter when present and validates its schema; `--no-model-planner` uses the four-stage fallback. `attach` opens the Ratatui dashboard and consumes daemon events until quit.

Exit codes are `0` success, `2` invalid user input or plan, `3` approval required, `4` unavailable dependency, and `5` runtime failure. JSON output uses a stable envelope containing `schema_version`, `ok`, `data`, and `error`.

- [ ] **Step 4: Write operator documentation**

Document installation, Herdr dependency, provider configuration, local endpoint configuration, budget and reserve semantics, task states, approval gates, recovery behavior, data locations, transcript retention, JSON mode, and a complete dry-run walkthrough.

- [ ] **Step 5: Verify GREEN and quality gates**

Run: `cargo test --test cli_e2e`

Run: `cargo test --all-targets --all-features`

Run: `cargo fmt --check`

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Run: `cargo build --release`

Expected: every command exits successfully.

- [ ] **Step 6: Commit**

When Git is available: `git add src/lib.rs src/main.rs src/cli.rs src/tui.rs tests/cli_e2e.rs README.md .gitignore && git commit -m "feat: ship orch terminal workflow"`

### Task 8: Whole-product verification and review

**Files:**
- Modify only files required by findings reproduced with failing tests.

**Interfaces:**
- Consumes: the complete implementation and product requirements.
- Produces: fresh verification evidence, a requirement checklist, and reviewed release state.

- [ ] **Step 1: Run the full automated gates**

Run: `cargo test --all-targets --all-features`

Run: `cargo fmt --check`

Run: `cargo clippy --all-targets --all-features -- -D warnings`

Run: `cargo build --release`

Expected: every command exits successfully with zero failed tests and zero warnings.

- [ ] **Step 2: Run a temporary-repository smoke test**

In a temporary valid Git repository, run:

```bash
orch init
orch doctor --json
orch plan "add a validation helper" --no-model-planner
orch run .orch/plans/*.toml --dry-run --json
orch status --json
orch explain --latest --json
```

Expected: each command exits 0; the plan has four stages; dry-run output includes routing and budget factors; the target branch commit is unchanged.

- [ ] **Step 3: Review requirements and failure modes**

Read the spec section by section and map every requirement to an implementation path and test. Review especially the five items under Review Focus, shell argument safety, secret redaction, request idempotency, task path normalization, stale approvals, and adapter downgrade behavior.

- [ ] **Step 4: Fix release-blocking findings through TDD**

For every Critical or Important finding, write a focused test that reproduces it, run the test and observe the expected failure, implement the smallest correction, rerun the focused test, and rerun the complete suite. Record Minor findings without changing code.

- [ ] **Step 5: Commit verified fixes**

When Git is available: `git add Cargo.toml Cargo.lock src tests README.md .gitignore && git commit -m "fix: address final orchestration review"`
