# Agent Orchestrator Product Requirements

## Product summary

The product is a local-first terminal orchestrator for coding agents. It uses Herdr for terminal sessions, worktrees, agent lifecycle events, and native session visibility. A separate Rust daemon, CLI, and terminal control view add goal decomposition, task scheduling, adaptive provider routing, capped-usage protection, deterministic validation, and approval gates.

The working binary name is `orch`. The first release supports one user, one machine, local Git repositories, installed subscription CLIs, and an OpenAI-compatible local-model endpoint.

## Goal and success criteria

The primary job is to take a natural-language repository change through plan approval, mixed-provider execution, validation, internal composition, and an integration-ready review while leaving the target branch unchanged until the user approves integration.

The primary benchmark is at least 50% fewer capped-agent turns than one capped agent handling the same task end to end. The orchestrated result must match the baseline acceptance-test pass rate and must not add critical review findings.

## User workflow

1. `orch init` discovers Herdr and supported provider CLIs and writes repository policy without copying credentials.
2. `orch plan "<goal>"` gathers repository facts and creates an editable task DAG with predicted capped usage.
3. The user edits or approves the graph.
4. `orch run <plan>` dispatches ready tasks according to dependencies, budgets, risk, capabilities, and learned provider performance.
5. The control TUI shows the graph, running agents, provider budgets, validation evidence, blockers, and pending approvals. Native provider panes remain directly inspectable in Herdr.
6. The orchestrator validates results, retries or reroutes bounded failures, and releases dependent work.
7. `orch integrate <run>` presents the ordered internal commits and their evidence. It updates the target branch only after explicit approval.

## Architecture

### Runtime

A local daemon owns durable run state. SQLite stores current plans, tasks, leases, provider profiles, budgets, approvals, and an append-only event log. Large transcripts, patches, and reports are content-addressed files outside the repository.

Models may propose plans and typed graph revisions. Deterministic code owns state transitions, dependency release, dispatch, budgets, retry limits, permissions, approval gates, and crash recovery.

The task lifecycle is:

`planned -> ready -> running -> validating -> succeeded | failed | blocked | cancelled`

After reconnect or restart, the daemon reconciles SQLite state with Herdr panes, processes, worktrees, and Git state before dispatching more work. Unknown process state becomes `needs_attention` and is not relaunched automatically.

### Herdr integration

The product is a separate binary that connects to Herdr's socket API. A thin Herdr plugin entry point opens the control TUI. The runtime uses Herdr event subscriptions and agent waits instead of polling terminal output. Every active task lease pins the Herdr pane occupant and the provider's native resumable session reference when available.

### Workspaces and composition

Every mutating task runs in an isolated Git worktree. Read-only work may share a checkout. Tasks with overlapping write scopes are serialized unless the graph explicitly orders them.

Each validated mutating task produces a commit. The daemon may apply validated commits to an internal run branch so dependent tasks can start from composed work. The user's target branch remains unchanged until final approval.

## Plans and task contracts

A `Plan` contains the goal, pinned base commit, target branch, budget, tasks, and approval state. A `TaskSpec` contains:

- Stable identifier, title, and task archetype
- Dependencies and priority
- Read and write scopes
- Required provider capabilities
- Risk class and permission profile
- Expected artifact and structured result schema
- Deterministic acceptance commands
- Retry and reroute allowance
- Per-task usage and time budget

A `TaskResult` contains status, summary, artifact references, changed paths, commit, check outcomes, confidence, normalized usage, timing, failure category, and provider session reference.

Workers receive compact task packets containing only the goal fragment, relevant repository facts, upstream artifacts, allowed paths, checks, permissions, and response schema. Workers publish artifacts through the daemon rather than exchanging free-form messages.

## Scheduling and next actions

The scheduler dispatches only ready tasks whose dependency, concurrency, write-scope, budget, and approval conditions are satisfied.

After a worker settles:

1. Run deterministic validation.
2. On success, record the result and release dependents.
3. On validation failure, allow one focused repair in the same session when budget permits.
4. When the same failure repeats, reroute with a compressed failure packet.
5. Escalate to a capped worker when local attempts are exhausted, confidence is low, or risk policy requires it.
6. Pause for permission requests, scope-changing ambiguity, exhausted budgets, unresolved composition conflicts, or final integration.

Automatic graph changes are limited to repair and validation tasks within the approved goal and budget. Scope-changing graph changes require approval.

## Provider adapters and routing

The initial adapters are Claude, Codex, Agy, OpenCode, and one OpenAI-compatible local endpoint. Subscription providers use their installed CLIs and existing authentication. Adapters normalize:

- Structured and streaming output
- Lifecycle state and typed errors
- Native session identifiers and resume commands
- Filesystem and sandbox capabilities
- Context limits and model identity
- Reported usage and quota signals

Routing is capability-based. Each provider/model profile accumulates recency-weighted outcomes by task archetype and may be segmented by repository language or framework. Signals include acceptance and check-pass rates, rework, reviewer findings, latency, stalls, capped usage, human interventions, and failure category.

The router filters candidates that cannot satisfy capability, risk, or remaining-budget constraints. It ranks eligible candidates using the current task and learned outcome distributions. Configured priors handle cold starts. Limited exploration is allowed only when capped reserves and run budget have headroom. Every decision records the observations that changed the ranking.

## Budget policy

Provider budgets support per-run dispatch ceilings, maximum concurrency, reset windows, task ceilings, and protected reserves. Provider-reported quota data takes precedence. When a CLI does not expose comparable tokens or quota, the ledger uses turns and active time as conservative units.

The router must not dispatch a task that would cross a hard ceiling or spend the protected reserve without approval.

## Safety and recovery

The default task permission ladder allows repository reads, writes in the assigned worktree, configured acceptance commands, and the selected provider profile's declared network access. The following conditions create approval gates:

- Destructive Git operations or writes outside the worktree
- Credential access
- Commands outside declared policy
- Provider permission prompts
- Unexpected network requirements
- Changes beyond declared write scope
- Scope-changing graph revisions
- Updates to the target branch

The orchestrator never answers a blocked interactive prompt using blind terminal input. Credentials remain in native provider stores. Logs redact configured secret patterns. Raw transcripts have configurable retention and never enter repository state.

Dispatch and result recording are idempotent. Cancellation stops new dispatch first and then requests graceful worker shutdown. Force termination requires a separate confirmation.

## CLI and configuration

The public CLI is:

- `orch init`
- `orch doctor`
- `orch plan "<goal>"`
- `orch run <plan>`
- `orch status`
- `orch attach`
- `orch approve|reject <gate>`
- `orch pause|resume|cancel`
- `orch retry|reroute <task>`
- `orch explain <task|decision>`
- `orch integrate <run>`

Repository policy lives in `.orch/config.toml`. Machine-specific provider paths, quotas, endpoints, run databases, and artifacts live under user configuration and state directories.

The versioned public data contracts are `Plan`, `TaskSpec`, `TaskResult`, `ProviderProfile`, `Budget`, `ApprovalGate`, and `DecisionRecord`.

## Acceptance scenarios

The test suite covers independent parallel tasks, dependency release, overlapping-write serialization, blocked prompts, stalled workers, validation repair, rerouting, provider exhaustion, daemon and Herdr restart, adapter capability drift, composition conflicts, cancellation, protected reserves, and final approval.

The adaptive-routing fixture includes a provider called Big Pickle. Strong review quality and slow independent implementation must raise its review rank and lower its time-sensitive implementation rank, with the decision explanation tied to stored observations.

## Initial release boundaries

The first release does not include remote workers, team accounts, shared credentials, model hosting, paid-API billing optimization, automatic target-branch integration, a replacement terminal multiplexer, or autonomous expansion beyond the approved goal. Reusable workflow recipes and richer Herdr sidebar extensions follow the core orchestration loop.
