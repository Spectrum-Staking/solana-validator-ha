# Design: Alpenglow-aware failover

Status: **implemented, pending live test** (see §12 for deviations) · Branch: `feat/alpenglow-support` · Verified against agave `v4.3.0-rc.1` (`2e10d67f90`)

## 1. Problem

`solana-validator-ha` decides that the active peer is down when, for `leaderless_samples_threshold`
consecutive samples, the active identity is either missing from gossip or not voting. "Not voting"
means it appears in `getVoteAccounts.delinquent`, or has no vote account at all.

After the Alpenglow migration, the gossip signal is unchanged, but the voting signal changes meaning:

| Signal | TowerBFT | Alpenglow |
|---|---|---|
| `lastVote` updated by | vote transactions landing | block-footer processing only: reward cert for slot S−8 in block S, or signer of an included finalization cert (`runtime/src/block_component_processor/vote_reward.rs`) |
| Healthy `slot − lastVote` | ~1–3 | ~1–9 (8-slot reward offset + skipped leaders) |
| `getHealth` compares against | latest optimistic slot seen via gossip | Votor's highest **finalized** slot. Returns `Unknown` until one has been observed (`rpc/src/rpc_health.rs`) |
| What stops voting that failover can't fix | identity balance below rent-exempt (vote tx fees) | vote account fails VAT (no BLS key, balance < VAT minimum) or falls outside top `MAX_ALPENGLOW_VOTE_ACCOUNTS` (`bank.rs get_vat_health_for_next_epoch`) |
| Cluster-wide stall | rare | Votor "standstill" (`DELTA_STANDSTILL` = 10s without finalization): **every** validator's `lastVote` freezes |

Consequences for the current code:

1. The 128-slot delinquency check is a slow proxy (~51s + samples ≈ 66s to fail over a zombie node).
2. `delinquency_bypass` and a low `delinquent_slot_distance_override` would fail over during a
   cluster-wide stall, where failover cannot help and adds equivocation risk.
3. The identity low-balance exemption is obsolete. Alpenglow votes are BLS messages, not transactions.
4. **Latent bug:** a VAT-excluded vote account likely disappears from `getVoteAccounts`. Today that
   path logs "no current or delinquent vote account found" and counts a leaderless sample, so peers
   would fail over to each other in a loop, and none of them can vote.
5. Around the migration, TowerBFT votes stop and certs only cover slots after the Alpenglow genesis
   slot `G`, so `lastVote` stalls for everyone for a while.

## 2. Goals / non-goals

**Goals**
- Detect the consensus phase (tower / migrating / alpenglow) automatically from RPC, with a manual override.
- Keep TowerBFT behaviour **byte-for-byte unchanged** while the cluster is on tower.
- On Alpenglow: detect a zombie active (in gossip, votes not landing) in ~25–30s, but never fail over
  on vote-based evidence while the cluster itself is not finalizing.
- Never fail over for conditions a failover can't fix (VAT exclusion).
- Never promote a local node that hasn't migrated or is on a different Alpenglow genesis.
- Full observability: metrics, logs, recording fields, replay.

**Non-goals**
- Per-slot reward-cert inclusion tracking. That needs an agave patch or Geyser footer decoding; see
  the monitoring design in agave memory. It can be added later as an optional external signal.
- Syncing vote-history files on failover. That is the job of the user-supplied active command,
  documented in the README only.
- Changing the gossip-absence trigger.

## 3. Detecting the consensus phase

### 3.1 RPC signals

| Source | Call | Meaning |
|---|---|---|
| Feature gate `A1pengvuM6JEcyNuTnMqepBKhwHE3N6PmUrdATGawhJS` | `getAccountInfo` (base64). Data is bincode `Feature { activated_at: Option<u64> }`, i.e. 9 bytes `[1, u64 LE]` when active | Migration scheduled: `migration_slot = activated_at + 5000` |
| `getAgGenesisCert` (BankData RPC, finalized bank) | `{"method":"getAgGenesisCert"}` | `null` while still on tower or migrating; `{"block":{"slot":G,...}}` once Alpenglow is enabled. Blocks after `G` are Alpenglow |

`getAgGenesisCert` is new. RPCs that don't implement it answer JSON-RPC `-32601 Method not found`.

### 3.2 Phase state machine

```
            feature active            genesis cert seen
  TOWER ─────────────────▶ MIGRATING ─────────────────▶ ALPENGLOW (sticky)
    ▲                          │
    └── feature not active ────┘   (only possible before cert; e.g. RPC flapping)

  UNKNOWN: no RPC answered either query → behaves like MIGRATING (conservative)
```

Rules:
- **ALPENGLOW is sticky.** Once a genesis cert with slot `G` is observed, the detector never leaves
  ALPENGLOW for the rest of the process lifetime. A lagging or old RPC returning `null` is logged at
  debug level and ignored. A cert with a *different* `G` is logged as an error and ignored, because
  the first value wins.
- `getAgGenesisCert` errors of type `-32601` mark that URL as "unsupported" for the process lifetime,
  and the next URL is tried. If no URL supports it, the local validator RPC is queried as a fallback.
- **Poll cadence:** every `cluster.consensus.detection_interval_duration` (default 60s) while not yet
  ALPENGLOW. Once ALPENGLOW, re-verify every 10 min for logging only.
- `cluster.consensus.mode: tower|alpenglow` pins the phase and skips detection. In pinned `alpenglow`
  mode, `G` is still fetched once, because the warm-up guard needs it. If it's unavailable, warm-up
  is skipped with a warning.

### 3.3 Local-node check
The detector also calls `getAgGenesisCert` on `validator.rpc_url` each detection cycle. When the
cluster phase is ALPENGLOW, the local node is **ineligible to be promoted** unless its cert exists and
its slot equals the cluster's `G`. This check comes on top of `getHealth`, which returns `Unknown`
until Votor sees a finalization. Mismatch reasons: `local_not_migrated` or `local_genesis_mismatch`.

## 4. Failover logic per phase

```
failover when, for N consecutive samples:
    active_down  AND  self_eligible  AND  no veto
```

| Phase | `active_down` | Vote evidence | Vetoes added | `delinquency_bypass` |
|---|---|---|---|---|
| TOWER | gossip-absent **or** delinquent/no vote account | as today (128 slots / override, identity balance exemption) | none | as configured |
| UNKNOWN / MIGRATING | gossip-absent only | ignored. The active in gossip counts as active | none | ignored (warn once) |
| ALPENGLOW, warm-up (`finalized ≤ G + warmup_slots`) | gossip-absent only | ignored | none | ignored |
| ALPENGLOW, steady | gossip-absent **or** `lag > vote_lag_slots_threshold` while cluster live | lag-based (below) | stall, VAT/excluded, local genesis | ignored (superseded) |

### 4.1 Alpenglow vote evaluation (replaces `isNodeActiveAndVoting` in ALPENGLOW phase)

```
verdict evaluateAlpenglowVoting(node):
    r, err := clusterRPC.GetVoteLag(votePubkey)   // getVoteAccounts{keepUnstakedDelinquents:true} + getSlot(processed), SAME URL
    if err                         → VOTING (assume innocence, as today)
    if account not found / 0 stake → EXCLUDED        // veto; failover can't fix. Error log + metric
    lag := r.processedSlot - r.lastVote
    if lag <= L                    → VOTING
    if !clusterLive                → VOTING, set stalled=true   // everyone is frozen; not the active's fault
    if stakeGateEnabled && ratio < min → VOTING, set stalled=true
    else                           → NOT_VOTING (reason=vote_lag, lag, L)
```

- **Same-URL pinning.** `lastVote` and the reference slot must come from the same RPC. Otherwise
  URL rotation mixes banks from different nodes and the lag is meaningless. New
  `rpc.Client.GetVoteLag` runs both calls inside one `executeWithRetry` operation, so both calls use
  one `*rpc.Client`.
- **`clusterLive`.** Once per sample, call `getSlot(finalized)` (cheap). Keep a monotonic max, since
  different URLs may be slightly behind. Live means the max advanced within
  `finalization_stall_duration` (default 15s, which is > `DELTA_STANDSTILL` 10s plus a poll interval).
- **Stake gate (optional, default on).** Every `network_stake_check_interval_duration` (default 60s),
  make one unfiltered `getVoteAccounts`. Compute `ratio = Σcurrent.activatedStake / Σ(current+delinquent)`.
  Cache the value, and treat it as stale after 3 intervals. `network_current_stake_ratio_min: 0`
  disables the gate.
- The identity-balance lookup is skipped in ALPENGLOW.
- Delinquency detail in recordings is reused for lag: `LastVoteSlot`, `CurrentSlot`, `SlotDistance`.

**Why this is sound.** A finalization certificate needs ≥60% of stake. If the cluster keeps
finalizing and the active's `lastVote` doesn't advance, its votes are not reaching leaders or
certs, so the fault is local to the active node.

### 4.2 Vetoes in the manager
Checked in `ensureHAState` right after the threshold is reached, and again after the rank delay.
They apply only when the leaderless evidence includes vote-based samples. Gossip-absence failover
is never vetoed by these; a dead host is dead during a stall too.

| Veto | Outcome result | Timeline event |
|---|---|---|
| cluster stalled (no finalization / low stake ratio) | `aborted_cluster_stalled` | `veto_cluster_stalled` |
| active vote account EXCLUDED | `aborted_vote_account_excluded` | `veto_vote_account_excluded` |
| local not migrated / genesis mismatch (eligibility) | `aborted_local_not_migrated` | `veto_local_genesis` |

To tell the evidence types apart, `gossip.State` tracks per-sample `LeaderlessReason`
(`gossip_absent` | `vote_lag` | `no_vote_account` | `delinquent`). The veto applies only if **no**
sample in the current leaderless streak was `gossip_absent`.

### 4.3 Timing (defaults)
- Zombie active: lag reaches 32 slots (≈12.8s at 400ms), then 3 samples (≈11–15s) ≈ **25–30s**. Today it is ≈66s.
- Host down: unchanged, ≈11–15s.
- Thresholds are in **slots**, not seconds. This agave tree supports per-slot durations (`ns_per_slot_at_slot`).

## 5. Configuration

```yaml
cluster:
  consensus:
    # auto | tower | alpenglow. Default auto.
    mode: auto
    # How often to query the consensus phase before Alpenglow is detected. Default 60s.
    detection_interval_duration: 60s

failover:
  alpenglow:
    # Slots of (processed_slot - lastVote) above which the active is considered not voting. Default 32, min 16.
    vote_lag_slots_threshold: 32
    # Slots after the Alpenglow genesis slot G before vote evidence is trusted. Default 64.
    warmup_slots: 64
    # The cluster counts as stalled if the finalized slot hasn't advanced for this long. Default 15s.
    finalization_stall_duration: 15s
    # Minimum share of stake that must be current for vote evidence to count. 0 disables. Default 0.85.
    network_current_stake_ratio_min: 0.85
    # How often to compute the ratio above (unfiltered getVoteAccounts). Default 60s.
    network_stake_check_interval_duration: 60s
```

Validation:
- `vote_lag_slots_threshold ≥ 16`.
- `finalization_stall_duration ≥ poll_interval_duration`.
- The ratio must be in `[0,1]`.
- Startup warnings:
  - `mode: alpenglow` together with `delinquency_bypass: true`: the bypass is ignored.
  - `delinquent_slot_distance_override` is set and mode isn't `tower`: the override applies only in the tower phase.

## 6. Observability

**Metrics** (new, all with the common labels):

| Metric | Type | Notes |
|---|---|---|
| `solana_validator_ha_consensus_phase{phase}` | gauge | 1 for the current phase |
| `solana_validator_ha_alpenglow_genesis_slot` | gauge | `G`, 0 if unknown |
| `solana_validator_ha_local_alpenglow_genesis_match` | gauge | 1/0 |
| `solana_validator_ha_active_vote_lag_slots` | gauge | last evaluated lag, all phases (useful on tower too) |
| `solana_validator_ha_cluster_finalized_slot` | gauge | monotonic max |
| `solana_validator_ha_cluster_live` | gauge | 1/0 |
| `solana_validator_ha_network_current_stake_ratio` | gauge | |
| `solana_validator_ha_failover_vetoes_total{reason}` | counter | |

Values go through `cache.State`, like the existing metrics.

**Recordings: schema v3**
- `ConfigSnapshot` adds: `consensus_mode`, `vote_lag_slots_threshold`, `warmup_slots`,
  `finalization_stall_duration`, `network_current_stake_ratio_min`.
- `GossipSample` adds: `consensus_phase`, `alpenglow_genesis_slot`, `local_genesis_match`,
  `finalized_slot`, `cluster_live`, `network_stake_ratio`, `leaderless_reason`.
- Timeline events added: `consensus_phase_changed`, `veto_*`, `alpenglow_vote_lag_exceeded`.
- Replay accepts v1–v3. It shows the phase in the header and the lag/live columns when present.
  Add golden scenarios `alpenglow-zombie` and `alpenglow-stall`.

## 7. Code changes

| Area | File(s) | Change |
|---|---|---|
| RPC | `internal/rpc/clients.go` | `GetAgGenesisCert` (raw `RPCCallForInto`, typed result, `ErrMethodNotFound` detection), `GetFeatureActivationSlot`, `GetVoteLag` (pinned URL), `GetSlotWithCommitment` |
| Consensus | **new** `internal/consensus/{detector,phase}.go` + tests | Phase enum, sticky state machine, local-genesis check, test hooks |
| Config | `internal/config/{cluster,failover}.go` + **new** `alpenglow.go` | Structs, defaults, validation, warnings |
| Gossip state | `internal/gossip/state.go` | `Refresh(phase PhaseView)`; keep `isNodeActiveAndVoting` for tower; add `evaluateAlpenglowVoting`, finalized-slot tracker, stake-ratio cache, `LeaderlessReason`, `ActivePeerExcluded`, `ClusterStalled` |
| Manager | `internal/ha/manager.go` | Share one cluster RPC client; build the detector; call `detector.MaybeRefresh()` at the top of `ensureHAState`; vetoes + local-genesis eligibility; phase-change logging |
| Metrics / cache | `internal/prometheus/metrics.go`, `internal/cache/cache.go` | New gauges and counters |
| Recording | `internal/recording/{event,replay}.go` | Schema v3 + replay rendering + goldens |
| Mock RPC | `integration/mock-solana/main.go` | Advancing slot clock; `getAgGenesisCert`, `getAccountInfo` (feature), `getSlot` by commitment, per-validator lag; control endpoints `set_phase`, `set_vote_lag`, `stall_finalization`, `resume_finalization`, `set_local_genesis` |
| Orchestrator | `integration/test-orchestrator/main.go` | New actions matching the endpoints above |
| Docs | `README.md` | "Alpenglow" section; fix the "thresholds agree" claim; vote-history note for active/passive scripts |

## 8. Test plan

**Unit**
- Detector: feature-account decoding (inactive/active/malformed); `-32601` fallback; sticky ALPENGLOW;
  conflicting `G`; pinned modes; local genesis mismatch.
- `evaluateAlpenglowVoting` table tests: lag ≤ L, > L live, > L stalled, stake gate, rpc error,
  excluded, warm-up.
- Manager: vetoes apply only to vote-based streaks; gossip-absent failover still fires during a
  stall; tower phase unchanged. Re-run the existing manager tests with phase=TOWER.
- Config validation and defaults.
- Replay golden files for v3.

**Integration scenarios** (`integration/scenarios/`)
- `05-alpenglow-zombie-active`: active stays in gossip, lag rises past L → one passive takes over.
- `06-alpenglow-cluster-stall`: finalization stalls + lag rises → **no** failover; resume → no failover.
- `07-alpenglow-stall-plus-host-down`: stall + active disconnects from gossip → failover (gossip path).
- `08-migration-window`: phase=migrating, lag huge → no failover; disconnect → failover.
- `09-local-not-migrated`: the rank-0 passive has no local genesis → rank-1 takes over.
- `10-vote-account-excluded`: active's vote account missing → no failover, veto metric increments.
- `11-phase-sticky`: after alpenglow, mock flips cert to null → phase stays alpenglow.
- Existing scenarios 01–04 run unchanged with `mode: auto` on a tower mock.

## 9. Delivery plan

Each PR can be merged on its own; PR1 changes no failover behaviour.

| # | PR | Contents | Behaviour change |
|---|---|---|---|
| 1 | Phase detection | RPC methods, `internal/consensus`, config `cluster.consensus`, phase metrics, recording field, logs | none (observe only) |
| 2 | Alpenglow vote evaluation | `GetVoteLag`, finalized tracker, stake gate, `evaluateAlpenglowVoting`, lag metrics, `LeaderlessReason`, VAT exclusion handling | ALPENGLOW phase only |
| 3 | Manager vetoes + eligibility | stall/excluded/local-genesis vetoes, outcomes, recording v3, replay | ALPENGLOW phase only |
| 4 | Mock + integration | mock-solana extensions, scenarios 05–11, CI wiring | test only |
| 5 | Docs | README Alpenglow section and corrections | none |

For live testing, all five are stacked on `feat/alpenglow-support` and tagged as a pre-release
(`vX.Y.Z-alpenglow.N`) from that branch.

## 10. Live test plan

1. **Pre-flight on the target cluster.**
   - `solana alpenglow-genesis-info` confirms the phase.
   - Check that each configured `cluster.rpc_urls` entry answers `getAgGenesisCert`; note which return `-32601`.
2. **Shadow run (≥ 1 epoch).**
   - Run the branch build next to the production HA with `failover.dry_run: true` and separate
     metrics ports, on every node.
   - Compare the `active_vote_lag_slots` distribution (expect p99 ≤ ~12), how often `cluster_live`
     flaps, and the stake ratio.
   - Tune `vote_lag_slots_threshold` if p99.9 > L/2.
3. **Fault injection (testnet only, dry-run first, then live).**
   - Kill the validator process → gossip path, ≈11–15s.
   - Block outbound Votor/BLS traffic from the active while gossip stays up → zombie path. Expect
     ≈25–30s. Confirm the lag metric rises and that the new active's `lastVote` resumes.
   - Restart a passive and confirm it stays ineligible until `getHealth` = ok **and** the local
     genesis matches.
   - Planned failover via `solana-validator-failover` with the vote-history file copied. Verify no
     equivocation warnings in Votor logs.
4. **Promote.** Switch production nodes to the branch build with `dry_run: false` on testnet, then
   mainnet after its migration, keeping `mode: auto`.

## 11. Open questions (resolve during PR2 / live test)

1. Does a VAT-excluded or zero-stake vote account appear in `getVoteAccounts` with
   `keepUnstakedDelinquents: true`? This decides how EXCLUDED is detected. Fallback:
   `getAccountInfo` of the cached vote pubkey plus BLS-key presence.
2. The real healthy lag distribution, which may be skewed by the finalization-cert path. This sets
   the default for L.
3. Which third-party RPC providers support `getAgGenesisCert`. This decides whether the local-RPC
   fallback is the main path in practice.
4. Should `migrating` also disable delinquency *logging* noise? Probably downgrade it to warn.
5. Whether a future agave RPC exposes the migration phase directly, which would replace the feature-account inference.

## 12. Implementation notes

Where the implementation differs from the sections above, and why:

1. **Feature-gate query failures keep the last known phase** instead of dropping to UNKNOWN, so a
   transient RPC error does not flap a TowerBFT cluster to "gossip only". The phase is UNKNOWN only
   until the first successful answer.
2. **No per-URL "unsupported" marking.** Every cluster URL is asked for `getAgGenesisCert` on each
   detection cycle; if all answer `-32601` the local validator is asked. The extra call is cheap, and
   RPCs or a local validator that are upgraded while the process runs are picked up without a restart.
3. **Local genesis check cadence:** on every poll while the local node is ineligible, and every
   detection interval once it matches. A local RPC error keeps the previous result.
4. **Stall veto also fires when finalization trails the tip.** If `processed − finalized` exceeds
   `vote_lag_slots_threshold`, the cluster counts as stalled immediately. Without this, last votes
   freeze the moment finalization stops, but the time-based check only reacts after
   `finalization_stall_duration`, which leaves room for up to ~2 vote-lag samples. The first
   finalized-slot observation is a baseline: the cluster counts as live only after an advance is seen.
5. **A stale network stake ratio closes the stake gate** (vetoes) rather than being ignored.
6. **Where vetoes act.** As in the §4.1 pseudo-code, a stalled or excluded sample counts the active as
   present, so the leaderless streak resets. The manager therefore only re-checks stall/excluded
   vetoes after the rank delay; a check right after the threshold would be dead code.
   `failover_vetoes_total{reason}` counts vetoed samples plus aborted takeovers.
7. **Local-genesis eligibility applies to every takeover in the Alpenglow phase**, including
   gossip-absent ones: it is a property of this node, like health.
8. **`finalization_stall_duration` defaults to max(15s, `poll_interval_duration`)**, so existing
   configs with a slow poll stay valid on TowerBFT.
9. `cluster.consensus` and `failover.alpenglow` are validated in `Config.validate`.
   `GetFeatureActivationSlot` is named `GetFeatureStatus`.
10. **Integration scenarios are renumbered.** The migration window runs first (05) because HA clients
    never leave the alpenglow phase and the mock's `reset` keeps the phase. Scenario 11 (sticky) cannot
    exercise the null-certificate path within the 10-minute re-verify interval, so the unit tests cover it.
11. A vote-lag sample also sets `ActivePeerDelinquent`, so recordings and replay reuse the
    delinquency slot evidence as §4.1 suggests. `delinquency_bypass` is only honoured in the tower
    phase (warn once otherwise).

Open question 1 is still open: both a missing vote account and one with zero activated stake are
treated as excluded.
