# ZERO OR HERO: PAPER software validation

Independent code-only preparation for the existing GitHub-hosted PAPER experiment.
Validation and read-only preflight workflows remain separate from the single
bounded cloud trading workflow. Preflight uses repository Secrets only to read
the bound PAPER account and private-state repository metadata. Trading requires
an explicit enable flag and verified private cutover proof. Publishing code does
not bypass those gates; absent or false enable flags skip its job.

The exclusive main-account cloud cutover was verified on 2026-10-15. The former
local trading worker remains stopped and its logon task disabled. A running
workflow retains its checked-out code. Reviewed updates are loaded by the next
naturally scheduled workflow; this preparation does not dispatch or rerun one,
create another schedule, or restart a local worker.

The authorized PAPER experiment has a USD100,000 total capital ceiling and the
existing latched 10% observed-peak drawdown rule. Allocation is USD50,000 swing
stocks, USD49,000 intraday stocks and USD1,000 for the user-authorized options
pilot. There is no live trading, funding transfer, leverage or short selling.
Profitability has not been established for any sleeve.

## Limited indicative-options pilot

The first phase buys standard 100-share calls only, selected from existing fresh
bullish intraday stock signals, with 7–21 calendar days to expiry and strikes
within 1% of the underlying quote. Each entry is one contract with at most
USD240 premium and a USD10 fee allowance; at most two contracts may be held or
pending simultaneously. The lifetime allowance is four contracts, USD960 total
premium and USD40 fee contingency within the USD1,000 allocation. Sale proceeds
are parked and cannot fund another entry or replenish this initial allowance.
Ambiguous orders and conservative historical reservations cannot be silently
released or blindly resubmitted.

The user accepted free indicative option quotes: these are derivative reference
prices, not executable OPRA quotes. Their freshness and spread are checked, but
PAPER fills do not establish real liquidity, queue priority or achievable
returns. The pilot is unvalidated; a stock signal is not evidence that its option
trade will be profitable. Full premium is at risk. Scheduled exits, observed
loss/profit triggers and do-not-exercise acknowledgements do not guarantee
execution or uninterrupted protection. No paid data subscription is purchased.
Normal holdings are scheduled to close twenty minutes before the session closes.
Alpaca accepts do-not-exercise requests only on expiration day; a single persisted
request is a fallback for abnormal expiry-day inventory, followed by an exit.
An unexpected exercise or expiry event pauses new entries for manual reconciliation;
resulting shares are never adopted automatically. Unavailable exits or extended
outages can create residual exercise exposure, so the premium budget is not a
guarantee of total economic loss under every operational failure.

## Provenance

`SOURCE-MANIFEST.json` lists every source copy and its SHA-256. The dashboard
builder copy replaces a machine-specific privacy-deny pattern with a generic
Windows user-directory pattern. No runtime, orders, FILLs, snapshots, archives,
exports, private paths or credentials are included. Test data are synthetic.

## Current boundary

Private-state checkpoints, archive bundles, scoped GitHub transport and the
opt-in cloud coordinator retain their independently tested persistence barriers.
The local worker does not load this coordinator or resume after cloud delays.
The coordinator refuses missing state, absent cutover proof, a different bound
account, or an uncertain persistence result. Its five-hour duration is checked
between natural cycles; it is not a hard real-time deadline.

The snapshot and offline-baseline tools preserve private evidence without
contacting the broker. An offline-validation branch is an acceptance artifact,
never permission to start trading. Full-size actual-account restoration,
remote write/readback, broker reconciliation and exclusive cutover remain
deployment checks, not conclusions drawn from code publication. Existing stock
broker stops do not replace time exits: a running
coordinator can retain confirmed-core exits during an archive-only outage,
but a cold start cannot execute any active exit if the full archive/cutover
evidence cannot be restored. No continuous-service guarantee is made.

The archive cache verifies old file hashes and compares every previous SQLite
row; only repeated decoding of verified content is avoided. Compact restore points
retain original package history and reuse its immutable BLOB bytes. A point stores
final row indexes and binds the complete logical snapshot to an original generation.
Cold recovery checks every original package, deeply validates the full assembled
snapshot and its exact hash, then replays at most 31 subsequent generations.
Intermediate prefix snapshots are not reconstructed again; their original bytes
and versions remain available for forensic replay. No historical package is pruned.
The first upgrade from a legacy chain still requires a full verification.
Storage and memory still scale with history. Synthetic tests and performance figures do not
prove real-account migration, lifetime free capacity or brokerage connectivity.
Do not run a cloud worker alongside an existing worker.

Run the same tests locally with `python -m unittest discover -s tests -v`.
On Linux, set LOCALAPPDATA to an empty temporary directory for dashboard imports.
