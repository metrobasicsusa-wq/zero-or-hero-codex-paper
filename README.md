# ZERO OR HERO: PAPER software validation

Independent code-only preparation for a zero-cost GitHub-hosted PAPER experiment.
Installed workflows are manually triggered synthetic offline testing, private
baseline cold-restore acceptance, a separate read-only account reconciliation and connection preflight
on standard Ubuntu runners. Preflight
uses repository Secrets only to read the bound PAPER account and private state
repository metadata. The bounded trading workflow requires an explicit repository
enable flag and verified private cutover proof. Cloud trading is not enabled by
publishing code; absent or false enable flags skip its job.

The experiment is limited to Alpaca PAPER US equities, USD100,000 total capital,
USD50,000 per sleeve and the existing latched 10% observed-peak drawdown rule.
No live account, options, funding, leverage or short-selling deployment is included.
Profitability has not been established.

## Provenance

`SOURCE-MANIFEST.json` lists every source copy and its SHA-256. The dashboard
builder copy replaces a machine-specific privacy-deny pattern with a generic
Windows user-directory pattern. No runtime, orders, FILLs, snapshots, archives,
exports, private paths or credentials are included. Test data are synthetic.

## Current boundary

Private-state checkpoints, archive bundles, scoped GitHub transport and the
opt-in cloud coordinator are tested using synthetic data. The local worker does
not load this coordinator. The installed cloud trading workflow remains disabled
until the separately verified, exclusive account cutover.
The coordinator refuses missing state, absent cutover proof, a different bound
account, or an uncertain persistence result. Its five-hour duration is checked
between natural cycles; it is not a hard real-time deadline.

The snapshot and offline-baseline tools preserve private evidence without
contacting the broker. An offline-validation branch is an acceptance artifact,
never permission to start trading. Full-size actual-account restoration,
remote write/readback, broker reconciliation and exclusive cutover remain
deployment checks. Existing broker stops do not replace time exits: a running
coordinator can retain confirmed-core exits during an archive-only outage,
but a cold start cannot execute any active exit if the full archive/cutover
evidence cannot be restored. No continuous-service guarantee is made.

The archive cache verifies old file hashes and compares every previous SQLite
row; only repeated decoding of verified content is avoided. Cold restores and
memory still scale with history. Synthetic tests and performance figures do not
prove real-account migration, lifetime free capacity or brokerage connectivity.
Do not run a cloud worker alongside an existing worker.

Run the same tests locally with `python -m unittest discover -s tests -v`.
On Linux, set LOCALAPPDATA to an empty temporary directory for dashboard imports.
