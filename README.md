# ZERO OR HERO: PAPER software validation

Independent code-only preparation for a zero-cost GitHub-hosted PAPER experiment.
Installed workflows are manually triggered synthetic offline testing and a
separate read-only connection preflight on standard Ubuntu runners. Preflight
uses repository Secrets only to read the bound PAPER account and private state
repository metadata. Neither workflow executes trading. Cloud trading is not enabled.

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

Private-state checkpoint, archive bundles, scoped GitHub transport and an opt-in
execution adapter are tested using synthetic data. The installed trading runner
does not use these adapters. The source contains no active cloud trading workflow.
Production runner wiring, archive capacity/performance,
brokerage read-only reconciliation and the single-executor cutover remain
deployment work. An archive-only outage must block entries; a core-state outage
can also prevent active exits. Existing broker stops do not replace time exits.
Passing tests does not establish brokerage connectivity or a successful cloud
migration. Do not run a cloud worker alongside an existing worker.

Run the same tests locally with `python -m unittest discover -s tests -v`.
On Linux, set LOCALAPPDATA to an empty temporary directory for dashboard imports.
