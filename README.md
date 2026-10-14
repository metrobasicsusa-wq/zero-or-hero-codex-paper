# ZERO OR HERO: PAPER software validation

Independent code-only preparation for a zero-cost GitHub-hosted PAPER experiment.
The only installed workflow is manually triggered, synthetic offline unit testing
on a standard Ubuntu runner. It has read-only repository permissions, no account
secrets, no trading schedule, and no trading command. Cloud trading is not enabled.

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

Private-state checkpoint and archive package code are offline foundations.
Production authentication, durable pre-order integration, outage-safe exits,
storage capacity validation and the single-executor cutover remain separate
deployment work. Passing tests does not establish brokerage connectivity or a
successful cloud migration. Do not run a cloud worker alongside an existing worker.

Run the same tests locally with `python -m unittest discover -s tests -v`.
On Linux, set LOCALAPPDATA to an empty temporary directory for dashboard imports.
