# cratedb-wtf backlog

## Iteration +1
- Expose collected data via Glances-like UI
- Experimental UI using Grafana Scenes

## Iteration +2
- Make `cratedb-wtf logs` also optionally consider `sys.` tables. 
- cratedb-wtf explore table|shard|partition|node
- High-level analysis, evaluating a set of threshold rules 
- Network diagnostics?

## Iteration +3
- Make it work with CrateDB Cloud.
  ```
  ctk cluster info
  ctk cluster health
  ctk cluster logs --slow-queries
  ```

## Done
- Make it work
- Proper marshalling of timestamp values (ISO 8601)
- Expose collected data via HTTP API
  ```
  cratedb-wtf serve
  ```
- Provide `scrub` option also via HTTP
- Complete collected queries and code snippets
- Harvest queries from Admin UI, crash, crate-admin-alt
- Harvest queries from experts
  - https://tools.cr8.net/grafana/d/RkpNJx84z/cratedb-jobs-log?orgId=1&refresh=5m&var-datasource=crate-production
  - https://tools.cr8.net/grafana/d/RkpNJx84z/cratedb-jobs-log?orgId=1&refresh=5m&var-datasource=crate-production&viewPanel=44
- Add `description` and `unit` fields to each `InfoElement` definition
