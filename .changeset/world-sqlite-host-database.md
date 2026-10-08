---
"@workflow/world-sqlite": minor
---

Support host-owned SQLite connections and synchronous transactional write hooks, and export the world's table names.

Add an optional validated `tablePrefix` to namespace all world tables and indexes in shared databases, with `worldTables(prefix)` exposing their names.
