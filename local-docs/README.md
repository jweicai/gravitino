# Local Docs

This directory is reserved for team-specific documentation created on top of the upstream
Apache Gravitino repository.

Rules:

- Keep official upstream documentation under `docs/` unchanged unless the task is explicitly to
  modify upstream docs.
- Put local research notes, implementation plans, rollout records, and internal operating guides
  in `local-docs/`.
- Prefer English for new documents unless there is a clear reason to keep a document in another
  language.

Current local documents:

- `Gravitino 调研报告.md`
- `gravitino-iceberg-spark-guide.html` - validated deployment and connectivity guide
- `gravitino-migration-plan.md` - DLF to Gravitino migration notes and scripts
- `gravitino-singapore-production-readiness.md` - production rollout and validation checklist
- `Gravitino + Iceberg + Spark SQL 集成指南.docx` - document copy; verify against the HTML guide before use
