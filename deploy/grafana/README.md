# Grafana dashboards

## orcp-overview-dashboard.json

Minimum end-to-end dashboard covering the pieces the ORCP pipeline owns.
Panels (7 total, two-column layout):

1. Ingest throughput: received / persisted / duplicate per second.
2. Ingest parse-error rate per minute.
3. Flink job state (stat): current uptime by job name.
4. Flink restart count over the last 5 minutes.
5. Last checkpoint duration.
6. JVM heap used, grouped by Spring instance.
7. Ingest processing time P95.

### Import

1. Grafana UI: Dashboards → Import → paste `orcp-overview-dashboard.json`.
2. Select your Prometheus datasource when prompted.
3. Save.

### What's deliberately missing

- Per-operator backpressure: use the upstream Flink dashboard (Grafana ID
  14911) for that. This dashboard is meant to be the single pane of glass
  for *our* application-level signals, not a Flink internals replacement.
- OceanBase-side metrics: OB exposes its own Prometheus endpoint; that
  belongs on a dedicated OB dashboard rather than mixed into app view.
- Alert overlays: Alertmanager's own UI at `:9093/#/alerts` is the source
  of truth; Grafana panels reflect the raw metric values, not alert state.
