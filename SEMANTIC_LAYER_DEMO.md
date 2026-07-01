<!--
Personal learning branch — an in-memory demo semantic-layer connector.
NOT for merge to master. Kept on the `sandbox/semantic-layer-demo` branch.
-->

# Semantic Layer — demo connector (learning sandbox)

A tiny **in-memory** semantic-layer connector ("Demo Sales") for learning how Superset's
Semantic Layer feature works, with **no external system**. Lets you create a semantic-layer
connection + view in the UI and chart it fully offline.

Full write-up (concepts, code walkthrough, gotchas, self-quiz):
`superset-spec/specs/semantic_layer_rnd.md` (separate worktree).

## Files

- `docker/pythonpath_dev/demo_semantic_layer.py` — the connector (the two contract classes +
  in-memory data). This dir is `*`-gitignored, so the file was **force-added** to this branch.
- `docker/pythonpath_dev/superset_config_docker.py` — **NOT committed** (gitignored local override,
  and shared across branches). It must contain the registration hook below.

## Activate

1. Ensure `SEMANTIC_LAYERS` is on (already `True` in `docker/pythonpath_dev/superset_config.py`).
2. Ensure your local `docker/pythonpath_dev/superset_config_docker.py` contains this hook (it
   no-ops harmlessly when the connector file isn't present, e.g. on `master`):

   ```python
   def FLASK_APP_MUTATOR(app):  # noqa: N802
       try:
           import demo_semantic_layer  # noqa: F401  (registers itself on import)
           app.logger.info("Registered demo semantic layer (type='demo').")
       except Exception:  # noqa: BLE001
           app.logger.exception("Failed to register demo semantic layer")
   ```
3. Restart: `docker compose restart superset superset-worker`.
4. Verify the type is registered:
   ```bash
   docker compose exec -T superset python -c "from superset.app import create_app; a=create_app(); \
   a.app_context().push(); from superset.semantic_layers.registry import registry; print(registry)"
   # -> {'demo': <class 'demo_semantic_layer.DemoSemanticLayer'>}
   ```

## Use

- UI: http://localhost:8091 (`admin`/`admin`). Create the **Demo Sales** connection
  (Settings → Database Connections → + → Semantic Layer), add the **Sales** view
  (Datasets → + → Add Semantic View).
- The new-chart datasource picker does NOT list semantic views (frontend gap); chart it via:
  `http://localhost:8091/explore/?datasource_type=semantic_view&datasource_id=<id>`
  (`<id>` = the integer `semantic_views.id`).

## Extend (the round-trip)

Add a metric in **two places** in `demo_semantic_layer.py`, then restart:
- `_METRICS` — the published catalog (so Superset knows it exists).
- `_METRIC_SOURCE` — the compute recipe (source column + pandas agg).

To make it a *real* connector, rewrite `_run` (translate the `SemanticQuery` → native SQL/API +
execute) and the catalog accessors (`get_metrics`/`get_dimensions`); the rest is reusable.

## Caveat

Requires the dev DB on the **master** schema. Switching branches that carry migrations (soft-delete,
versioning) needs a dev-DB rebuild: `docker compose down -v && docker compose up -d`.
