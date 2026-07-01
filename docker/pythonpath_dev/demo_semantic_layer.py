# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
A tiny **in-memory demo semantic-layer connector** — for learning only.

It registers a "Demo Sales" semantic layer with a single view ("Sales") backed
by a hardcoded table. No external system, no database. It implements the
``superset_core.semantic_layers`` contract so you can:

  1. Create the "Demo Sales" connection in the UI (Data connections → + → Semantic Layer)
  2. Add the "Sales" view as a datasource (Datasets → + → Add Semantic View)
  3. Build a chart on it (e.g. revenue by region)

The two classes below ARE the contract. ``DemoSemanticLayer`` is the *connection*
(discovers views); ``DemoSemanticView`` is the *queryable model* (exposes metrics
and dimensions, and — the interesting bit — translates a ``SemanticQuery`` into a
result in ``get_table``). Registration happens at the bottom of the file.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pyarrow as pa
from pydantic import BaseModel
from superset_core.semantic_layers.layer import SemanticLayer
from superset_core.semantic_layers.types import (
    AggregationType,
    Dimension,
    Metric,
    SemanticQuery,
    SemanticRequest,
    SemanticResult,
)
from superset_core.semantic_layers.view import SemanticView, SemanticViewFeature

# --------------------------------------------------------------------------- #
# The "warehouse": a small hardcoded sales table.                              #
# In a real connector this lives in Snowflake / dbt / Cube; here it's a frame. #
# --------------------------------------------------------------------------- #
_DATA = pd.DataFrame(
    [
        {"region": r, "product": p, "month": m, "revenue": rev, "orders": o}
        for (r, p, m, rev, o) in [
            ("North", "Widget", "2026-01", 1200.0, 30),
            ("North", "Gadget", "2026-01", 800.0, 16),
            ("South", "Widget", "2026-01", 950.0, 24),
            ("South", "Gadget", "2026-01", 1500.0, 25),
            ("East", "Widget", "2026-01", 600.0, 15),
            ("West", "Gadget", "2026-01", 1100.0, 22),
            ("North", "Widget", "2026-02", 1350.0, 34),
            ("South", "Gadget", "2026-02", 1700.0, 28),
            ("East", "Widget", "2026-02", 720.0, 18),
            ("West", "Widget", "2026-02", 980.0, 20),
            ("North", "Gadget", "2026-03", 900.0, 18),
            ("South", "Widget", "2026-03", 1250.0, 31),
            ("East", "Gadget", "2026-03", 640.0, 13),
            ("West", "Gadget", "2026-03", 1320.0, 27),
        ]
    ]
)

# metric name -> (source column in _DATA, pandas aggregation)
_METRIC_SOURCE = {
    "total_revenue": ("revenue", "sum"),
    "order_count": ("orders", "sum"),
    "avg_revenue": ("revenue", "mean"),  # bonus: an AVG, not a SUM
}

# The metrics & dimensions this view exposes. Note `type` is a *pyarrow* type;
# Superset maps it to its own GenericDataType (NUMERIC/STRING/TEMPORAL/...).
_DIMENSIONS = {
    Dimension(id="region", name="region", type=pa.string()),
    Dimension(id="product", name="product", type=pa.string()),
    Dimension(id="month", name="month", type=pa.string()),
}
_METRICS = {
    Metric(
        id="total_revenue",
        name="total_revenue",
        type=pa.float64(),
        definition="SUM(revenue)",
        aggregation=AggregationType.SUM,
    ),
    Metric(
        id="order_count",
        name="order_count",
        type=pa.int64(),
        definition="SUM(orders)",
        aggregation=AggregationType.SUM,
    ),
    Metric(
        id="avg_revenue",
        name="avg_revenue",
        type=pa.float64(),
        definition="AVG(revenue)",
        aggregation=AggregationType.AVG,
    ),
}


class DemoConfig(BaseModel):
    """The connection config schema (intentionally trivial for the demo)."""

    label: str | None = None


class DemoSemanticView(SemanticView):
    """A single queryable model. This is the heart of the contract."""

    # The mapper reads `.features` to gate group-limit / group-others / adhoc
    # order-by. We advertise none, so those query shapes are rejected — fine for
    # a basic bar/line chart.
    features: set[SemanticViewFeature] = set()

    def __init__(self, name: str = "Sales") -> None:
        # The host reads `.name` on impl views (the /views endpoint lists/sorts
        # by it), so expose it directly — it's part of the *implicit* contract.
        self.name = name

    def uid(self) -> str:
        return f"demo.{self.name}"

    # The mapper uses the `.metrics` / `.dimensions` *properties*; the host model
    # uses the `get_metrics()` / `get_dimensions()` *methods*. Provide both.
    @property
    def metrics(self) -> set[Metric]:
        return set(_METRICS)

    @property
    def dimensions(self) -> set[Dimension]:
        return set(_DIMENSIONS)

    def get_metrics(self) -> set[Metric]:
        return set(_METRICS)

    def get_dimensions(self) -> set[Dimension]:
        return set(_DIMENSIONS)

    def get_compatible_metrics(
        self, selected_metrics: set[Metric], selected_dimensions: set[Dimension]
    ) -> set[Metric]:
        # A real layer might forbid certain combos; the demo allows everything.
        return set(_METRICS)

    def get_compatible_dimensions(
        self, selected_metrics: set[Metric], selected_dimensions: set[Dimension]
    ) -> set[Dimension]:
        return set(_DIMENSIONS)

    def get_values(
        self, dimension: Dimension, filters: Any = None
    ) -> SemanticResult:
        """Distinct values for a dimension (powers filter dropdowns)."""
        values = sorted(_DATA[dimension.name].dropna().unique().tolist())
        table = pa.table({dimension.name: values})
        return SemanticResult(
            requests=[
                SemanticRequest(
                    type="demo",
                    definition=f"SELECT DISTINCT {dimension.name} FROM sales",
                )
            ],
            results=table,
        )

    def get_row_count(self, query: SemanticQuery) -> SemanticResult:
        result = self._run(query)
        table = pa.table({"rowcount": [len(result)]})
        return SemanticResult(
            requests=[SemanticRequest(type="demo", definition="-- row count")],
            results=table,
        )

    def get_table(self, query: SemanticQuery) -> SemanticResult:
        """Translate a SemanticQuery into a result. THIS is the query engine."""
        result = self._run(query)
        table = pa.Table.from_pandas(result, preserve_index=False)
        return SemanticResult(
            requests=[SemanticRequest(type="demo", definition=self._describe(query))],
            results=table,
        )

    # ---- internal helpers --------------------------------------------------- #
    def _run(self, query: SemanticQuery) -> pd.DataFrame:
        frame = self._apply_filters(_DATA.copy(), query)
        dim_names = [d.name for d in query.dimensions]
        metric_names = [m.name for m in query.metrics]

        if dim_names:
            out: pd.DataFrame | None = None
            for name in metric_names:
                src, how = _METRIC_SOURCE.get(name, (None, None))
                if src is None:
                    continue
                part = (
                    frame.groupby(dim_names, as_index=False)[src]
                    .agg(how)
                    .rename(columns={src: name})
                )
                out = part if out is None else out.merge(part, on=dim_names)
            if out is None:  # dimensions only, no metrics
                out = frame[dim_names].drop_duplicates().reset_index(drop=True)
        else:  # no group-by: a single aggregate row
            row = {}
            for name in metric_names:
                src, how = _METRIC_SOURCE.get(name, (None, None))
                if src is not None:
                    row[name] = [getattr(frame[src], how)()]
            out = pd.DataFrame(row)

        out = self._apply_order(out, query)
        limit = getattr(query, "limit", None)
        if limit:
            out = out.head(int(limit))
        return out.reset_index(drop=True)

    @staticmethod
    def _apply_filters(frame: pd.DataFrame, query: SemanticQuery) -> pd.DataFrame:
        """Best-effort demo filtering (EQUALS / IN on dimension columns)."""
        for flt in getattr(query, "filters", None) or []:
            try:
                target = (
                    getattr(flt, "column", None)
                    or getattr(flt, "dimension", None)
                    or getattr(flt, "name", None)
                )
                col = getattr(target, "name", target)
                if col not in frame.columns:
                    continue
                op = getattr(getattr(flt, "operator", None), "name", "")
                val = getattr(flt, "value", None)
                if val is None:
                    val = getattr(flt, "values", None)
                seq = val if isinstance(val, (list, tuple, set)) else [val]
                if op == "EQUALS":
                    frame = frame[frame[col] == val]
                elif op == "NOT_EQUALS":
                    frame = frame[frame[col] != val]
                elif op == "IN":
                    frame = frame[frame[col].isin(list(seq))]
                elif op == "NOT_IN":
                    frame = frame[~frame[col].isin(list(seq))]
                # other operators are ignored in this demo
            except Exception:  # noqa: BLE001  (demo: never break on a filter)
                continue
        return frame

    @staticmethod
    def _apply_order(frame: pd.DataFrame, query: SemanticQuery) -> pd.DataFrame:
        by: list[str] = []
        asc: list[bool] = []
        for item in getattr(query, "order", None) or []:
            try:
                if isinstance(item, (list, tuple)):
                    target, direction = item[0], (item[1] if len(item) > 1 else None)
                else:
                    target = (
                        getattr(item, "target", None)
                        or getattr(item, "metric", None)
                        or getattr(item, "dimension", None)
                        or getattr(item, "column", None)
                    )
                    direction = getattr(item, "direction", None)
                name = getattr(target, "name", target)
                dirname = getattr(direction, "name", str(direction)).upper()
                if name in frame.columns:
                    by.append(name)
                    asc.append("DESC" not in dirname)
            except Exception:  # noqa: BLE001
                continue
        return frame.sort_values(by=by, ascending=asc) if by else frame

    @staticmethod
    def _describe(query: SemanticQuery) -> str:
        dims = ", ".join(d.name for d in query.dimensions) or "(none)"
        mets = ", ".join(m.name for m in query.metrics) or "(none)"
        return f"-- demo query: metrics=[{mets}] grouped by [{dims}]"


class DemoSemanticLayer(SemanticLayer):
    """The *connection*. Discovers and hands out semantic views."""

    configuration_class = DemoConfig

    def __init__(self, configuration: DemoConfig | None = None) -> None:
        # The framework reads `.configuration` off the instance (e.g. the
        # runtime-schema endpoint does `layer.implementation.configuration`), so
        # it must be exposed under exactly that name — a parsed pydantic config.
        self.configuration = configuration or DemoConfig()

    @classmethod
    def from_configuration(cls, configuration: dict[str, Any]) -> "DemoSemanticLayer":
        # Superset hands us the decrypted config dict; parse it into ConfigT.
        return cls(cls.configuration_class.model_validate(configuration or {}))

    @classmethod
    def get_configuration_schema(
        cls, configuration: Any = None
    ) -> dict[str, Any]:
        # JSON Schema → rendered as the connection form by the frontend.
        return {
            "type": "object",
            "properties": {
                "label": {
                    "type": "string",
                    "title": "Label",
                    "description": "Optional label for this demo connection.",
                }
            },
            "required": [],
        }

    @classmethod
    def get_runtime_schema(
        cls, configuration: Any, runtime_data: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        # No per-view runtime config needed to list the demo's single view.
        return {"type": "object", "properties": {}}

    def get_semantic_views(
        self, runtime_configuration: dict[str, Any]
    ) -> set[DemoSemanticView]:
        return {DemoSemanticView("Sales")}

    def get_semantic_view(
        self, name: str, additional_configuration: dict[str, Any]
    ) -> DemoSemanticView:
        return DemoSemanticView(name)


# --------------------------------------------------------------------------- #
# Registration. The `@semantic_layer` decorator is the real mechanism, but it  #
# only exists after app-init injection; for a dev demo we insert directly into #
# the registry. `registry` keys are the `type` stored on each SemanticLayer.   #
# --------------------------------------------------------------------------- #
DemoSemanticLayer.name = "Demo Sales"
DemoSemanticLayer.description = "In-memory demo connector (learning only)."
DemoSemanticLayer._semantic_layer_id = "demo"

from superset.semantic_layers.registry import registry  # noqa: E402

registry["demo"] = DemoSemanticLayer
