import os
import re
import time

from flask import Flask, jsonify, request, render_template_string
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------
# INLINE ASSETS (HTML, CSS, JS)
# --------------------------------------------------------------------------


# Set Hadoop Home programmatically
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"
CSS_CONTENT = """
:root {
  --void:        #0d1117;
  --panel:       #161b22;
  --panel-raise: #21262d;
  --hairline:    #30363d;
  --text:        #c9d1d9;
  --text-muted:  #8b949e;
  --text-dim:    #484f58;
  --ember:       #58a6ff;
  --ember-hi:    #79c0ff;
  --amber:       #d2a8ff;
  --pine:        #2ea043;
  --rust:        #f85149;
  --font-display: 'Poppins', system-ui, sans-serif;
  --font-body:    'Roboto', system-ui, sans-serif;
  --font-mono:    'Fira Code', ui-monospace, monospace;
}

* { box-sizing: border-box; }

html, body {
  margin: 0; padding: 0;
  background: var(--void);
  color: var(--text);
  font-family: var(--font-body);
  min-height: 100vh;
}

@media (prefers-reduced-motion: no-preference) {
  .dot--live { animation: pulse 2.4s ease-in-out infinite; }
}

@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: .35; }
}

.grain {
  position: fixed; inset: 0; pointer-events: none; z-index: 0; opacity: .035;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='120' height='120'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='2' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E");
}

.topbar {
  position: relative; z-index: 1; display: flex; align-items: center; justify-content: space-between;
  padding: 22px 32px; border-bottom: 1px solid var(--hairline);
}
.topbar__title { display: flex; align-items: center; gap: 14px; }
.topbar__mark { color: var(--ember); font-size: 22px; line-height: 1; }
.topbar h1 { font-family: var(--font-display); font-weight: 600; font-size: 19px; letter-spacing: 0.01em; margin: 0; }
.topbar__subtitle { margin: 2px 0 0; font-family: var(--font-mono); font-size: 12px; color: var(--text-muted); letter-spacing: 0.02em; }
.topbar__station { font-family: var(--font-mono); font-size: 12px; color: var(--text-muted); display: flex; align-items: center; gap: 8px; }
.dot { width: 7px; height: 7px; border-radius: 50%; display: inline-block; }
.dot--live { background: var(--pine); box-shadow: 0 0 8px var(--pine); }

.readout { position: relative; z-index: 1; display: flex; align-items: stretch; padding: 20px 32px; gap: 0; overflow-x: auto; }
.readout__item { display: flex; flex-direction: column; gap: 6px; padding-right: 28px; white-space: nowrap; }
.readout__label { font-family: var(--font-mono); font-size: 10.5px; text-transform: uppercase; letter-spacing: 0.09em; color: var(--text-dim); }
.readout__value { font-family: var(--font-display); font-size: 26px; font-weight: 600; font-variant-numeric: tabular-nums; }
.readout__value--sm { font-size: 15px; font-weight: 500; padding-top: 4px; }
.readout__value--ember { color: var(--ember-hi); }
.readout__divider { width: 1px; background: var(--hairline); margin: 0 28px 0 0; }

.console { position: relative; z-index: 1; display: grid; grid-template-columns: 1fr 260px; grid-template-rows: auto auto auto; gap: 20px; padding: 4px 32px 40px; }
.panel--editor  { grid-column: 1; grid-row: 1; }
.panel--schema  { grid-column: 2; grid-row: 1 / 3; }
.panel--results { grid-column: 1; grid-row: 2; }
.panel--kg      { grid-column: 1 / 3; grid-row: 3; }
.panel { background: var(--panel); border: 1px solid var(--hairline); border-radius: 10px; padding: 18px 20px 20px; }
.panel__head { display: flex; align-items: baseline; justify-content: space-between; margin-bottom: 14px; }
.panel__head h2 { font-family: var(--font-display); font-size: 14px; font-weight: 600; letter-spacing: 0.02em; margin: 0; color: var(--text); }
.panel__hint { font-family: var(--font-mono); font-size: 11.5px; color: var(--text-dim); }

.examples { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 14px; }
.chip { font-family: var(--font-mono); font-size: 12px; color: var(--text-muted); background: var(--panel-raise); border: 1px solid var(--hairline); border-radius: 20px; padding: 6px 13px; cursor: pointer; transition: border-color .15s ease, color .15s ease; }
.chip:hover { border-color: var(--ember); color: var(--text); }
.chip:focus-visible { outline: 2px solid var(--ember-hi); outline-offset: 2px; }

#sql-input { width: 100%; min-height: 150px; resize: vertical; background: var(--void); border: 1px solid var(--hairline); border-radius: 8px; color: var(--text); font-family: var(--font-mono); font-size: 13.5px; line-height: 1.6; padding: 14px 16px; }
#sql-input:focus-visible { outline: none; border-color: var(--ember); }
.panel__actions { display: flex; align-items: center; gap: 10px; margin-top: 14px; }
.btn { font-family: var(--font-body); font-size: 13.5px; font-weight: 600; border-radius: 7px; padding: 9px 16px; border: 1px solid transparent; cursor: pointer; display: inline-flex; align-items: center; gap: 8px; transition: transform .1s ease, filter .15s ease; }
.btn:active { transform: translateY(1px); }
.btn:focus-visible { outline: 2px solid var(--ember-hi); outline-offset: 2px; }
.btn--primary { background: var(--ember); color: #0d1117; }
.btn--primary:hover { filter: brightness(1.1); }
.btn__icon { font-size: 10px; }
.btn--ghost { background: transparent; color: var(--text-muted); border-color: var(--hairline); }
.btn--ghost:hover { color: var(--text); border-color: var(--text-dim); }
.run-status { font-family: var(--font-mono); font-size: 12px; color: var(--text-dim); }

.schema-list { display: flex; flex-direction: column; gap: 2px; max-height: 480px; overflow-y: auto; }
.schema-row { display: flex; justify-content: space-between; align-items: center; background: transparent; border: none; border-radius: 6px; padding: 7px 9px; cursor: pointer; text-align: left; }
.schema-row:hover { background: var(--panel-raise); }
.schema-row:focus-visible { outline: 2px solid var(--ember-hi); outline-offset: -2px; }
.schema-row__name { font-family: var(--font-mono); font-size: 12.5px; color: var(--text); }
.schema-row__type { font-family: var(--font-mono); font-size: 10.5px; color: var(--text-dim); }

.error-banner { background: rgba(248, 81, 73, 0.14); border: 1px solid var(--rust); color: #ff7b72; font-family: var(--font-mono); font-size: 12.5px; border-radius: 7px; padding: 12px 14px; margin-bottom: 14px; white-space: pre-wrap; }
.empty-state { color: var(--text-dim); font-family: var(--font-body); font-size: 13.5px; padding: 30px 4px; text-align: center; }
.table-wrap { overflow: auto; max-height: 520px; border: 1px solid var(--hairline); border-radius: 8px; }
table.results-table { border-collapse: collapse; width: 100%; font-family: var(--font-mono); font-size: 12.5px; }
.results-table thead th { position: sticky; top: 0; background: var(--panel-raise); color: var(--amber); text-align: left; padding: 9px 14px; font-weight: 600; border-bottom: 1px solid var(--hairline); white-space: nowrap; }
.results-table tbody td { padding: 7px 14px; border-bottom: 1px solid rgba(48, 54, 61, 0.5); white-space: nowrap; color: var(--text); }
.results-table tbody tr:hover td { background: rgba(88, 166, 255, 0.1); }
.cell-fire { color: var(--rust); font-weight: 600; }
.cell-nofire { color: var(--pine); }

.kg-wrap { overflow: auto; border: 1px solid var(--hairline); border-radius: 8px; background: var(--void); }
.kg-svg { display: block; width: 100%; height: auto; }
.kg-node rect { fill: var(--panel-raise); stroke: var(--hairline); stroke-width: 1; }
.kg-node--Observation rect { stroke: var(--ember); }
.kg-node__title { font-family: var(--font-display); font-weight: 600; font-size: 13px; fill: var(--ember-hi); }
.kg-node__id { font-family: var(--font-mono); font-size: 10.5px; fill: var(--amber); }
.kg-node__attr { font-family: var(--font-mono); font-size: 10px; fill: var(--text-muted); }
.kg-edge { stroke: var(--text-dim); stroke-width: 1.3; fill: none; }
.kg-edge__label { font-family: var(--font-mono); font-size: 10px; fill: var(--text-dim); }
.kg-legend { display: flex; gap: 18px; flex-wrap: wrap; margin-top: 10px; font-family: var(--font-mono); font-size: 11px; color: var(--text-dim); }
.kg-legend__swatch { display: inline-block; width: 9px; height: 9px; border-radius: 2px; background: var(--panel-raise); border: 1px solid var(--ember); margin-right: 6px; vertical-align: middle; }
.kg-legend__swatch--hit { border-color: var(--rust); }

.kg-tabs { display: flex; gap: 6px; }
.kg-tab { font-family: var(--font-mono); font-size: 11.5px; color: var(--text-muted); background: transparent; border: 1px solid var(--hairline); border-radius: 20px; padding: 5px 12px; cursor: pointer; transition: border-color .15s ease, color .15s ease, background .15s ease; }
.kg-tab:hover { color: var(--text); border-color: var(--text-dim); }
.kg-tab--active { color: #0d1117; background: var(--ember); border-color: var(--ember); font-weight: 600; }
.kg-tab:focus-visible { outline: 2px solid var(--ember-hi); outline-offset: 2px; }
.kg-subhint { font-family: var(--font-mono); font-size: 11.5px; color: var(--text-dim); margin-bottom: 10px; }
.kg-node--matched rect { stroke: var(--rust); stroke-width: 1.6; }
.kg-node__attr--hit { fill: var(--rust); font-weight: 600; }
.kg-node__id--hit { fill: var(--rust); }

@media (max-width: 880px) {
  .console { grid-template-columns: 1fr; }
  .panel--editor  { grid-column: 1; grid-row: 1; }
  .panel--schema  { grid-column: 1; grid-row: 2; }
  .panel--results { grid-column: 1; grid-row: 3; }
  .panel--kg      { grid-column: 1; grid-row: 4; }
  .readout { flex-wrap: wrap; row-gap: 14px; }
  .topbar { flex-direction: column; align-items: flex-start; gap: 8px; }
}
"""

JS_CONTENT = """
const sqlInput = document.getElementById("sql-input");
const runBtn = document.getElementById("run-btn");
const clearBtn = document.getElementById("clear-btn");
const runStatus = document.getElementById("run-status");
const tableWrap = document.getElementById("table-wrap");
const errorBanner = document.getElementById("error-banner");
const resultsMeta = document.getElementById("results-meta");

document.querySelectorAll(".chip").forEach((chip) => {
  chip.addEventListener("click", () => {
    sqlInput.value = chip.dataset.sql;
    sqlInput.focus();
  });
});

document.querySelectorAll(".schema-row").forEach((row) => {
  row.addEventListener("click", () => {
    const name = row.dataset.colname;
    const start = sqlInput.selectionStart ?? sqlInput.value.length;
    const end = sqlInput.selectionEnd ?? sqlInput.value.length;
    sqlInput.value = sqlInput.value.slice(0, start) + name + sqlInput.value.slice(end);
    sqlInput.focus();
    const cursor = start + name.length;
    sqlInput.setSelectionRange(cursor, cursor);
  });
});

clearBtn.addEventListener("click", () => {
  sqlInput.value = "";
  sqlInput.focus();
});

sqlInput.addEventListener("keydown", (e) => {
  const isRunShortcut = (e.metaKey || e.ctrlKey) && e.key === "Enter";
  if (isRunShortcut) {
    e.preventDefault();
    runQuery();
  }
});

runBtn.addEventListener("click", runQuery);

function escapeHtml(value) {
  if (value === null || value === undefined) return "";
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function renderTable(columns, rows) {
  if (rows.length === 0) {
    tableWrap.innerHTML = `<div class="empty-state">Query ran successfully but returned no rows.</div>`;
    return;
  }
  const labelColIdx = columns.indexOf("label");
  const wildfireColIdx = columns.indexOf("Wildfire");

  const thead = `<thead><tr>${columns.map((c) => `<th>${escapeHtml(c)}</th>`).join("")}</tr></thead>`;
  const body = rows
    .map((row) => {
      const cells = columns
        .map((col, idx) => {
          const val = row[col];
          let cls = "";
          if (idx === labelColIdx) {
            cls = Number(val) === 1 ? "cell-fire" : "cell-nofire";
          } else if (idx === wildfireColIdx) {
            cls = val === "Yes" ? "cell-fire" : "cell-nofire";
          }
          return `<td class="${cls}">${escapeHtml(val)}</td>`;
        })
        .join("");
      return `<tr>${cells}</tr>`;
    })
    .join("");
  tableWrap.innerHTML = `<table class="results-table">${thead}<tbody>${body}</tbody></table>`;
}

async function runQuery() {
  const sql = sqlInput.value.trim();
  if (!sql) return;

  runBtn.disabled = true;
  runStatus.textContent = "running…";
  errorBanner.hidden = true;

  try {
    const res = await fetch("/api/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql }),
    });
    const data = await res.json();

    if (!res.ok) {
      errorBanner.textContent = data.error || "Query failed.";
      errorBanner.hidden = false;
      resultsMeta.textContent = "error";
      tableWrap.innerHTML = "";
      return;
    }

    renderTable(data.columns, data.rows);
    const truncNote = data.truncated ? ` (capped at ${data.row_count})` : "";
    resultsMeta.textContent = `${data.row_count} row${data.row_count === 1 ? "" : "s"}${truncNote} · ${data.elapsed_ms} ms`;
    runStatus.textContent = "";
    fetchQueryGraph(sql);
  } catch (err) {
    errorBanner.textContent = "Could not reach the server: " + err.message;
    errorBanner.hidden = false;
  } finally {
    runBtn.disabled = false;
    if (runStatus.textContent === "running…") runStatus.textContent = "";
  }
}

// --------------------------------------------------------------------------
// Knowledge graph panel: renders the entity/attribute/relationship ontology
// (schema-level, not row data) as an SVG graph.
// --------------------------------------------------------------------------
const SVG_NS = "http://www.w3.org/2000/svg";

function svgEl(tag, attrs) {
  const el = document.createElementNS(SVG_NS, tag);
  for (const k in attrs) el.setAttribute(k, attrs[k]);
  return el;
}

function renderOntology(ontology, opts) {
  const wrap = document.getElementById("kg-wrap");
  if (!wrap) return;
  const entities = (ontology && ontology.entities) || [];
  const relationships = (ontology && ontology.relationships) || [];
  const matched = (opts && opts.matched) || [];
  const emptyMessage = (opts && opts.emptyMessage) ||
    '<div class="empty-state">No ontology available &mdash; knowledge base did not load.</div>';

  if (entities.length === 0) {
    wrap.innerHTML = emptyMessage;
    return;
  }

  const BOX_W = 190;
  const LINE_H = 15;
  const boxHeight = (e) => 40 + (e.attributes.length * LINE_H) + 12;

  const cx = 480, cy = 230, radius = Math.max(230, entities.length * 55);
  const startAngle = -Math.PI / 2;
  const positions = {};
  entities.forEach((e, i) => {
    const angle = startAngle + (i * 2 * Math.PI) / Math.max(entities.length, 1);
    const bh = boxHeight(e);
    positions[e.id] = {
      x: cx + radius * Math.cos(angle) - BOX_W / 2,
      y: cy + radius * 0.62 * Math.sin(angle) - bh / 2,
      w: BOX_W,
      h: bh,
    };
  });

  let maxX = 0, maxY = 0, minX = 0, minY = 0;
  Object.values(positions).forEach((p) => {
    minX = Math.min(minX, p.x); minY = Math.min(minY, p.y);
    maxX = Math.max(maxX, p.x + p.w); maxY = Math.max(maxY, p.y + p.h);
  });
  const pad = 30;
  const viewW = (maxX - minX) + pad * 2;
  const viewH = (maxY - minY) + pad * 2;
  const offX = pad - minX, offY = pad - minY;

  const svg = svgEl("svg", {
    class: "kg-svg",
    viewBox: "0 0 " + viewW + " " + viewH,
  });

  const defs = svgEl("defs", {});
  const marker = svgEl("marker", {
    id: "kg-arrow", viewBox: "0 0 10 10", refX: "9", refY: "5",
    markerWidth: "7", markerHeight: "7", orient: "auto-start-reverse",
  });
  const arrowPath = svgEl("path", { d: "M0,0 L10,5 L0,10 z", fill: "var(--text-dim)" });
  marker.appendChild(arrowPath);
  defs.appendChild(marker);
  svg.appendChild(defs);

  function boxCenter(id) {
    const p = positions[id];
    return { x: p.x + offX + p.w / 2, y: p.y + offY + p.h / 2 };
  }

  function edgePoint(id, towardId) {
    const p = positions[id];
    const c = boxCenter(id);
    const t = boxCenter(towardId);
    const dx = t.x - c.x, dy = t.y - c.y;
    const halfW = p.w / 2, halfH = p.h / 2;
    const scaleX = dx !== 0 ? halfW / Math.abs(dx) : Infinity;
    const scaleY = dy !== 0 ? halfH / Math.abs(dy) : Infinity;
    const scale = Math.min(scaleX, scaleY);
    return { x: c.x + dx * scale, y: c.y + dy * scale };
  }

  relationships.forEach((r) => {
    if (!positions[r.source] || !positions[r.target]) return;
    const p1 = edgePoint(r.source, r.target);
    const p2 = edgePoint(r.target, r.source);
    const line = svgEl("line", {
      x1: p1.x, y1: p1.y, x2: p2.x, y2: p2.y,
      class: "kg-edge", "marker-end": "url(#kg-arrow)",
    });
    svg.appendChild(line);

    const mx = (p1.x + p2.x) / 2, my = (p1.y + p2.y) / 2;
    const labelBg = svgEl("rect", {
      x: mx - (r.label.length * 3.1), y: my - 8, width: r.label.length * 6.2, height: 14,
      fill: "var(--void)",
    });
    svg.appendChild(labelBg);
    const label = svgEl("text", { x: mx, y: my + 3, class: "kg-edge__label", "text-anchor": "middle" });
    label.textContent = r.label;
    svg.appendChild(label);
  });

  entities.forEach((e) => {
    const p = positions[e.id];
    const x = p.x + offX, y = p.y + offY;
    const entityMatched = e.identifier && matched.includes(e.identifier) ||
      e.attributes.some((a) => matched.includes(a));
    const nodeClass = "kg-node kg-node--" + e.id + (entityMatched ? " kg-node--matched" : "");
    const g = svgEl("g", { class: nodeClass });

    g.appendChild(svgEl("rect", { x, y, width: p.w, height: p.h, rx: 8 }));

    const title = svgEl("text", { x: x + 12, y: y + 20, class: "kg-node__title" });
    title.textContent = e.id;
    g.appendChild(title);

    let lineY = y + 38;
    if (e.identifier) {
      const idHit = matched.includes(e.identifier);
      const idText = svgEl("text", { x: x + 12, y: lineY, class: "kg-node__id" + (idHit ? " kg-node__id--hit" : "") });
      idText.textContent = "id: " + e.identifier;
      g.appendChild(idText);
      lineY += LINE_H;
    }
    e.attributes.forEach((attr) => {
      const attrHit = matched.includes(attr);
      const attrText = svgEl("text", { x: x + 12, y: lineY, class: "kg-node__attr" + (attrHit ? " kg-node__attr--hit" : "") });
      attrText.textContent = attr;
      g.appendChild(attrText);
      lineY += LINE_H;
    });

    svg.appendChild(g);
  });

  wrap.innerHTML = "";
  wrap.appendChild(svg);
}

// --------------------------------------------------------------------------
// KG tabs: "Full schema" (static ontology) vs "This query" (dynamic
// subgraph built from whatever SQL is currently in the editor / was last
// run). Both share the renderOntology() renderer above.
// --------------------------------------------------------------------------
let FULL_ONTOLOGY = { entities: [], relationships: [] };
let QUERY_GRAPH = null;       // last dynamic graph fetched for a query
let QUERY_GRAPH_SQL = null;   // sql it was built from
let kgMode = "full";

const kgSubhint = document.getElementById("kg-subhint");

function paintKg() {
  if (kgMode === "full") {
    if (kgSubhint) kgSubhint.textContent = "Entity types and relationships across the whole schema.";
    renderOntology(FULL_ONTOLOGY, {});
  } else {
    if (!QUERY_GRAPH) {
      if (kgSubhint) kgSubhint.textContent = "Run a query to see the slice of the graph it touches.";
      renderOntology({ entities: [], relationships: [] }, {
        emptyMessage: '<div class="empty-state">Run a query to generate its knowledge graph.</div>',
      });
      return;
    }
    const cols = QUERY_GRAPH.matched_columns || [];
    if (kgSubhint) {
      kgSubhint.textContent = cols.length
        ? "Subgraph touched by the last run query \u2014 highlighted fields were referenced directly."
        : "Query didn't reference specific columns, so the full schema is shown.";
    }
    renderOntology(QUERY_GRAPH, {
      matched: cols,
      emptyMessage: '<div class="empty-state">This query does not touch any known entity types.</div>',
    });
  }
}

document.querySelectorAll(".kg-tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".kg-tab").forEach((t) => t.classList.remove("kg-tab--active"));
    tab.classList.add("kg-tab--active");
    kgMode = tab.dataset.tab;
    paintKg();
  });
});

async function fetchQueryGraph(sql) {
  try {
    const res = await fetch("/api/query_graph", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql }),
    });
    const data = await res.json();
    if (!res.ok) return;
    QUERY_GRAPH = data;
    QUERY_GRAPH_SQL = sql;
    // Auto-switch to the query tab so the dynamic graph is what the user sees.
    const queryTab = document.querySelector('.kg-tab[data-tab="query"]');
    if (queryTab) {
      document.querySelectorAll(".kg-tab").forEach((t) => t.classList.remove("kg-tab--active"));
      queryTab.classList.add("kg-tab--active");
      kgMode = "query";
    }
    paintKg();
  } catch (err) {
    // Non-fatal: the query itself already ran/rendered; the graph is a bonus view.
  }
}

(function initOntology() {
  const dataEl = document.getElementById("ontology-data");
  if (!dataEl) return;
  try {
    FULL_ONTOLOGY = JSON.parse(dataEl.textContent);
    paintKg();
  } catch (err) {
    const wrap = document.getElementById("kg-wrap");
    if (wrap) wrap.innerHTML = '<div class="empty-state">Could not render ontology: ' + err.message + '</div>';
  }
})();
"""

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Wildfire Knowledge Base — SQL Console</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;600;700&family=Roboto:wght@400;500;600&family=Fira+Code:wght@400;500;600&display=swap" rel="stylesheet">
<style>{{ css | safe }}</style>
</head>
<body>

<div class="grain"></div>

<header class="topbar">
  <div class="topbar__title">
    <span class="topbar__mark">&#9673;</span>
    <div>
      <h1>Wildfire Knowledge Base</h1>
      <p class="topbar__subtitle">SparkSQL console &mdash; read-only</p>
    </div>
  </div>
  <div class="topbar__station">station status: <span class="dot dot--live"></span> connected</div>
</header>

<section class="readout" aria-label="Knowledge base summary">
  <div class="readout__item">
    <span class="readout__label">Records</span>
    <span class="readout__value" id="stat-rows">{{ "{:,}".format(status.total_rows) }}</span>
  </div>
  <div class="readout__divider"></div>
  <div class="readout__item">
    <span class="readout__label">Locations</span>
    <span class="readout__value">{{ "{:,}".format(status.total_locations) }}</span>
  </div>
  <div class="readout__divider"></div>
  <div class="readout__item">
    <span class="readout__label">Date range</span>
    <span class="readout__value readout__value--sm">{{ status.min_date }} &rarr; {{ status.max_date }}</span>
  </div>
  <div class="readout__divider"></div>
  <div class="readout__item">
    <span class="readout__label">Fire days logged</span>
    <span class="readout__value">{{ "{:,}".format(status.total_fire_days) }}</span>
  </div>
  <div class="readout__divider"></div>
  <div class="readout__item readout__item--ember">
    <span class="readout__label">Overall fire rate</span>
    <span class="readout__value readout__value--ember">{{ status.fire_rate_pct }}%</span>
  </div>
</section>

<main class="console">
  <div class="panel panel--editor">
    <div class="panel__head">
      <h2>Query</h2>
      <div class="panel__hint">⌘ / Ctrl + Enter to run</div>
    </div>
    <div class="examples" id="examples">
      {% for ex in examples %}
      <button class="chip" data-sql="{{ ex.sql }}">{{ ex.label }}</button>
      {% endfor %}
    </div>
    <textarea id="sql-input" spellcheck="false" placeholder="SELECT * FROM wildfire_kb LIMIT 50">{{ examples[0].sql }}</textarea>
    <div class="panel__actions">
      <button id="run-btn" class="btn btn--primary">
        <span class="btn__icon">&#9654;</span> Run query
      </button>
      <button id="clear-btn" class="btn btn--ghost">Clear</button>
      <span id="run-status" class="run-status"></span>
    </div>
  </div>

  <div class="panel panel--schema">
    <div class="panel__head">
      <h2>Columns</h2>
      <div class="panel__hint">wildfire_kb</div>
    </div>
    <div class="schema-list">
      {% for col in schema %}
      <button class="schema-row" data-colname="{{ col.name }}">
        <span class="schema-row__name">{{ col.name }}</span>
        <span class="schema-row__type">{{ col.type }}</span>
      </button>
      {% endfor %}
    </div>
  </div>

  <div class="panel panel--results">
    <div class="panel__head">
      <h2>Results</h2>
      <div class="panel__hint" id="results-meta">&mdash;</div>
    </div>
    <div id="error-banner" class="error-banner" hidden></div>
    <div class="table-wrap" id="table-wrap">
      <div class="empty-state">
        Run a query to see results here. Try one of the presets above to start.
      </div>
    </div>
  </div>

  <div class="panel panel--kg">
    <div class="panel__head">
      <h2>Knowledge graph</h2>
      <div class="kg-tabs">
        <button class="kg-tab kg-tab--active" data-tab="full">Full schema</button>
        <button class="kg-tab" data-tab="query">This query</button>
      </div>
    </div>
    <div class="kg-subhint" id="kg-subhint">Entity types and relationships across the whole schema.</div>
    <div class="kg-wrap" id="kg-wrap"></div>
    <div class="kg-legend">
      <span><span class="kg-legend__swatch"></span>entity: identifier + attributes</span>
      <span>&#8594; typed relationship between entity types</span>
      <span><span class="kg-legend__swatch kg-legend__swatch--hit"></span>referenced by the query</span>
    </div>
  </div>
</main>

<script id="ontology-data" type="application/json">{{ ontology | tojson }}</script>
<script>{{ js | safe }}</script>
</body>
</html>
"""

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
KB_PATH = os.path.abspath(os.path.join(BASE_DIR, "wildfire_kb", "enriched.parquet"))

MAX_ROWS = 2000  # hard cap on rows returned to the browser

app = Flask(__name__)

# --------------------------------------------------------------------------
# Spark session — created ONCE at startup, reused for every request
# --------------------------------------------------------------------------
print("Starting Spark session (this happens once, at boot) ...")
spark = (
    SparkSession.builder
    .appName("WildfireWebConsole")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "32")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f"Loading knowledge base directly using Spark: {KB_PATH}")

if not os.path.exists(KB_PATH):
    print(f"\n[ERROR] Direct path check failed! Folder does not exist at: {KB_PATH}")
    print("Please verify that 'wildfire_kb' and 'enriched.parquet' sit directly inside C:\\capston\\\n")
    STATUS = {"total_rows": 0, "total_locations": 0, "min_date": "-", "max_date": "-", "total_fire_days": 0, "fire_rate_pct": 0}
    SCHEMA = []
else:
    try:
        # Load Parquet file natively through PySpark (no Pandas layer)
        kb = spark.read.parquet(KB_PATH)
        kb.createOrReplaceTempView("wildfire_kb")
        kb.cache()  # keep in memory across requests

        # Pre-compute status readout once at boot
        _status_row = spark.sql("""
            SELECT
                COUNT(*)                                AS total_rows,
                COUNT(DISTINCT location_id)             AS total_locations,
                MIN(datetime)                           AS min_date,
                MAX(datetime)                           AS max_date,
                SUM(label)                              AS total_fire_days,
                ROUND(100.0 * SUM(label) / COUNT(*), 3) AS fire_rate_pct
            FROM wildfire_kb
        """).collect()[0]

        STATUS = {
            "total_rows": _status_row["total_rows"],
            "total_locations": _status_row["total_locations"],
            "min_date": str(_status_row["min_date"]),
            "max_date": str(_status_row["max_date"]),
            "total_fire_days": _status_row["total_fire_days"],
            "fire_rate_pct": _status_row["fire_rate_pct"],
        }
        print("Knowledge base loaded successfully!")
        print("Status:", STATUS)
        SCHEMA = [{"name": f.name, "type": f.dataType.simpleString()} for f in kb.schema.fields]
    except Exception as e:
        print(f"\n[ERROR] Failed to load Parquet file. Path: {KB_PATH}")
        print(f"Details: {e}\n")
        STATUS = {"total_rows": 0, "total_locations": 0, "min_date": "-", "max_date": "-", "total_fire_days": 0, "fire_rate_pct": 0}
        SCHEMA = []


# --------------------------------------------------------------------------
# Knowledge graph ontology — entity/attribute/relationship model of the
# schema itself (NOT a dump of row-level data; the KB can hold millions of
# rows, which wouldn't render as a readable graph).
# --------------------------------------------------------------------------
def build_ontology(schema):
    """Classify the flat column list into a richer set of entity types using
    name patterns (not a handful of hardcoded literals), then wire up a
    dense web of typed relationships between those entity types — including
    cross-links, not just a single hub-and-spoke star. Falls back to
    'Observation' as a catch-all so no column is ever silently dropped and
    no entity silently disappears (which previously caused Season to lose
    its only relationship whenever the schema had no literal 'datetime'
    column)."""
    col_names = [c["name"] for c in schema]

    KNOWN = {
        "Location":        {"location_id", "latitude", "longitude", "lat", "lon", "lng", "elevation", "elev"},
        "Season":          {"season"},
        "Year":            {"year"},
        "FuelMoisture":    {"fm100", "fm1000"},
        "FireDangerIndex": {"bi", "erc", "fwi", "ffmc", "dmc", "dc", "isi"},
        "WeatherReading":  {"tmmx", "tmmn", "rmax", "rmin", "vs", "pr", "srad", "sph", "th", "vpd"},
        "FireOutcome":     {"label", "wildfire"},
    }
    SUBSTRING_RULES = [
        # (entity_id, substrings to match, case-insensitive)
        ("TemporalFeature", ("roll", "lag", "window", "_avg", "_mean", "_std", "_sum", "_min", "_max", "trend")),
        ("FuelMoisture",    ("fuel", "moist")),
        ("FireDangerIndex", ("danger", "risk_index", "hazard")),
        ("FireOutcome",     ("fire", "ignit")),
        ("Location",        ("location", "elev", "slope", "aspect", "terrain")),
        ("WeatherReading",  ("temp", "humid", "wind", "precip", "radiat")),
        ("Observation",     ("date", "time", "timestamp", "_dt")),
    ]

    buckets = {eid: [] for eid in (
        "Location", "Observation", "Season", "Year", "WeatherReading",
        "FuelMoisture", "FireDangerIndex", "TemporalFeature", "FireOutcome",
    )}

    for col in col_names:
        lower = col.lower()
        placed = False
        for eid, names in KNOWN.items():
            if lower in names:
                buckets[eid].append(col)
                placed = True
                break
        if placed:
            continue
        for eid, substrings in SUBSTRING_RULES:
            if any(s in lower for s in substrings):
                buckets[eid].append(col)
                placed = True
                break
        if not placed:
            buckets["Observation"].append(col)  # catch-all hub, never dropped

    IDENTIFIER_HINTS = {
        "Location": "location_id", "Observation": "datetime", "Season": "season",
        "Year": "year", "FireOutcome": "label",
    }

    def entity(entity_id):
        attrs = buckets[entity_id]
        hint = IDENTIFIER_HINTS.get(entity_id)
        identifier = hint if hint in attrs else (attrs[0] if attrs else None)
        return {
            "id": entity_id,
            "identifier": identifier,
            "attributes": [a for a in attrs if a != identifier],
        }

    entities = [entity(eid) for eid in buckets]
    # Only keep entity types that actually matched a column in this schema.
    entities = [e for e in entities if e["identifier"] or e["attributes"]]
    present_ids = {e["id"] for e in entities}

    # A deliberately dense, non-tree relationship graph: several entities
    # have more than one incoming/outgoing edge, and there are multiple
    # distinct paths into FireOutcome rather than a single chain.
    all_relationships = [
        {"source": "Location",         "target": "Observation",      "label": "RECORDED_AT"},
        {"source": "Observation",      "target": "Season",           "label": "OCCURRED_IN"},
        {"source": "Observation",      "target": "Year",             "label": "OCCURRED_IN"},
        {"source": "Season",           "target": "Year",             "label": "SUBDIVIDES"},
        {"source": "Location",         "target": "Season",           "label": "HAS_CLIMATE_PROFILE"},
        {"source": "Observation",      "target": "WeatherReading",   "label": "MEASURES"},
        {"source": "Observation",      "target": "FuelMoisture",     "label": "MEASURES"},
        {"source": "WeatherReading",   "target": "FuelMoisture",     "label": "DRIVES"},
        {"source": "WeatherReading",   "target": "FireDangerIndex",  "label": "DRIVES"},
        {"source": "FuelMoisture",     "target": "FireDangerIndex",  "label": "DRIVES"},
        {"source": "Observation",      "target": "TemporalFeature",  "label": "AGGREGATED_INTO"},
        {"source": "TemporalFeature",  "target": "FireDangerIndex",  "label": "REFINES"},
        {"source": "FireDangerIndex",  "target": "FireOutcome",      "label": "PREDICTS"},
        {"source": "WeatherReading",   "target": "FireOutcome",      "label": "PREDICTS"},
        {"source": "Location",         "target": "FireOutcome",      "label": "EXPOSED_TO"},
    ]
    relationships = [
        r for r in all_relationships
        if r["source"] in present_ids and r["target"] in present_ids
    ]

    # Flat column -> entity_id lookup, reused to build the per-query dynamic
    # subgraph without re-deriving the classification rules a second time.
    column_entity_map = {}
    for eid, cols in buckets.items():
        for col in cols:
            column_entity_map[col] = eid

    return {"entities": entities, "relationships": relationships}, column_entity_map


def build_query_ontology(sql, schema, full_ontology, column_entity_map):
    """Build a dynamic knowledge-graph subgraph scoped to a single SQL query.

    Rather than re-parsing SQL properly (overkill for a read-only console),
    this scans the query text for whole-word matches against known schema
    column names, maps each matched column to its entity type via
    `column_entity_map`, and returns the slice of the full ontology induced
    by those entity types (entities + only the relationships that run
    between two touched entities). Falls back to the full graph when the
    query doesn't name specific columns (e.g. `SELECT *`, `COUNT(*)`),
    since in that case the query really does touch everything.
    """
    col_names = [c["name"] for c in schema]
    touched_cols = [
        col for col in col_names
        if re.search(r"\b" + re.escape(col) + r"\b", sql, re.IGNORECASE)
    ]

    is_star_select = bool(re.search(r"select\s+\*", sql, re.IGNORECASE))
    full_entities = full_ontology["entities"]

    if is_star_select or not touched_cols:
        touched_entity_ids = {e["id"] for e in full_entities}
    else:
        touched_entity_ids = {
            column_entity_map[col] for col in touched_cols if col in column_entity_map
        }

    entities = []
    for e in full_entities:
        if e["id"] not in touched_entity_ids:
            continue
        entities.append({
            "id": e["id"],
            "identifier": e["identifier"],
            "attributes": list(e["attributes"]),
        })

    present_ids = {e["id"] for e in entities}
    relationships = [
        r for r in full_ontology["relationships"]
        if r["source"] in present_ids and r["target"] in present_ids
    ]

    return {
        "entities": entities,
        "relationships": relationships,
        "matched_columns": touched_cols,
    }


ONTOLOGY, COLUMN_ENTITY_MAP = build_ontology(SCHEMA)


EXAMPLE_QUERIES = [
    {
        "label": "Fire rate by season",
        "sql": (
            "SELECT season, COUNT(*) AS total_days, SUM(label) AS fire_days,\n"
            "       ROUND(100.0 * SUM(label) / COUNT(*), 3) AS fire_rate_pct\n"
            "FROM wildfire_kb\n"
            "GROUP BY season\n"
            "ORDER BY fire_rate_pct DESC"
        ),
    },
    {
        "label": "Fire rate by year",
        "sql": (
            "SELECT year, COUNT(*) AS total_days, SUM(label) AS fire_days,\n"
            "       ROUND(100.0 * SUM(label) / COUNT(*), 3) AS fire_rate_pct\n"
            "FROM wildfire_kb\n"
            "GROUP BY year\n"
            "ORDER BY year"
        ),
    },
    {
        "label": "Weather: fire days vs non-fire days",
        "sql": (
            "SELECT label,\n"
            "       ROUND(AVG(tmmx), 2) AS avg_max_temp_k,\n"
            "       ROUND(AVG(rmin), 2) AS avg_min_humidity,\n"
            "       ROUND(AVG(vs), 2)   AS avg_wind_speed,\n"
            "       ROUND(AVG(bi), 2)   AS avg_burning_index,\n"
            "       ROUND(AVG(erc), 2)  AS avg_energy_release_component\n"
            "FROM wildfire_kb\n"
            "GROUP BY label"
        ),
    },
    {
        "label": "Riskiest locations (min 30 days observed)",
        "sql": (
            "SELECT location_id, COUNT(*) AS days_observed, SUM(label) AS fire_days,\n"
            "       ROUND(100.0 * SUM(label) / COUNT(*), 2) AS fire_rate_pct\n"
            "FROM wildfire_kb\n"
            "GROUP BY location_id\n"
            "HAVING COUNT(*) >= 30\n"
            "ORDER BY fire_rate_pct DESC\n"
            "LIMIT 15"
        ),
    },
    {
        "label": "Browse raw rows",
        "sql": "SELECT * FROM wildfire_kb LIMIT 50",
    },
]

# --------------------------------------------------------------------------
# Query safety guard: read-only statements only
# --------------------------------------------------------------------------
_WRITE_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|MERGE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)

def is_read_only_query(sql: str) -> bool:
    stripped = sql.strip().rstrip(";").strip()
    if not stripped:
        return False
    starts_ok = bool(re.match(r"^\s*(SELECT|WITH)\b", stripped, re.IGNORECASE))
    has_write_keyword = bool(_WRITE_KEYWORDS.search(stripped))
    return starts_ok and not has_write_keyword

# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------
@app.route("/")
def index():
    return render_template_string(
        HTML_TEMPLATE,
        status=STATUS,
        schema=SCHEMA,
        examples=EXAMPLE_QUERIES,
        ontology=ONTOLOGY,
        css=CSS_CONTENT,
        js=JS_CONTENT
    )

@app.route("/api/status")
def api_status():
    return jsonify(STATUS)

@app.route("/api/schema")
def api_schema():
    return jsonify(SCHEMA)

@app.route("/api/ontology")
def api_ontology():
    return jsonify(ONTOLOGY)

@app.route("/api/examples")
def api_examples():
    return jsonify(EXAMPLE_QUERIES)

@app.route("/api/query_graph", methods=["POST"])
def api_query_graph():
    """Return a dynamic knowledge graph scoped to the given SQL query — the
    subset of entities/relationships that query actually touches, plus which
    columns were matched (used by the frontend to highlight them)."""
    body = request.get_json(silent=True) or {}
    sql = (body.get("sql") or "").strip()

    if not sql:
        return jsonify({"error": "No query provided."}), 400

    graph = build_query_ontology(sql, SCHEMA, ONTOLOGY, COLUMN_ENTITY_MAP)
    return jsonify(graph)


@app.route("/api/query", methods=["POST"])
def api_query():
    body = request.get_json(silent=True) or {}
    sql = (body.get("sql") or "").strip()

    if not sql:
        return jsonify({"error": "No query provided."}), 400

    if not is_read_only_query(sql):
        return jsonify({
            "error": "Only read-only SELECT / WITH queries are allowed in this console."
        }), 400

    try:
        start = time.time()
        result_df = spark.sql(sql).limit(MAX_ROWS)
        
        columns = result_df.columns
        collected_rows = result_df.collect()
        
        # Serialize PySpark Row objects to JSON-friendly dictionaries
        rows = []
        for row in collected_rows:
            row_dict = {}
            for col in columns:
                val = row[col]
                if val is None:
                    row_dict[col] = None
                elif hasattr(val, "isoformat"):
                    row_dict[col] = val.isoformat()
                else:
                    row_dict[col] = val
            rows.append(row_dict)

        elapsed_ms = round((time.time() - start) * 1000, 1)

    except Exception as exc:  # Spark raises a wide variety of exception types
        return jsonify({"error": str(exc)}), 400

    return jsonify({
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "truncated": len(rows) == MAX_ROWS,
        "elapsed_ms": elapsed_ms,
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)