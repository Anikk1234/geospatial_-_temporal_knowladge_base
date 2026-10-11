<div align="center">

#  Geospatial & Temporal Knowledge Base

### Query wildfire history with plain SQL — straight from your browser.

**SparkSQL engine · Flask web console · Parquet knowledge base**

<br>

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/PySpark-SparkSQL-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-Web_Console-000000?style=for-the-badge&logo=flask&logoColor=white)
![Parquet](https://img.shields.io/badge/Apache-Parquet-50ABF1?style=for-the-badge&logo=apache&logoColor=white)
![Status](https://img.shields.io/badge/Mode-Read--Only-5c7052?style=for-the-badge)

<br>

<!-- 📸 Replace with a real screenshot: save it as docs/screenshot.png -->
<img src="docs/screenshot.png" alt="Wildfire Knowledge Base SQL Console" width="90%">

</div>

---

##  What is this?

A capstone project that turns a large, enriched **wildfire dataset** into a fast, queryable **knowledge base**. Weather, fire-danger indices, seasons, and locations are all stored in Parquet and served through a sleek, dark **SQL console** — ask questions like *"Which season burns the most?"* or *"How do fire days differ from non-fire days?"* and get answers in milliseconds.

> **Pipeline:** raw data → Spark enrichment → `enriched.parquet` → Flask + SparkSQL console

---

##  Features

| | |
|---|---|
|  **Live SQL console** | Write SparkSQL in the browser and run with `Ctrl/⌘ + Enter` |
|  **Cached in memory** | Spark session boots once; every query reuses the cached dataset |
|  **Instant dashboard** | Records, locations, date range, fire days, and overall fire rate at a glance |
|  **Schema explorer** | Click any column to insert it into your query |
|  **One-click presets** | Fire rate by season/year, weather comparisons, riskiest locations |
|  **Read-only guard** | Only `SELECT` / `WITH` allowed — write keywords are blocked |
|  **Fire-coded results** | Fire days glow rust-red, safe days pine-green |
|  **Responsive** | Works on desktop and mobile layouts |

---

##  Quick Start

```bash
# 1. Clone
git clone https://github.com/<your-username>/<your-repo>.git
cd <your-repo>

# 2. Install dependencies
pip install flask pyspark pandas pyarrow

# 3. Build the knowledge base first (produces wildfire_kb/enriched.parquet)
python 01_spark_knowledge_base.py

# 4. Launch the console
python run.py
```

Then open **http://localhost:5000** 🎉

---

##  Project Structure

```
.
├── 01_spark_knowledge_base.py   # Stage 1 – build & enrich the Parquet knowledge base
├── run.py                       # Stage 3 – Flask + SparkSQL web console
├── wildfire_kb/
│   └── enriched.parquet         # Generated knowledge base
└── docs/
    └── screenshot.png
```

---

##  Try These Queries

** Fire rate by season**
```sql
SELECT season, COUNT(*) AS total_days, SUM(label) AS fire_days,
       ROUND(100.0 * SUM(label) / COUNT(*), 3) AS fire_rate_pct
FROM wildfire_kb
GROUP BY season
ORDER BY fire_rate_pct DESC
```

** Weather: fire days vs. non-fire days**
```sql
SELECT label,
       ROUND(AVG(tmmx), 2) AS avg_max_temp_k,
       ROUND(AVG(rmin), 2) AS avg_min_humidity,
       ROUND(AVG(vs), 2)   AS avg_wind_speed,
       ROUND(AVG(bi), 2)   AS avg_burning_index,
       ROUND(AVG(erc), 2)  AS avg_energy_release_component
FROM wildfire_kb
GROUP BY label
```

** Riskiest locations (min. 30 days observed)**
```sql
SELECT location_id, COUNT(*) AS days_observed, SUM(label) AS fire_days,
       ROUND(100.0 * SUM(label) / COUNT(*), 2) AS fire_rate_pct
FROM wildfire_kb
GROUP BY location_id
HAVING COUNT(*) >= 30
ORDER BY fire_rate_pct DESC
LIMIT 15
```

---

##  Data Dictionary

| Column | Meaning |
|---|---|
| `location_id` | Unique grid location identifier |
| `datetime` | Observation date |
| `label` | `1` = fire occurred, `0` = no fire |
| `season` | Season of the observation |
| `year` | Calendar year |
| `tmmx` | Maximum temperature (K) |
| `rmin` | Minimum relative humidity (%) |
| `vs` | Wind speed |
| `bi` | Burning Index |
| `erc` | Energy Release Component |

---

##  Windows Troubleshooting

<details>
<summary><b>❌ <code>UnsatisfiedLinkError: NativeIO$Windows.access0</code> / <code>TABLE_OR_VIEW_NOT_FOUND</code></b></summary>

<br>

Spark's native file listing on Windows needs `winutils.exe` + `hadoop.dll`. Without them the Parquet load crashes, the `wildfire_kb` view is never created, and every query fails.

`run.py` already sidesteps this by reading the file with **pandas/pyarrow** and handing it to Spark in memory:

```python
pandas_df = pd.read_parquet(KB_PATH)
kb = spark.createDataFrame(pandas_df)
kb.createOrReplaceTempView("wildfire_kb")
```

Prefer the native route? Install matching `winutils.exe` / `hadoop.dll` into `C:\hadoop\bin`, set `HADOOP_HOME=C:\hadoop`, add `C:\hadoop\bin` to `PATH`, and open a **fresh** terminal.

</details>

<details>
<summary><b>❌ <code>pip install pandas</code> tries to compile from source (MSYS2 Python)</b></summary>

<br>

MSYS2's Python doesn't match PyPI's pre-built Windows wheels. Install the native packages instead from the **MSYS2 UCRT64** terminal:

```bash
pacman -S mingw-w64-ucrt-x86_64-python-pandas mingw-w64-ucrt-x86_64-python-pyarrow
```

Or use a standard python.org Python install, where `pip` gets ready-made wheels.

</details>

---

##  Roadmap

- [ ]  Knowledge-graph panel — entity–relationship ontology with identifiers, attributes, and entities
- [ ]  Map view of high-risk locations
- [ ]  Charts for query results
- [ ]  CSV export

---

##  Contributing

Pull requests and ideas are welcome. Open an issue first to discuss big changes.

##  License

Released under the [MIT License](LICENSE).

---

<div align="center">

**Built with for the capstone project**

*If this helped you, drop a ⭐ on the repo!*

</div>
