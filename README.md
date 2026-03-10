# 🤖 AI-Powered Genie Space Generator

Point this framework at any Unity Catalog schema and it will **automatically create metric views + a Genie Space** — powered by AI. No manual metric definitions needed.

```
You provide:  catalog + schema + business context
You get:      Metric views + Genie Space (ready to ask questions)
Time:         ~60–90 seconds
```

---

## ⚡ Quick Start (3 steps)

### 1️⃣ Edit `config.yaml`

Open [`config.yaml`](./config.yaml) and fill in your details:

```yaml
# Where are your tables?
catalog: my_catalog
schema: my_schema

# What should the Genie Space be called?
genie_space_name: "My Analytics Space"

# Which AI model to use? (don't change unless needed)
llm_model: databricks-claude-opus-4-6

# Tables to skip (pattern matching)
exclude_table_patterns:
  - "%_logs%"
  - "test_%"

# Describe your business (the AI reads this to decide what metrics to create)
business_domain: "What does your business do?"
data_description: "What data lives in these tables?"
stakeholders_and_decisions: "Who uses this data and for what?"
additional_context: "Any other helpful context (optional)"

# What questions does your team ask?
sample_questions:
  - "What is total revenue by month?"
  - "Which products sell the most?"
```

> 💡 **Pro tip:** Use Databricks Assistant on this file and ask: *"For given catalog and schema, analyse all the tables and generate insights to update the config file. Do not update the keys. Only update the config values. Do not change the LLM model."* — It will auto-fill everything for you!

### 2️⃣ Run the notebook

Open **`AI-Powered Genie Space Generator.ipynb`** and run:

| Cell | What it does | When to run |
| --- | --- | --- |
| **Cell 1** | 📝 Instructions | Read once |
| **Cell 2** | 📦 Install packages | First time only |
| **Cell 3** | 🔄 Restart Python | Only after editing `.py` files |
| **Cell 4** | 🚀 **Run Framework** | Every time |

### 3️⃣ Open your Genie Space

The output will print a URL like:
```
✨ GENIE SPACE: My Analytics Space
   URL: https://your-workspace.databricks.net/genie/rooms/abc123
```
Click it → start asking questions in natural language! 🎉

---

## 🏗️ How It Works

```
 config.yaml          ──→  📋 Read Config
                              │
 Unity Catalog        ──→  🔍 Scan Tables & Columns (Step 1)
                              │
 Claude Opus 4        ──→  🤖 Generate Metrics via AI (Step 2)
                              │                              
                           ┌──┴──┬─────────────┐
                           │     │             │
                           ▼     ▼             ▼
                         📊    🔗           📈
                       Measures Joins      Dimensions
                       (3 parallel workers)
                           │     │             │
                           └──┬──┴─────────────┘
                              │
                           ──→ 🧱 Create Metric Views in UC (Step 3)
                              │
                           ──→ ✨ Create Genie Space via API (Step 4)
                              │
                           ──→ 🔗 Output: Genie Space URL
```

**Step 2 detail — AI generates everything in parallel:**

| Phase | What runs | Parallelism |
| --- | --- | --- |
| Phase 1 | Filter relevant tables | Sequential |
| Phase 2 | Dimensions + Measures (3 workers) + Joins | All in parallel |
| Phase 3 | Sample questions + Business instructions | Both in parallel |

The measures step uses **3 parallel AI workers** for speed:
* 🅰️ Simple aggregates + statistical (5 measures)
* 🅱️ Ratios + percentages (10 measures)
* 🅲️ Derived business KPIs (15 measures)

---

## 📁 Project Structure

```
ai-genie-space-generator-GitLab/
├── 📓 AI-Powered Genie Space Generator.ipynb  ← Run this
├── 📝 config.yaml                             ← Edit this
├── 📖 README.md                               ← You are here
└── framework/
    ├── config_handler.py        Parses config.yaml
    ├── metadata_scanner.py      Scans your UC tables, columns & relationships
    ├── llm_orchestrator.py      Calls AI to generate metrics, dimensions, joins
    ├── metric_view_generator.py Creates UC metric views (CREATE MODEL)
    ├── genie_space_creator.py   Creates the Genie Space via REST API
    └── main_orchestrator.py     Ties everything together
```

---

## 📊 What Gets Created

### Metric Views

Created in your schema as `metrics_{table_name}`:

```sql
-- Example: query a generated metric view
SELECT * FROM my_catalog.my_schema.metrics_orders;
```

Each view contains AI-generated **measures** (e.g. `total_revenue`, `avg_order_value`) and **dimensions** (e.g. `order_date`, `region`).

### Genie Space

A ready-to-use natural language interface with:
* ✅ All your source tables + metric views
* ✅ Join definitions between tables
* ✅ Sample questions with SQL
* ✅ Business context as instructions
* ✅ Measure definitions with synonyms

---

## 🔧 Configuration Reference

| Key | Required | Description |
| --- | --- | --- |
| `catalog` | ✅ | Unity Catalog name |
| `schema` | ✅ | Schema containing your tables |
| `genie_space_name` | ❌ | Display name (defaults to schema name) |
| `llm_model` | ❌ | AI model endpoint (default: `databricks-claude-opus-4-6`) |
| `exclude_table_patterns` | ❌ | SQL LIKE patterns for tables to skip |
| `business_domain` | ✅ | What your business does |
| `data_description` | ✅ | What data lives in these tables |
| `stakeholders_and_decisions` | ✅ | Who uses this data and for what decisions |
| `additional_context` | ❌ | Any extra helpful context |
| `sample_questions` | ❌ | Questions your team asks (improves metric quality) |

---

## 🐛 Troubleshooting

| Problem | Fix |
| --- | --- |
| **"No tables found"** | Check `catalog` and `schema` names. Run `SHOW TABLES IN catalog.schema` to verify. |
| **LLM timeout** | Retry — model endpoints can be temporarily busy. |
| **Metric view creation failed** | Check console for invalid SQL formulas. The framework auto-filters most issues. |
| **Genie Space creation failed** | Verify you have permissions to create Genie Spaces. Check if Genie is enabled. |
| **Wrong tables picked** | Add unwanted tables to `exclude_table_patterns` in config.yaml. |
| **Metrics don't match my needs** | Improve `sample_questions` — the AI uses them to decide what to generate. |
| **Changed framework code but no effect** | Run **Cell 3** (Restart Python) before re-running Cell 4. |

---

## 📋 Prerequisites

* ☁️ Databricks workspace with **Unity Catalog** enabled
* 🐍 Runtime **14.3 LTS** or higher
* 🔑 Permissions: `SELECT` on tables, `CREATE MODEL` on schema, Genie Space creation
* 🤖 Access to foundation model endpoint (default: `databricks-claude-opus-4-6`)

---

*Built with Databricks Unity Catalog, Claude Opus 4, and LangChain.*