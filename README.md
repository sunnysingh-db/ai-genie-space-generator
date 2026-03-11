# AI-Powered Genie Space Generator

[This](https://gitlab.com/sunny.singh7/ai-genie-space-generator) Repo can be shared outside EMU

Point this tool at any Unity Catalog schema and it will **automatically create a Genie Space** — powered by AI. No manual setup needed.

```
You provide:  catalog name + schema name
You get:      Metric views + Genie Space (ready to ask questions)
Time:         ~2–3 minutes
```

---

## Quick Start

Open **AI-Powered Genie Space Generator.ipynb** and run the cells in order:

| Step | Cell | What it does |
| --- | --- | --- |
| 1 | **Install Packages** | One-time setup |
| 2 | **Auto-Configure** | Scans your schema and generates config.yaml automatically |
| 3 | **Run Framework** | Creates metric views and Genie Space |

That's it. Each step shows a clickable output:

* **After Auto-Configure** — a link to review the generated `config.yaml`
* **After Run Framework** — buttons to open your **Genie Space** and **Metric Views**

---

## End-to-End Flow

```
┌───────────────────────────────────────────────────────────────────────┐
│  AUTO-CONFIGURE  (Cell 4)                                           │
│                                                                     │
│  catalog + schema                                                   │
│       │                                                             │
│       ▼                                                             │
│  ┌────────────────────┐   Queries information_schema for all              │
│  │  Scan Tables &    │   managed tables, their columns, data types,    │
│  │  Sample Data      │   and comments. Samples rows from each table.   │
│  └────────────────────┘                                                │
│       │                                                             │
│       ▼                                                             │
│  ┌────────────────────┐   Sends table metadata + sample data to the AI.   │
│  │  AI Analyses      │   AI identifies what the data is about and       │
│  │  Your Data        │   generates business context, questions, and     │
│  └────────────────────┘   table exclusion patterns automatically.         │
│       │                                                             │
│       ▼                                                             │
│  ┌────────────────────┐   Writes all values into config.yaml. Keys and    │
│  │  Write            │   the AI model name are never changed.           │
│  │  config.yaml      │   Shows a clickable link to review the file.     │
│  └────────────────────┘                                                │
└───────────────────────────────────────────────────────────────────────┘
       │
       ▼  User reviews config.yaml (optional)
       │
┌───────────────────────────────────────────────────────────────────────┐
│  RUN FRAMEWORK  (Cell 5)                                            │
│                                                                     │
│  config.yaml                                                        │
│       │                                                             │
│       ▼                                                             │
│  ┌────────────────────┐   Re-scans your schema using the config.         │
│  │  Step 1:          │   Applies exclusion patterns to skip system      │
│  │  Scan Metadata    │   tables. Samples 100 rows per table.            │
│  └────────────────────┘   Infers relationships between tables.            │
│       │                                                             │
│       ▼                                                             │
│  ┌────────────────────┐   Six AI calls across three phases:               │
│  │  Step 2:          │                                                │
│  │  AI Generates     │    1. Filters to business-relevant tables       │
│  │  Metrics          │    2. Generates dimensions (date, region...)    │
│  └────────────────────┘    3. Generates measures (revenue, counts...)    │
│       │                    4. Generates joins between tables           │
│       │                    5. Generates sample questions with SQL       │
│       │                    6. Generates business instructions           │
│       │                                                             │
│       ▼                                                             │
│  ┌────────────────────┐   Creates one metric view per table in your       │
│  │  Step 3:          │   schema (e.g. metrics_orders, metrics_flights). │
│  │  Create Metric    │   Each view bundles the AI-generated measures    │
│  │  Views            │   and dimensions for that table. Auto-retries    │
│  └────────────────────┘   and removes invalid columns if needed.          │
│       │                                                             │
│       ▼                                                             │
│  ┌────────────────────┐   Creates a Genie Space with all source tables,   │
│  │  Step 4:          │   metric views, joins, sample questions, and     │
│  │  Create Genie     │   business instructions baked in. Shows           │
│  │  Space            │   clickable buttons to open the Genie Space      │
│  └────────────────────┘   and view the metric views.                      │
└───────────────────────────────────────────────────────────────────────┘
       │
       ▼
  Open Genie Space → Ask questions in plain English
```

---

## How It Works — In Detail

### Auto-Configure (Cell 4)

You provide a **catalog** and **schema** name. The tool does the rest:

| Phase | What happens | Details |
| --- | --- | --- |
| **Scan** | Discovers your data | Queries `information_schema` for all managed tables and their columns. Samples a few rows from each table in parallel to understand the actual data values, categories, and date ranges. |
| **Analyse** | AI understands your data | Sends table names, column types, and sample values to the AI. The AI figures out what the data is about, generates a business description, suggests which tables to exclude (logs, tests, system tables), and writes 10 sample business questions. |
| **Write** | Updates config.yaml | Writes all generated values into `config.yaml`. The AI model name and all config keys are never changed — only the values are filled in. A clickable link lets you review the file. |

> You can review and tweak the generated config, or just run the framework directly.

### Run Framework (Cell 5)

The framework reads `config.yaml` and runs a four-step pipeline:

| Step | What happens | Details |
| --- | --- | --- |
| **1. Scan Metadata** | Reads your schema | Re-scans all tables, columns, and sample data — this time applying the exclusion patterns from config to skip non-business tables. Samples 100 rows per table and infers foreign key relationships by matching column names and data overlap across tables. |
| **2. Generate Metrics** | AI creates the analytics layer | Makes six AI calls: (1) picks which tables are relevant to your business, (2) generates **dimensions** like date, region, and category, (3) generates **measures** like total revenue, average order value, and conversion rates using three parallel workers, (4) generates **joins** between related tables, (5) writes **sample questions** with SQL, and (6) writes **business instructions** for the Genie Space. |
| **3. Create Metric Views** | Saves to Unity Catalog | Creates one metric view per table in your schema (e.g., `metrics_orders`, `metrics_flights`). Each view bundles the AI-generated measures and dimensions for that table. If a column reference is invalid, the framework automatically removes it and retries. |
| **4. Create Genie Space** | Builds the Q&A interface | Creates a Genie Space via the Databricks API, pre-loaded with all source tables, metric views, join definitions, sample questions with SQL, and business instructions. Shows clickable buttons to open the Genie Space and browse metric views. |

The three types of measures the AI generates:

| Type | Examples |
| --- | --- |
| **Simple aggregates** | Total revenue, average order value, record counts |
| **Ratios and percentages** | Cancellation rate, occupancy percentage, refund rate |
| **Business KPIs** | Revenue per customer, delivery time vs preparation time, early payment days |

---

## What Gets Created

### Metric Views

Saved in your schema as `metrics_{table_name}`. Each view contains AI-generated **measures** (like total revenue, average order value) and **dimensions** (like date, region, category).

```sql
SELECT * FROM my_catalog.my_schema.metrics_orders;
```

### Genie Space

A ready-to-use conversational interface with:
* All your source tables and metric views
* Join definitions between tables
* Sample questions with SQL
* Business context as instructions

Just open the link and start asking questions in plain English.

---

## Project Structure

```
ai-genie-space-generator/
├── AI-Powered Genie Space Generator.ipynb   <- Run this
├── config.yaml                              <- Auto-generated
├── README.md                                <- You are here
└── framework/
    ├── auto_configurator.py    Scans schema and auto-generates config.yaml
    ├── config_handler.py       Reads and validates config.yaml
    ├── metadata_scanner.py     Scans tables, columns, and relationships
    ├── llm_orchestrator.py     AI-powered metric and dimension generation
    ├── metric_view_generator.py Creates metric views in Unity Catalog
    ├── genie_space_creator.py   Creates the Genie Space via API
    └── main_orchestrator.py     Runs the end-to-end pipeline
```

---

## Configuration

The `config.yaml` file is auto-generated by the Auto-Configure step. You can also edit it manually.

| Field | Auto-filled? | What it controls |
| --- | --- | --- |
| `catalog` | Yes | Which catalog to use |
| `schema` | Yes | Which schema to scan |
| `genie_space_name` | Yes | Name shown on the Genie Space |
| `llm_model` | No (preserved) | AI model used (default: `databricks-claude-opus-4-6`) |
| `exclude_table_patterns` | Yes | Tables to skip (system, test, logs) |
| `business_domain` | Yes | What your business does |
| `data_description` | Yes | What data lives in these tables |
| `stakeholders_and_decisions` | Yes | Who uses this data and for what |
| `additional_context` | Yes | Extra helpful context |
| `sample_questions` | Yes | Questions your team asks |

> The Auto-Configure step fills in everything except `llm_model`, which is never changed.

---

## Troubleshooting

| Problem | What to do |
| --- | --- |
| No tables found | Check your `catalog` and `schema` names are correct |
| AI timeout | Just retry — model endpoints can be temporarily busy |
| Metric view failed | The framework auto-retries and removes invalid columns. Check the output for details. |
| Wrong tables picked | Add unwanted table patterns to `exclude_table_patterns` in config.yaml |
| Metrics don't match needs | Edit `sample_questions` in config.yaml — the AI uses them to decide what to generate |
| Code changes not taking effect | Run the **Restart Python** cell before re-running the framework |

---

## Prerequisites

* Databricks workspace with **Unity Catalog** enabled
* Permissions: `SELECT` on tables, `CREATE MODEL` on schema, Genie Space creation
* Access to a foundation model endpoint (default: `databricks-claude-opus-4-6`)

---

## Author

Built by **Sunny Singh** — sunny.singh@databricks.com

If this saved you time or you found it useful, feel free to reach out and say hi.

*Built with Databricks Unity Catalog, Claude Opus 4, and LangChain.*