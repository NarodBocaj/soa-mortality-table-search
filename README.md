# SOA Mortality Table Search

A local search tool for all ~3,000 public mortality tables hosted by the [Society of Actuaries](https://mort.soa.org). Enter a partial rate vector, gender, and issue age to identify which table(s) your rates come from.

## How it works

The scraper fetches every table from `mort.soa.org`, parses the HTML, and stores the rates in a SQLite database. It then exports the data as a `data.js` file that the search page loads directly — no server required.

Each row in the database represents one **issue age** from one table, storing the full select-period rate vector (q(x,1), q(x,2), …) alongside the table name, gender, risk class, and a direct link back to the SOA website for verification.

## Files

| File | Description |
|---|---|
| `scraper.py` | Fetches and parses all SOA mortality tables into a local SQLite database and exports `data.js` |
| `index.html` | Self-contained search UI — open directly in a browser, no server needed |
| `mortality.db` | SQLite database (~33 MB) with all scraped tables and rates |
| `data.js` | JavaScript export of the database (~12 MB) loaded by `index.html` |
| `data.json` | Same data as `data.js` in plain JSON format |

> **Note:** `data.js`, `data.json`, and `mortality.db` are generated files. If you are cloning this repo fresh you will need to run the scraper to produce them (see below).

## Quickstart

### 1. Install dependencies

Requires Python 3.9+ and two packages:

```bash
pip install requests beautifulsoup4
```

### 2. Run the scraper

```bash
python3 scraper.py --full --workers 25 --delay 0
```

This scans ~3,600 table IDs on `mort.soa.org`, parses the valid ones, and writes `mortality.db`, `data.json`, and `data.js`. Expect it to take **3–5 minutes** with 25 workers.

| Flag | Default | Description |
|---|---|---|
| `--full` | off | Scrape all tables (omit for a 10-table test run) |
| `--workers` | 10 | Number of parallel HTTP workers |
| `--delay` | 0.1 | Per-worker pause between requests (seconds) |
| `--max-id` | 3500 | Upper bound for table ID scan |
| `--limit` | 10 | Max tables to scrape in test mode (ignored with `--full`) |

### 3. Open the search page

```bash
open index.html
```

Or just double-click `index.html`. No local server needed.

## Using the search

The search page filters the full database client-side:

- **Gender** — Male, Female, or Combined/Unisex
- **Issue Age** — the age at policy issue for the row you want to match
- **Rates** — enter as many duration rates as you have, starting at Duration 1 (q(x,1)). Leave later durations blank; only filled-in values are matched.

Matches are found within a tolerance of **±0.000001** per rate. Results are sorted with select-period tables first, then alphabetically by table name. Each result card links directly to the source table on the SOA website.

**Example search:** Gender = Female, Issue Age = 50, Duration 1 = `0.00027`, Duration 2 = `0.00047` → returns 2015 VBT Female Non-Smoker tables.

## Database schema

```sql
tables (
    table_identity  INTEGER PRIMARY KEY,
    name            TEXT,
    content_type    TEXT,
    nation          TEXT,
    gender          TEXT,       -- 'Male', 'Female', or 'Combined'
    risk_class      TEXT,
    select_period   INTEGER,    -- number of duration columns; 1 = aggregate table
    min_age         INTEGER,
    max_age         INTEGER,
    link            TEXT        -- direct URL to SOA viewer
)

rates (
    id              INTEGER PRIMARY KEY,
    table_identity  INTEGER,
    issue_age       INTEGER,
    d1 … d50        REAL        -- NULL for durations beyond the table's select period
)
```

## Data notes

- **Ultimate rates are excluded.** For Select & Ultimate tables only the select portion is stored.
- **Aggregate tables** (no select period) are stored with a single rate in `d1` and `select_period = 1`. They match searches with one rate entered.
- Select periods vary widely: most common are 1 (aggregate), 5, 15, and 25 years. Some historical and improvement-scale tables have 40–120 columns; these are truncated at 50 durations.
- Gender and risk class are inferred from the table name and description — a small number of older tables may be miscategorised.
- All data is sourced from the SOA's public repository. Verify critical rates against the [original source](https://mort.soa.org).

## Refreshing the data

To pull the latest tables from the SOA (e.g. after new tables are published):

```bash
rm mortality.db data.json data.js
python3 scraper.py --full --workers 25 --delay 0
```

Then reload `index.html` in your browser.
