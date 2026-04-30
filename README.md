# SOA Mortality Table Search

A searchable database of all ~3,000 public mortality tables hosted by the [Society of Actuaries](https://mort.soa.org). Enter a partial rate vector, gender, and issue age to identify which table(s) your rates come from. Each matching result can be exported directly to an AXIS-compatible Excel file.

## Live demo

Hosted on Vercel — no installation required:

👉 **[soa-mortality-table-search.vercel.app](https://soa-mortality-table-search.vercel.app)**

## How it works

The scraper fetches every table from `mort.soa.org`, parses the HTML, and stores the rates in a SQLite database. It then exports the data as a `data.js` file that the search page loads directly — no server required.

Each row in the database represents one **issue age** from one table, storing the full select-period rate vector (q(x,1), q(x,2), …) alongside the table name, gender, risk class, and a direct link back to the SOA website for verification.

## Files

| File | Description |
|---|---|
| `scraper.py` | Fetches and parses all SOA mortality tables into a local SQLite database and exports `data.js` |
| `index.html` | Self-contained search UI — open directly in a browser or deploy statically |
| `data.js` | JavaScript export of the database (~12 MB) loaded by `index.html`; committed so the live site works without running the scraper |
| `mortality.db` | SQLite database (~33 MB) — generated locally, not committed |
| `data.json` | Same data as `data.js` in plain JSON format — generated locally, not committed |

> **Note:** `data.js` is committed to the repository so the Vercel deployment works out of the box. `data.json` and `mortality.db` are local-only and excluded from git. If you want to refresh the data (e.g. after new SOA tables are published), run the scraper locally and commit the updated `data.js`.

## Quickstart (local)

### 1. Install dependencies

Requires Python 3.9+ and two packages:

```bash
pip install requests beautifulsoup4
```

### 2. Open the search page

```bash
open index.html
```

Or just double-click `index.html`. No local server needed — `data.js` is already in the repo.

### 3. (Optional) Refresh the data

To pull the latest tables from the SOA after new tables are published:

```bash
rm mortality.db data.json data.js
python3 scraper.py --full --workers 25 --delay 0
```

Then commit the updated `data.js` and push to redeploy.

| Flag | Default | Description |
|---|---|---|
| `--full` | off | Scrape all tables (omit for a 10-table test run) |
| `--workers` | 10 | Number of parallel HTTP workers |
| `--delay` | 0.1 | Per-worker pause between requests (seconds) |
| `--max-id` | 3500 | Upper bound for table ID scan |
| `--limit` | 10 | Max tables to scrape in test mode (ignored with `--full`) |

## Deploying to Vercel

Because the project is a static site with no build step, Vercel deploys it automatically:

1. Fork or clone this repo and push to GitHub.
2. Import the repository in the [Vercel dashboard](https://vercel.com/new).
3. Leave all build settings at their defaults (framework: **Other**, build command: blank, output directory: blank).
4. Click **Deploy** — Vercel will serve `index.html` and `data.js` as static assets.

To update the live data after a rescrape, commit the new `data.js` and push; Vercel redeploys automatically.

## Using the search

The search page filters the full database client-side:

- **Gender** — Male, Female, or Combined/Unisex
- **Issue Age** — the age at policy issue for the row you want to match
- **Rates** — enter as many duration rates as you have, starting at Duration 1 (q(x,1)). Leave later durations blank; only filled-in values are matched.
- **Name search only** — check this box to search by table name only, returning one card per table instead of one per issue age.

Matches are found within a tolerance of **±0.000001** per rate. Results are sorted with select-period tables first, then alphabetically by table name. Each result card links directly to the source table on the SOA website.

**Export to AXIS:** any select-period table card shows an "Export to AXIS ↗" link. Clicking it downloads an Excel file formatted for import into AXIS actuarial software (`EA_NNN_100` shape, select rates in rows 1–N, ultimate rates in row N+1).

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
    link            TEXT,       -- direct URL to SOA viewer
    ultimate_json   TEXT        -- JSON object mapping attained age → rate
)

rates (
    id              INTEGER PRIMARY KEY,
    table_identity  INTEGER,
    issue_age       INTEGER,
    d1 … d50        REAL        -- NULL for durations beyond the table's select period
)
```

## Data notes

- **Ultimate rates** for Select & Ultimate tables are stored separately in `ultimate_json` and used only for the AXIS export's ultimate row. The select portion is stored in `d1`–`d50`.
- **Aggregate tables** (no select period) are stored with a single rate in `d1` and `select_period = 1`. They match searches with one rate entered.
- Select periods vary widely: most common are 1 (aggregate), 5, 15, and 25 years. Some historical and improvement-scale tables have 40–120 columns; these are truncated at 50 durations.
- Gender and risk class are inferred from the table name and description — a small number of older tables may be miscategorised.
- All data is sourced from the SOA's public repository. Verify critical rates against the [original source](https://mort.soa.org).
