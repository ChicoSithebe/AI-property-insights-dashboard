# -*- coding: utf-8 -*-
r"""
Jayanalytics v3 (Enterprise + Star Schema + PowerBI-like Relationship Diagram)
FIX: SQL Studio "glitch reset" by persisting state in dcc.Store (store_sql_state)

Place this script in SAME folder as:
- Contracts.csv
- Conditions.csv

SQLite DB created in same folder: jayanalytics.db
"""

import os
import re
import json
import shutil
import sqlite3
import threading
import webbrowser
from datetime import date, datetime

import numpy as np
import pandas as pd
import requests  # verify=False required by user instruction

from dash import Dash, dcc, html, Input, Output, State, dash_table, no_update, callback_context
import plotly.express as px
import plotly.graph_objects as go


# =============================================================================
# OPENAI CONFIG (REQUIRED KEY)
# =============================================================================
API_KEY = (Secrete - cannot allow it to be ran)
OPENAI_MODEL = "gpt-4o"


# =============================================================================
# CONFIG
# =============================================================================
HT_BLUE = "#0B2E6D"
HT_BLUE2 = "#123B8D"
HT_ORANGE = "#F47C20"
BG = "#F5F7FB"
TXT = "#0E1B2A"
MUTED = "#6B7280"

APP_PORT = 8050
OPEN_BROWSER = True

CONTRACTS_FILE = "Contracts.csv"
CONDITIONS_FILE = "Conditions.csv"
DB_FILE = "jayanalytics.db"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


# =============================================================================
# CSS (Injected via app.index_string)
# =============================================================================
APP_CSS = f"""
:root {{
  --ht-blue: {HT_BLUE};
  --ht-blue2: {HT_BLUE2};
  --ht-orange: {HT_ORANGE};
  --bg: {BG};
  --txt: {TXT};
  --muted: {MUTED};
}}

html, body {{
  margin: 0;
  padding: 0;
  background: var(--bg);
  font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial;
  color: var(--txt);
}}

* {{ box-sizing: border-box; }}

.header {{
  background: linear-gradient(90deg, var(--ht-blue) 0%, var(--ht-blue2) 60%, var(--ht-orange) 150%);
  color: white;
  padding: 16px 18px;
  border-bottom-left-radius: 22px;
  border-bottom-right-radius: 22px;
  box-shadow: 0 10px 26px rgba(11,46,109,0.22);
}}

.badge {{
  display:inline-block;
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 900;
  border: 1px solid rgba(255,255,255,0.22);
  background: rgba(255,255,255,0.08);
  color: white;
}}

.container {{
  padding: 14px;
}}

.card {{
  background: white;
  border-radius: 18px;
  border: 1px solid rgba(11,46,109,0.10);
  box-shadow: 0 10px 24px rgba(11,46,109,0.08);
  padding: 12px;
}}

.card-title {{
  font-size: 12px;
  font-weight: 900;
  color: var(--txt);
  margin-bottom: 8px;
}}

.filters {{
  display: grid;
  grid-template-columns: repeat(6, 1fr);
  gap: 10px;
}}
@media (max-width: 1400px) {{
  .filters {{ grid-template-columns: repeat(3, 1fr); }}
}}

.kpis {{
  display:grid;
  grid-template-columns: repeat(6, 1fr);
  gap: 10px;
}}
@media (max-width: 1400px) {{
  .kpis {{ grid-template-columns: repeat(3, 1fr); }}
}}

.kpi {{
  background: white;
  border-radius: 16px;
  border: 1px solid rgba(11,46,109,0.10);
  box-shadow: 0 10px 24px rgba(11,46,109,0.08);
  padding: 12px;
  cursor: pointer;
  transition: transform .08s ease-in-out, box-shadow .08s ease-in-out;
}}
.kpi:hover {{
  transform: translateY(-2px);
  box-shadow: 0 14px 30px rgba(11,46,109,0.12);
}}
.kpi-label {{
  font-size: 11px;
  color: var(--muted);
  font-weight: 900;
}}
.kpi-value {{
  margin-top: 6px;
  font-size: 20px;
  font-weight: 900;
}}
.kpi-bar {{
  height: 7px;
  margin-top: 10px;
  border-radius: 999px;
  background: rgba(244,124,32,0.18);
}}
.kpi-bar > div {{
  height: 100%;
  border-radius: 999px;
  background: var(--ht-orange);
  width: 62%;
}}

.tabwrap {{
  background: white;
  border-radius: 18px;
  border: 1px solid rgba(11,46,109,0.10);
  box-shadow: 0 10px 24px rgba(11,46,109,0.08);
  overflow: hidden;
}}

.note {{
  font-size: 12px;
  color: var(--muted);
}}

.warn {{
  color: #B45309;
  font-weight: 900;
}}

pre {{
  margin: 0;
}}

.sqlbox {{
  width: 100%;
  height: 180px;
  border-radius: 12px;
  padding: 10px;
  border: 1px solid rgba(11,46,109,0.15);
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 12px;
}}
"""


# =============================================================================
# HELPERS: CSV, MONEY, DATES, REGION
# =============================================================================
def safe_read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    df = None
    for enc in ["utf-8", "utf-8-sig", "latin1", "cp1252"]:
        try:
            df = pd.read_csv(path, dtype=str, encoding=enc, low_memory=False)
            print(f"[INFO] Read {os.path.basename(path)} with encoding: {enc}")
            break
        except Exception:
            df = None

    if df is None:
        df = pd.read_csv(path, dtype=str, encoding_errors="ignore", low_memory=False)
        print(f"[WARN] Fallback read used for: {os.path.basename(path)}")

    df.columns = [c.strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
        df.loc[df[c].isin(["nan", "None", "NaT", ""]), c] = np.nan
    return df


def to_dt(s):
    return pd.to_datetime(s, errors="coerce")


def to_money(s):
    x = s.astype(str).str.strip()
    x = x.replace({"nan": np.nan, "None": np.nan})
    x = x.str.replace(r"[^\d\-\.\(\),]", "", regex=True)
    x = x.str.replace(",", "")
    x = x.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
    return pd.to_numeric(x, errors="coerce")


def region_code(contract_name: str):
    if not isinstance(contract_name, str) or not contract_name.strip():
        return None
    m = re.match(r"^[A-Za-z]{2}([A-Za-z]{2,3})\d+", contract_name.strip())
    return m.group(1).upper() if m else None


def years_until(dt):
    if pd.isna(dt):
        return np.nan
    today = pd.Timestamp(date.today())
    return (dt - today).days / 365.25


# =============================================================================
# OPENAI
# =============================================================================
def openai_chat(messages, temperature=0.2):
    payload = {"model": OPENAI_MODEL, "messages": messages, "temperature": temperature}
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
    r = requests.post("https://api.openai.com/v1/chat/completions", json=payload, headers=headers, verify=False)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]


def schema_text_star():
    return (
        "STAR SCHEMA TABLES:\n"
        "DIMENSIONS:\n"
        "- dim_contract_star\n- dim_region\n- dim_city\n- dim_status\n- dim_landlord\n- dim_landlord_type\n"
        "- dim_currency\n- dim_expiration_type\n- dim_date\n\n"
        "FACTS:\n- fact_rent_step\n- fact_contract_snapshot\n\n"
        "VIEWS:\n- vw_star_snapshot\n- vw_rent_steps\n- vw_star_contracts\n"
    )


def ai_plan(question: str) -> dict:
    sys_msg = (
        "You are Jayanalytics AI Copilot for lease/property analytics.\n"
        "Return JSON only with keys: answer, sql, chart.\n"
        "SQL must be a single SAFE SQLite SELECT.\n"
        "Prefer querying views: vw_star_snapshot and vw_rent_steps.\n"
        "For non-aggregated results always LIMIT 500.\n"
        "chart.type: bar|line|pie|scatter|none. Provide chart.x, chart.y, chart.title.\n"
    )
    user_msg = f"SCHEMA:\n{schema_text_star()}\n\nQUESTION:\n{question}\n\nReturn JSON only."
    text = openai_chat([{"role": "system", "content": sys_msg}, {"role": "user", "content": user_msg}], temperature=0.2)

    try:
        obj = json.loads(text)
    except Exception:
        m = re.search(r"\{.*\}", text, flags=re.DOTALL)
        obj = json.loads(m.group(0)) if m else {"answer": "Could not parse JSON.", "sql": "", "chart": {"type": "none", "x": "", "y": "", "title": ""}}

    sql = (obj.get("sql") or "").strip()
    if sql:
        low = sql.lower()
        if (not low.startswith("select")) or any(k in low for k in ["insert", "update", "delete", "drop", "alter", "pragma", "create", "attach", "detach", "replace"]):
            obj["sql"] = ""
            obj["answer"] = (obj.get("answer", "") + "\n\nBlocked unsafe SQL (only SELECT allowed in AI). Use SQL Studio for updates.").strip()

    ch = obj.get("chart") or {}
    if not isinstance(ch, dict):
        ch = {"type": "none", "x": "", "y": "", "title": ""}
    for k in ["type", "x", "y", "title"]:
        ch.setdefault(k, "" if k != "type" else "none")
    obj["chart"] = ch
    return obj


def chart_from_df(df: pd.DataFrame, chart: dict) -> go.Figure:
    if df is None or df.empty:
        fig = go.Figure()
        fig.update_layout(title="No data to display", paper_bgcolor="white", plot_bgcolor="white")
        return fig

    ctype = (chart.get("type") or "none").lower()
    x = chart.get("x") or ""
    y = chart.get("y") or ""
    title = chart.get("title") or "AI Chart"

    cols = df.columns.tolist()
    if ctype == "none":
        fig = go.Figure()
        fig.update_layout(title="No chart requested", paper_bgcolor="white", plot_bgcolor="white")
        return fig

    if x not in cols and len(cols) >= 1:
        x = cols[0]
    if y not in cols and len(cols) >= 2:
        y = cols[1]

    try:
        if ctype == "bar":
            fig = px.bar(df, x=x, y=y, title=title)
            fig.update_traces(marker_color=HT_ORANGE)
        elif ctype == "line":
            fig = px.line(df, x=x, y=y, title=title, markers=True)
        elif ctype == "pie":
            fig = px.pie(df, names=x, values=y, title=title)
        elif ctype == "scatter":
            fig = px.scatter(df, x=x, y=y, title=title)
        else:
            fig = go.Figure()
            fig.update_layout(title="Unknown chart type", paper_bgcolor="white", plot_bgcolor="white")
    except Exception as e:
        fig = go.Figure()
        fig.update_layout(title=f"Chart failed: {e}", paper_bgcolor="white", plot_bgcolor="white")

    fig.update_layout(paper_bgcolor="white", plot_bgcolor="white", margin=dict(l=30, r=20, t=50, b=40))
    return fig


# =============================================================================
# DB BUILD: BASE + STAR SCHEMA
# =============================================================================
def build_base_tables(conn: sqlite3.Connection, contracts: pd.DataFrame, conditions: pd.DataFrame):
    for dc in ["Contract start date", "1st Contract End", "Contract end date", "RFI Date"]:
        if dc in contracts.columns:
            contracts[dc] = to_dt(contracts[dc])

    if "Unit Price yearly" not in conditions.columns:
        raise KeyError("Conditions.csv missing 'Unit Price yearly'")
    if "Condition Valid From" not in conditions.columns or "Condition Valid To" not in conditions.columns:
        raise KeyError("Conditions.csv missing Condition Valid From/To")

    conditions["rent_yearly"] = to_money(conditions["Unit Price yearly"])
    if "Unit Price Monthly" in conditions.columns:
        conditions["rent_monthly"] = to_money(conditions["Unit Price Monthly"])
    else:
        conditions["rent_monthly"] = conditions["rent_yearly"] / 12.0

    conditions["valid_from"] = to_dt(conditions["Condition Valid From"])
    conditions["valid_to"] = to_dt(conditions["Condition Valid To"])

    contracts.to_sql("raw_contracts", conn, if_exists="replace", index=False)
    conditions.to_sql("raw_conditions", conn, if_exists="replace", index=False)

    dim = contracts.copy()
    dim["contract_name"] = dim["Contract Name"].astype(str).str.strip()
    if "Contract start date" in dim.columns:
        dim = dim.sort_values(["contract_name", "Contract start date"], ascending=[True, False])
    dim = dim.drop_duplicates(subset=["contract_name"], keep="first").reset_index(drop=True)
    dim["contract_id"] = np.arange(1, len(dim) + 1, dtype=int)

    dim["region_code"] = dim["contract_name"].apply(region_code)
    dim["lease_status"] = dim["Lease Status"] if "Lease Status" in dim.columns else np.nan
    dim["landlord_type"] = dim["Landlord Type"] if "Landlord Type" in dim.columns else np.nan
    dim["site_city"] = dim["Site City"] if "Site City" in dim.columns else np.nan
    dim["expiration_type"] = dim["Expiration Type"] if "Expiration Type" in dim.columns else np.nan
    dim["currency"] = dim["Contract Currency"] if "Contract Currency" in dim.columns else np.nan
    dim["contract_start_date"] = dim["Contract start date"] if "Contract start date" in dim.columns else pd.NaT
    dim["contract_end_date"] = dim["Contract end date"] if "Contract end date" in dim.columns else pd.NaT

    end_dt = pd.to_datetime(dim["contract_end_date"], errors="coerce")
    dim["expiry_year"] = end_dt.dt.year
    dim["years_to_end"] = end_dt.apply(years_until)
    today = pd.Timestamp(date.today())
    dim["expired_flag"] = np.where((pd.notna(end_dt)) & (end_dt < today), "Expired", "Active/Not Expired")

    dim_out = dim[[
        "contract_id", "contract_name", "region_code",
        "lease_status", "landlord_type", "site_city",
        "currency", "expiration_type",
        "contract_start_date", "contract_end_date",
        "expiry_year", "years_to_end", "expired_flag"
    ]].copy()
    dim_out.to_sql("dim_contract", conn, if_exists="replace", index=False)

    fc = conditions.copy()
    fc["contract_name"] = fc["Contract Name"].astype(str).str.strip()
    fc = fc.merge(dim_out[["contract_id", "contract_name"]], on="contract_name", how="left")
    fc = fc.sort_values(["contract_id", "valid_from"], ascending=[True, True]).reset_index(drop=True)
    fc["condition_id"] = np.arange(1, len(fc) + 1, dtype=int)

    def _maybe(col):
        return col if col in fc.columns else None

    fact_condition = pd.DataFrame({
        "condition_id": fc["condition_id"],
        "contract_id": fc["contract_id"],
        "contract_name": fc["contract_name"],
        "valid_from": fc["valid_from"],
        "valid_to": fc["valid_to"],
        "rent_yearly": fc["rent_yearly"],
        "rent_monthly": fc["rent_monthly"],
        "condition_type": fc[_maybe("Condition Type")] if _maybe("Condition Type") else np.nan,
        "condition_purpose": fc[_maybe("Condition Purpose")] if _maybe("Condition Purpose") else np.nan,
        "frequency": fc[_maybe("Frequency")] if _maybe("Frequency") else np.nan,
        "frequency_unit": fc[_maybe("Frequency Unit")] if _maybe("Frequency Unit") else np.nan,
    })
    fact_condition.to_sql("fact_condition", conn, if_exists="replace", index=False)

    as_of = pd.Timestamp(date.today())
    far_future = pd.Timestamp("2099-12-31")
    metric_rows = []

    for cid, g in fact_condition.groupby("contract_id", dropna=False):
        if pd.isna(cid):
            continue

        g2 = g.copy()
        g2["vf_eff"] = pd.to_datetime(g2["valid_from"], errors="coerce").fillna(pd.Timestamp("1900-01-01"))
        g2["vt_eff"] = pd.to_datetime(g2["valid_to"], errors="coerce").fillna(far_future)

        gp = g2.dropna(subset=["rent_yearly"]).copy()
        line_count = int(len(g2))

        initial = np.nan
        current = np.nan
        cur_vf = pd.NaT
        cur_vt = pd.NaT

        if len(gp):
            gp = gp.sort_values(["vf_eff", "condition_id"], ascending=[True, True])
            initial = float(gp.iloc[0]["rent_yearly"])

            covers = gp[(gp["vf_eff"] <= as_of) & (gp["vt_eff"] >= as_of)]
            if len(covers):
                cur = covers.sort_values(["vf_eff", "condition_id"], ascending=[False, False]).iloc[0]
            else:
                past = gp[gp["vf_eff"] <= as_of]
                if len(past):
                    cur = past.sort_values(["vf_eff", "condition_id"], ascending=[False, False]).iloc[0]
                else:
                    future = gp[gp["vf_eff"] > as_of].sort_values(["vf_eff", "condition_id"])
                    cur = future.iloc[0] if len(future) else gp.iloc[-1]

            current = float(cur["rent_yearly"])
            cur_vf = cur["valid_from"]
            cur_vt = cur["valid_to"]

        pct = ((current - initial) / initial * 100.0) if (pd.notna(current) and pd.notna(initial) and initial != 0) else np.nan

        metric_rows.append({
            "contract_id": int(cid),
            "conditions_line_count": line_count,
            "initial_rent_yearly": initial,
            "current_rent_yearly": current,
            "current_rent_monthly": (current / 12.0) if pd.notna(current) else np.nan,
            "rent_change_pct": round(float(pct), 2) if pd.notna(pct) else np.nan,
            "current_valid_from": cur_vf,
            "current_valid_to": cur_vt,
            "as_of_date": as_of
        })

    metrics = pd.DataFrame(metric_rows)
    metrics.to_sql("fact_contract_metrics", conn, if_exists="replace", index=False)

    cur = conn.cursor()
    cur.execute("DROP VIEW IF EXISTS vw_contracts_merged")
    cur.execute("""
    CREATE VIEW vw_contracts_merged AS
    SELECT
        dc.contract_id,
        dc.contract_name AS "Contract Name",
        dc.region_code   AS "Region Code",
        dc.lease_status  AS "Lease Status",
        dc.landlord_type AS "Landlord Type",
        dc.site_city     AS "Site City",
        dc.currency      AS "Currency",
        dc.expiration_type AS "Expiration Type",
        dc.contract_start_date AS "Contract start date",
        dc.contract_end_date   AS "Contract end date",
        dc.expiry_year    AS "Expiry Year",
        dc.years_to_end   AS "Years to End",
        dc.expired_flag   AS "Expired Lease",
        fcm.conditions_line_count AS "Conditions Line Count",
        fcm.initial_rent_yearly AS "Initial Rent (Yearly)",
        fcm.current_rent_yearly AS "Current Rent (Yearly)",
        fcm.current_rent_monthly AS "Current Rent (Monthly)",
        fcm.rent_change_pct AS "Rent Change %",
        fcm.current_valid_from AS "Current Conditions Valid From",
        fcm.current_valid_to   AS "Current Conditions Valid To",
        fcm.as_of_date AS "As Of Date"
    FROM dim_contract dc
    LEFT JOIN fact_contract_metrics fcm
      ON dc.contract_id = fcm.contract_id
    """)
    conn.commit()


def build_star_schema(conn: sqlite3.Connection):
    dim = pd.read_sql_query("SELECT * FROM dim_contract", conn)
    fc = pd.read_sql_query("SELECT * FROM fact_condition", conn)
    metrics = pd.read_sql_query("SELECT * FROM fact_contract_metrics", conn)

    def make_dim(df, col, dim_name, key_name, value_name):
        x = df[[col]].dropna().drop_duplicates().sort_values(col).reset_index(drop=True)
        x[key_name] = np.arange(1, len(x) + 1, dtype=int)
        x = x[[key_name, col]].rename(columns={col: value_name})
        x.to_sql(dim_name, conn, if_exists="replace", index=False)
        return x

    dim_region = make_dim(dim, "region_code", "dim_region", "region_id", "region_code")
    dim_city = make_dim(dim, "site_city", "dim_city", "city_id", "site_city")
    dim_status = make_dim(dim, "lease_status", "dim_status", "status_id", "lease_status")
    dim_landlord_type = make_dim(dim, "landlord_type", "dim_landlord_type", "landlord_type_id", "landlord_type")
    dim_currency = make_dim(dim, "currency", "dim_currency", "currency_id", "currency")
    dim_exp = make_dim(dim, "expiration_type", "dim_expiration_type", "expiration_type_id", "expiration_type")

    # dim_landlord (derived)
    dtmp = dim.copy()
    dtmp["landlord_type"] = dtmp["landlord_type"].astype(str)
    dtmp.loc[dtmp["landlord_type"].isin(["nan", "None"]), "landlord_type"] = np.nan

    city_f = dtmp["site_city"].astype(str)
    city_f.loc[city_f.isin(["nan", "None"])] = ""
    reg_f = dtmp["region_code"].astype(str)
    reg_f.loc[reg_f.isin(["nan", "None"])] = ""

    dtmp["landlord_key"] = dtmp["landlord_type"].fillna("Unknown") + "|" + np.where(city_f.str.len() > 0, city_f, reg_f)
    dim_landlord = dtmp[["landlord_key", "landlord_type"]].drop_duplicates().sort_values("landlord_key").reset_index(drop=True)
    dim_landlord["landlord_id"] = np.arange(1, len(dim_landlord) + 1, dtype=int)
    dim_landlord["landlord_name"] = np.nan
    dim_landlord = dim_landlord[["landlord_id", "landlord_key", "landlord_type", "landlord_name"]]
    dim_landlord.to_sql("dim_landlord", conn, if_exists="replace", index=False)

    dtmp2 = dtmp[["contract_id", "landlord_key"]].merge(dim_landlord[["landlord_id", "landlord_key"]], on="landlord_key", how="left")

    dim2 = dim.copy()
    dim2 = dim2.merge(dim_region, how="left", on="region_code")
    dim2 = dim2.merge(dim_city, how="left", on="site_city")
    dim2 = dim2.merge(dim_status, how="left", on="lease_status")
    dim2 = dim2.merge(dim_landlord_type, how="left", on="landlord_type")
    dim2 = dim2.merge(dim_currency, how="left", on="currency")
    dim2 = dim2.merge(dim_exp, how="left", on="expiration_type")
    dim2 = dim2.merge(dtmp2[["contract_id", "landlord_id"]], how="left", on="contract_id")

    dim_star = dim2[[
        "contract_id", "contract_name",
        "region_id", "city_id", "status_id", "landlord_id", "landlord_type_id", "currency_id", "expiration_type_id",
        "contract_start_date", "contract_end_date", "expiry_year", "years_to_end", "expired_flag"
    ]].copy()
    dim_star.to_sql("dim_contract_star", conn, if_exists="replace", index=False)

    # dim_date
    dates = []
    for c in ["contract_start_date", "contract_end_date"]:
        if c in dim_star.columns:
            dates.append(pd.to_datetime(dim_star[c], errors="coerce"))
    for c in ["valid_from", "valid_to"]:
        if c in fc.columns:
            dates.append(pd.to_datetime(fc[c], errors="coerce"))
    for c in ["current_valid_from", "current_valid_to", "as_of_date"]:
        if c in metrics.columns:
            dates.append(pd.to_datetime(metrics[c], errors="coerce"))

    all_dates = pd.concat(dates).dropna().dt.normalize() if len(dates) else pd.Series([], dtype="datetime64[ns]")
    if len(all_dates) == 0:
        all_dates = pd.Series(pd.date_range("2020-01-01", "2035-12-31", freq="D"))

    dmin = all_dates.min()
    dmax = all_dates.max()
    cal = pd.DataFrame({"date": pd.date_range(dmin, dmax, freq="D")})
    cal["date_id"] = cal["date"].dt.strftime("%Y%m%d").astype(int)
    cal["year"] = cal["date"].dt.year
    cal["month"] = cal["date"].dt.month
    cal["month_name"] = cal["date"].dt.strftime("%b")
    cal["quarter"] = cal["date"].dt.quarter
    cal["day"] = cal["date"].dt.day
    cal["day_name"] = cal["date"].dt.strftime("%a")
    cal.to_sql("dim_date", conn, if_exists="replace", index=False)

    # fact_rent_step
    frs = fc.copy()
    frs["valid_from"] = pd.to_datetime(frs["valid_from"], errors="coerce").dt.normalize()
    frs["valid_to"] = pd.to_datetime(frs["valid_to"], errors="coerce").dt.normalize()
    frs["date_id_valid_from"] = pd.to_numeric(frs["valid_from"].dt.strftime("%Y%m%d"), errors="coerce").astype("Int64")
    frs["date_id_valid_to"] = pd.to_numeric(frs["valid_to"].dt.strftime("%Y%m%d"), errors="coerce").astype("Int64")
    frs = frs.merge(dim_star[["contract_id", "currency_id"]], on="contract_id", how="left")

    frs_out = frs[[
        "condition_id", "contract_id", "currency_id",
        "date_id_valid_from", "date_id_valid_to",
        "rent_yearly", "rent_monthly",
        "condition_type", "condition_purpose", "frequency", "frequency_unit"
    ]].copy()
    frs_out.to_sql("fact_rent_step", conn, if_exists="replace", index=False)

    # fact_contract_snapshot
    snap = metrics.copy()
    snap["as_of_date"] = pd.to_datetime(snap["as_of_date"], errors="coerce").dt.normalize()
    snap["as_of_date_id"] = pd.to_numeric(snap["as_of_date"].dt.strftime("%Y%m%d"), errors="coerce").astype("Int64")

    add = dim_star[["contract_id", "expiry_year", "years_to_end", "expired_flag"]]
    snap = snap.merge(add, on="contract_id", how="left")

    y = pd.to_numeric(snap["years_to_end"], errors="coerce")
    snap["risk_band"] = np.select(
        [y <= 0, y <= 1, y <= 2, y <= 5],
        ["Expired", "Critical", "High", "Medium"],
        default="Low"
    )

    snap_out = snap[[
        "contract_id", "as_of_date_id",
        "initial_rent_yearly", "current_rent_yearly", "current_rent_monthly",
        "rent_change_pct", "years_to_end", "expiry_year",
        "risk_band"
    ]].copy()
    snap_out.to_sql("fact_contract_snapshot", conn, if_exists="replace", index=False)

    cur = conn.cursor()

    cur.execute("DROP VIEW IF EXISTS vw_star_contracts")
    cur.execute("""
    CREATE VIEW vw_star_contracts AS
    SELECT
      dcs.contract_id,
      dcs.contract_name AS "Contract Name",
      r.region_code AS "Region Code",
      s.lease_status AS "Lease Status",
      lt.landlord_type AS "Landlord Type",
      l.landlord_key AS "Landlord Key",
      l.landlord_name AS "Landlord Name",
      c.site_city AS "Site City",
      cu.currency AS "Currency",
      et.expiration_type AS "Expiration Type",
      dcs.contract_start_date AS "Contract start date",
      dcs.contract_end_date AS "Contract end date",
      dcs.expiry_year AS "Expiry Year",
      dcs.years_to_end AS "Years to End",
      dcs.expired_flag AS "Expired Lease"
    FROM dim_contract_star dcs
    LEFT JOIN dim_region r ON dcs.region_id = r.region_id
    LEFT JOIN dim_status s ON dcs.status_id = s.status_id
    LEFT JOIN dim_landlord_type lt ON dcs.landlord_type_id = lt.landlord_type_id
    LEFT JOIN dim_landlord l ON dcs.landlord_id = l.landlord_id
    LEFT JOIN dim_city c ON dcs.city_id = c.city_id
    LEFT JOIN dim_currency cu ON dcs.currency_id = cu.currency_id
    LEFT JOIN dim_expiration_type et ON dcs.expiration_type_id = et.expiration_type_id
    """)

    cur.execute("DROP VIEW IF EXISTS vw_star_snapshot")
    cur.execute("""
    CREATE VIEW vw_star_snapshot AS
    SELECT
      sc.*,
      fcs.initial_rent_yearly AS "Initial Rent (Yearly)",
      fcs.current_rent_yearly AS "Current Rent (Yearly)",
      fcs.current_rent_monthly AS "Current Rent (Monthly)",
      fcs.rent_change_pct AS "Rent Change %",
      fcs.risk_band AS "Risk Band",
      fcs.years_to_end AS "Years to End (Snapshot)",
      fcs.expiry_year AS "Expiry Year (Snapshot)",
      fcs.as_of_date_id AS "As Of Date ID"
    FROM vw_star_contracts sc
    LEFT JOIN fact_contract_snapshot fcs
      ON sc.contract_id = fcs.contract_id
    """)

    cur.execute("DROP VIEW IF EXISTS vw_rent_steps")
    cur.execute("""
    CREATE VIEW vw_rent_steps AS
    SELECT
      sc.*,
      frs.condition_id AS "Condition ID",
      frs.date_id_valid_from AS "Valid From ID",
      frs.date_id_valid_to AS "Valid To ID",
      d1.date AS "Valid From",
      d2.date AS "Valid To",
      frs.rent_yearly AS "Rent (Yearly)",
      frs.rent_monthly AS "Rent (Monthly)",
      frs.condition_type AS "Condition Type",
      frs.condition_purpose AS "Condition Purpose",
      frs.frequency AS "Frequency",
      frs.frequency_unit AS "Frequency Unit"
    FROM fact_rent_step frs
    LEFT JOIN vw_star_contracts sc ON frs.contract_id = sc.contract_id
    LEFT JOIN dim_date d1 ON frs.date_id_valid_from = d1.date_id
    LEFT JOIN dim_date d2 ON frs.date_id_valid_to = d2.date_id
    """)

    cur.execute('CREATE INDEX IF NOT EXISTS idx_frs_contract ON fact_rent_step(contract_id);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_frs_from ON fact_rent_step(date_id_valid_from);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_snap_contract ON fact_contract_snapshot(contract_id);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_snap_date ON fact_contract_snapshot(as_of_date_id);')
    conn.commit()


def build_metadata(conn: sqlite3.Connection):
    schema_rows = []
    cur = conn.cursor()

    objects = cur.execute("""
        SELECT name, type
        FROM sqlite_master
        WHERE type IN ('table','view')
          AND name NOT LIKE 'sqlite_%'
        ORDER BY type, name
    """).fetchall()

    for name, ttype in objects:
        try:
            rc = cur.execute(f"SELECT COUNT(*) FROM [{name}]").fetchone()[0]
        except Exception:
            rc = None

        try:
            df = pd.read_sql_query(f"SELECT * FROM [{name}] LIMIT 1", conn)
            for c in df.columns:
                schema_rows.append({"object": name, "type": ttype, "column": c, "row_count": rc})
        except Exception:
            schema_rows.append({"object": name, "type": ttype, "column": None, "row_count": rc})

    md = pd.DataFrame(schema_rows)
    md.to_sql("metadata_schema", conn, if_exists="replace", index=False)


def build_database(project_dir: str):
    contracts_path = os.path.join(project_dir, CONTRACTS_FILE)
    conditions_path = os.path.join(project_dir, CONDITIONS_FILE)

    if not os.path.exists(contracts_path) or not os.path.exists(conditions_path):
        raise FileNotFoundError("Contracts.csv and Conditions.csv must be in same folder as this script.")

    print("[STEP] Loading CSVs...")
    contracts = safe_read_csv(contracts_path)
    conditions = safe_read_csv(conditions_path)

    if "Contract Name" not in contracts.columns:
        raise KeyError("Contracts.csv missing 'Contract Name'")
    if "Contract Name" not in conditions.columns:
        raise KeyError("Conditions.csv missing 'Contract Name'")

    db_path = os.path.join(project_dir, DB_FILE)
    conn = sqlite3.connect(db_path, check_same_thread=False)

    print("[STEP] Building base tables...")
    build_base_tables(conn, contracts, conditions)

    print("[STEP] Building STAR schema (with dim_landlord)...")
    build_star_schema(conn)

    print("[STEP] Building metadata...")
    build_metadata(conn)

    print("[OK] Database ready:", db_path)
    return conn, db_path


# =============================================================================
# SQL STUDIO (Safety + backups)
# =============================================================================
def is_safe_select(sql: str) -> bool:
    low = (sql or "").strip().lower()
    if not low.startswith("select"):
        return False
    bad = ["insert", "update", "delete", "drop", "alter", "pragma", "create", "attach", "detach", "replace"]
    return not any(b in low for b in bad)


def is_allowed_write(sql: str) -> bool:
    low = (sql or "").strip().lower()
    if not (low.startswith("update") or low.startswith("insert") or low.startswith("delete")):
        return False
    blocked = ["drop", "alter", "pragma", "create", "attach", "detach", "replace", "vacuum"]
    if any(b in low for b in blocked):
        return False
    allowed_targets = ["dim_contract_star", "dim_contract", "fact_rent_step", "fact_condition", "dim_landlord"]
    return any(t in low for t in allowed_targets)


def backup_db(db_path: str):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(os.path.dirname(db_path), f"jayanalytics_backup_{ts}.db")
    shutil.copy2(db_path, backup_path)
    return backup_path


def refresh_star_after_write(conn: sqlite3.Connection):
    build_star_schema(conn)
    build_metadata(conn)


# =============================================================================
# RELATIONSHIP DIAGRAM
# =============================================================================
def relationship_diagram_figure():
    nodes = [
        ("fact_contract_snapshot", "fact", 0.0, 0.2),
        ("fact_rent_step", "fact", 0.0, -0.2),
        ("dim_contract_star", "dim", -0.35, 0.0),
        ("dim_region", "dim", -0.75, 0.25),
        ("dim_city", "dim", -0.75, -0.25),
        ("dim_status", "dim", -0.75, 0.0),
        ("dim_landlord", "dim", -0.75, -0.05),
        ("dim_landlord_type", "dim", -0.75, -0.45),
        ("dim_currency", "dim", 0.75, 0.25),
        ("dim_expiration_type", "dim", 0.75, -0.25),
        ("dim_date", "dim", 0.55, 0.0),
    ]

    edges = [
        ("fact_contract_snapshot", "dim_contract_star", "contract_id"),
        ("fact_rent_step", "dim_contract_star", "contract_id"),
        ("dim_contract_star", "dim_region", "region_id"),
        ("dim_contract_star", "dim_city", "city_id"),
        ("dim_contract_star", "dim_status", "status_id"),
        ("dim_contract_star", "dim_landlord", "landlord_id"),
        ("dim_contract_star", "dim_landlord_type", "landlord_type_id"),
        ("dim_contract_star", "dim_currency", "currency_id"),
        ("dim_contract_star", "dim_expiration_type", "expiration_type_id"),
        ("fact_rent_step", "dim_date", "date_id_valid_from/to"),
        ("fact_contract_snapshot", "dim_date", "as_of_date_id"),
    ]

    pos = {n: (x, y) for n, _, x, y in nodes}
    edge_x, edge_y = [], []
    for a, b, _label in edges:
        x0, y0 = pos[a]
        x1, y1 = pos[b]
        edge_x += [x0, x1, None]
        edge_y += [y0, y1, None]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=edge_x, y=edge_y,
        mode="lines",
        line=dict(color="rgba(11,46,109,0.25)", width=3),
        hoverinfo="none",
        showlegend=False
    ))

    xs = [x for _, _, x, _ in nodes]
    ys = [y for _, _, _, y in nodes]
    names = [n for n, _, _, _ in nodes]
    groups = [g for _, g, _, _ in nodes]

    colors = [HT_ORANGE if g == "fact" else HT_BLUE for g in groups]
    sizes = [28 if g == "fact" else 22 for g in groups]

    fig.add_trace(go.Scatter(
        x=xs, y=ys,
        mode="markers+text",
        marker=dict(size=sizes, color=colors, line=dict(color="white", width=2)),
        text=names,
        textposition="top center",
        textfont=dict(size=12, color=TXT),
        hovertemplate="<b>%{text}</b><extra></extra>",
        showlegend=False
    ))

    fig.update_layout(
        title="Jayanalytics Star Schema (Power BI–style Model View)",
        paper_bgcolor="white",
        plot_bgcolor="white",
        margin=dict(l=20, r=20, t=50, b=20),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        height=520
    )
    return fig


# =============================================================================
# DASH APP
# =============================================================================
def build_app(conn: sqlite3.Connection, db_path: str):
    app = Dash(__name__)
    app.title = "Jayanalytics (Star Schema + Model View)"

    app.index_string = f"""
    <!DOCTYPE html>
    <html>
      <head>
        {{%metas%}}
        <title>{{%title%}}</title>
        {{%favicon%}}
        {{%css%}}
        <style>{APP_CSS}</style>
      </head>
      <body>
        {{%app_entry%}}
        <footer>
          {{%config%}}
          {{%scripts%}}
          {{%renderer%}}
        </footer>
      </body>
    </html>
    """

    def load_snapshot():
        df = pd.read_sql_query("SELECT * FROM vw_star_snapshot", conn)
        for c in ["Contract start date", "Contract end date"]:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce")
        return df

    def load_rent_steps(limit=20000):
        return pd.read_sql_query(f"SELECT * FROM vw_rent_steps LIMIT {int(limit)}", conn)

    snap = load_snapshot()

    expiry_opts = [{"label": str(int(x)), "value": str(int(x))} for x in sorted(pd.to_numeric(snap["Expiry Year"], errors="coerce").dropna().unique())]
    region_opts = [{"label": str(x), "value": str(x)} for x in sorted(snap["Region Code"].dropna().unique())]
    status_opts = [{"label": str(x), "value": str(x)} for x in sorted(snap["Lease Status"].dropna().unique())]
    landlord_opts = [{"label": str(x), "value": str(x)} for x in sorted(snap["Landlord Type"].dropna().unique())]
    risk_opts = [{"label": str(x), "value": str(x)} for x in ["Low", "Medium", "High", "Critical", "Expired"]]

    default_sql = (
        "-- Example: avg current rent monthly by region\n"
        "SELECT \"Region Code\", AVG(\"Current Rent (Monthly)\") AS avg_rent\n"
        "FROM vw_star_snapshot\n"
        "GROUP BY \"Region Code\"\n"
        "ORDER BY avg_rent DESC\n"
        "LIMIT 50;\n"
    )

    # Persist SQL Studio UI state here (THIS FIXES YOUR GLITCH)
    sql_state_defaults = {
        "sql_text": default_sql,
        "status": "Ready.",
        "fig": go.Figure().update_layout(title="Run a query to see results", paper_bgcolor="white", plot_bgcolor="white").to_dict(),
        "data": [],
        "columns": []
    }

    app.layout = html.Div([
        html.Div(className="header", children=[
            html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}, children=[
                html.Div([
                    html.Div("Jayanalytics", style={"fontSize": "26px", "fontWeight": "900"}),
                    html.Div(f"Star Schema + Model View • SQLite: {os.path.basename(db_path)}", style={"fontSize": "12px", "opacity": 0.9})
                ]),
                html.Div("Enterprise Portfolio Intelligence • Leases • Payments • Expiry Risk • AI", className="badge")
            ])
        ]),
        html.Div(className="container", children=[
            html.Div(className="tabwrap", children=[
                dcc.Tabs(id="tabs", value="tab_exec", children=[
                    dcc.Tab(label="Executive Dashboard", value="tab_exec"),
                    dcc.Tab(label="Payments & Rent", value="tab_pay"),
                    dcc.Tab(label="AI Copilot", value="tab_ai"),
                    dcc.Tab(label="SQL Studio", value="tab_sql"),
                    dcc.Tab(label="Data Model", value="tab_model"),
                ]),
                html.Div(id="tab_content", style={"padding": "14px"})
            ])
        ]),
        dcc.Store(id="store_ai_hist", data=[]),
        dcc.Store(id="store_refresh_tick", data=0),
        dcc.Store(id="store_sql_state", data=sql_state_defaults),
    ])

    # ----------------- Tab Renderer -----------------
    @app.callback(
        Output("tab_content", "children"),
        Input("tabs", "value"),
        Input("store_refresh_tick", "data"),
        State("store_sql_state", "data")
    )
    def render_tab(tab, _tick, sql_state):
        snap_local = load_snapshot()
        sql_state = sql_state or {}

        if tab == "tab_exec":
            return html.Div([
                html.Div(className="filters", children=[
                    html.Div(className="card", children=[
                        html.Div("Expiry Year", className="card-title"),
                        dcc.Dropdown(id="f_expiry", options=expiry_opts, multi=True, placeholder="Select year(s)...")
                    ]),
                    html.Div(className="card", children=[
                        html.Div("Region Code", className="card-title"),
                        dcc.Dropdown(id="f_region", options=region_opts, multi=True, placeholder="KN, NK, LU ...")
                    ]),
                    html.Div(className="card", children=[
                        html.Div("Lease Status", className="card-title"),
                        dcc.Dropdown(id="f_status", options=status_opts, multi=True, placeholder="Active / On Hold ...")
                    ]),
                    html.Div(className="card", children=[
                        html.Div("Landlord Type", className="card-title"),
                        dcc.Dropdown(id="f_landlord", options=landlord_opts, multi=True, placeholder="Individual / Government ...")
                    ]),
                    html.Div(className="card", children=[
                        html.Div("Risk Band", className="card-title"),
                        dcc.Dropdown(id="f_risk", options=risk_opts, multi=True, placeholder="Low/Medium/High/Critical/Expired")
                    ]),
                    html.Div(className="card", children=[
                        html.Div("Quick Search", className="card-title"),
                        dcc.Input(id="f_text", type="text", placeholder="e.g. Kinshasa OR DRKN8818", style={"width": "100%"})
                    ]),
                ]),
                html.Div(style={"height": "10px"}),
                html.Div(id="exec_warning", className="note"),
                html.Div(style={"height": "10px"}),
                html.Div(id="kpi_row", className="kpis"),
                html.Div(style={"height": "12px"}),

                html.Div(style={"display": "grid", "gridTemplateColumns": "1.2fr 1fr 1fr", "gap": "12px"}, children=[
                    html.Div(className="card", children=[html.Div("Expiry Profile (Sites per Year)", className="card-title"),
                                                         dcc.Graph(id="g_expiry", style={"height": "320px"})]),
                    html.Div(className="card", children=[html.Div("Risk Band Mix", className="card-title"),
                                                         dcc.Graph(id="g_risk", style={"height": "320px"})]),
                    html.Div(className="card", children=[html.Div("Avg Current Rent (Monthly) by Region", className="card-title"),
                                                         dcc.Graph(id="g_rent_region", style={"height": "320px"})]),
                ]),

                html.Div(style={"height": "12px"}),

                html.Div(className="card", children=[
                    html.Div("Portfolio Table (Star Snapshot)", className="card-title"),
                    dash_table.DataTable(
                        id="tbl_sites",
                        columns=[{"name": c, "id": c} for c in snap_local.columns],
                        data=[],
                        page_size=15,
                        sort_action="native",
                        filter_action="native",
                        virtualization=True,
                        style_table={"overflowX": "auto", "height": "560px", "overflowY": "auto"},
                        style_cell={"fontSize": 12, "padding": "8px", "whiteSpace": "normal", "height": "auto"},
                        style_header={"backgroundColor": HT_BLUE, "color": "white", "fontWeight": "900"}
                    )
                ]),
            ])

        if tab == "tab_pay":
            return html.Div([
                html.Div(className="card", children=[
                    html.Div("Payments & Rent (Rent Step Fact Table)", className="card-title"),
                    html.Div("This tab uses fact_rent_step via vw_rent_steps.", className="note")
                ]),
                html.Div(style={"height": "12px"}),

                html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "12px"}, children=[
                    html.Div(className="card", children=[
                        html.Div("Top Regions by Avg Rent (Monthly)", className="card-title"),
                        dcc.Graph(id="g_pay_region", style={"height": "340px"})
                    ]),
                    html.Div(className="card", children=[
                        html.Div("Rent Trend Over Valid-From Date (Avg Yearly) — Top Regions", className="card-title"),
                        dcc.Graph(id="g_pay_trend", style={"height": "340px"})
                    ]),
                ]),

                html.Div(style={"height": "12px"}),

                html.Div(className="card", children=[
                    html.Div("Rent Steps Table (vw_rent_steps) – Top 20,000 rows loaded", className="card-title"),
                    dash_table.DataTable(
                        id="tbl_rent_steps",
                        columns=[],
                        data=[],
                        page_size=15,
                        sort_action="native",
                        filter_action="native",
                        virtualization=True,
                        style_table={"overflowX": "auto", "height": "620px", "overflowY": "auto"},
                        style_cell={"fontSize": 12, "padding": "8px", "whiteSpace": "normal", "height": "auto"},
                        style_header={"backgroundColor": HT_BLUE, "color": "white", "fontWeight": "900"}
                    )
                ])
            ])

        if tab == "tab_ai":
            return html.Div(style={"display": "grid", "gridTemplateColumns": "1.15fr 1fr", "gap": "12px"}, children=[
                html.Div(className="card", children=[
                    html.Div("AI Copilot (Star Schema Aware)", className="card-title"),
                    html.Div("AI queries vw_star_snapshot / vw_rent_steps.", className="note"),
                    dcc.Textarea(
                        id="ai_q",
                        style={"width": "100%", "height": "100px", "borderRadius": "12px", "padding": "10px",
                               "border": "1px solid rgba(11,46,109,0.15)"},
                        placeholder="Examples:\n- show sites where Current Rent (Monthly) >= 500\n- average Current Rent (Monthly) by Region Code\n- leases expiring in 2027 grouped by Risk Band\n- top landlords (derived) by total monthly rent"
                    ),
                    html.Div(style={"display": "flex", "gap": "10px", "marginTop": "10px"}, children=[
                        html.Button("Ask AI", id="ai_ask", n_clicks=0,
                                    style={"background": HT_ORANGE, "color": "white", "border": "none",
                                           "padding": "12px 16px", "borderRadius": "14px",
                                           "fontWeight": "900", "cursor": "pointer"}),
                        html.Button("Clear", id="ai_clear", n_clicks=0,
                                    style={"background": HT_BLUE, "color": "white", "border": "none",
                                           "padding": "12px 16px", "borderRadius": "14px",
                                           "fontWeight": "900", "cursor": "pointer"}),
                    ]),
                    html.Div(id="ai_status", className="note", style={"marginTop": "10px"}),
                    html.Hr(),
                    html.Div("Conversation", className="card-title"),
                    html.Div(id="ai_hist_view", style={"maxHeight": "460px", "overflowY": "auto"})
                ]),
                html.Div(className="card", children=[
                    html.Div("AI Results", className="card-title"),
                    dcc.Graph(id="ai_graph", style={"height": "320px"}),
                    html.Div("Table", className="card-title", style={"marginTop": "10px"}),
                    dash_table.DataTable(
                        id="ai_table",
                        columns=[],
                        data=[],
                        page_size=10,
                        sort_action="native",
                        filter_action="native",
                        style_table={"overflowX": "auto"},
                        style_cell={"fontSize": 12, "padding": "8px", "whiteSpace": "normal", "height": "auto"},
                        style_header={"backgroundColor": HT_BLUE, "color": "white", "fontWeight": "900"}
                    ),
                    html.Div("Generated SQL (SELECT only)", className="card-title", style={"marginTop": "10px"}),
                    html.Pre(
                        id="ai_sql",
                        style={"background": "#0b1220", "color": "#E5E7EB", "padding": "10px",
                               "borderRadius": "12px", "fontSize": "12px", "overflowX": "auto"}
                    ),
                ]),
            ])

        if tab == "tab_sql":
            fig = go.Figure(sql_state.get("fig", {}))
            return html.Div([
                html.Div(className="card", children=[
                    html.Div("SQL Studio (Enterprise Data Management)", className="card-title"),
                    html.Div(
                        "Run SELECT for analysis. WRITE allowed only on dim_contract_star / dim_contract / fact_rent_step / fact_condition / dim_landlord. "
                        "Writes create automatic DB backup then refresh Star Schema.",
                        className="note"
                    ),
                    html.Div(style={"height": "10px"}),
                    dcc.Textarea(
                        id="sql_text",
                        className="sqlbox",
                        value=sql_state.get("sql_text", ""),
                    ),
                    html.Div(style={"display": "flex", "gap": "10px", "marginTop": "10px", "flexWrap": "wrap"}, children=[
                        html.Button("Run SELECT", id="btn_sql_run", n_clicks=0,
                                    style={"background": HT_ORANGE, "color": "white", "border": "none", "padding": "10px 14px",
                                           "borderRadius": "12px", "fontWeight": "900"}),
                        html.Button("Run WRITE (Update/Insert/Delete)", id="btn_sql_write", n_clicks=0,
                                    style={"background": HT_BLUE, "color": "white", "border": "none", "padding": "10px 14px",
                                           "borderRadius": "12px", "fontWeight": "900"}),
                        dcc.Checklist(
                            id="chk_confirm_write",
                            options=[{"label": "I understand this changes the database", "value": "YES"}],
                            value=[],
                            style={"marginLeft": "8px", "fontSize": "12px"}
                        ),
                    ]),
                    html.Div(id="sql_status", className="note", style={"marginTop": "10px"},
                             children=sql_state.get("status", "Ready."))
                ]),
                html.Div(style={"height": "12px"}),

                html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "12px"}, children=[
                    html.Div(className="card", children=[
                        html.Div("SQL Results Chart (auto)", className="card-title"),
                        dcc.Graph(id="sql_graph", figure=fig, style={"height": "320px"})
                    ]),
                    html.Div(className="card", children=[
                        html.Div("Results Table", className="card-title"),
                        dash_table.DataTable(
                            id="sql_table",
                            columns=sql_state.get("columns", []),
                            data=sql_state.get("data", []),
                            page_size=12,
                            sort_action="native",
                            filter_action="native",
                            style_table={"overflowX": "auto", "height": "320px", "overflowY": "auto"},
                            style_cell={"fontSize": 12, "padding": "8px", "whiteSpace": "normal", "height": "auto"},
                            style_header={"backgroundColor": HT_BLUE, "color": "white", "fontWeight": "900"}
                        )
                    ]),
                ])
            ])

        # tab_model
        md = pd.read_sql_query("SELECT * FROM metadata_schema", conn)
        fig_model = relationship_diagram_figure()

        return html.Div([
            html.Div(className="card", children=[
                html.Div("Model View (Power BI–Style Relationships)", className="card-title"),
                dcc.Graph(figure=fig_model, style={"height": "560px"})
            ]),
            html.Div(style={"height": "12px"}),
            html.Div(className="card", children=[
                html.Div("Objects + Columns + Row Counts", className="card-title"),
                dash_table.DataTable(
                    data=md.to_dict("records"),
                    columns=[{"name": c, "id": c} for c in md.columns],
                    page_size=14,
                    filter_action="native",
                    sort_action="native",
                    style_table={"overflowX": "auto", "height": "420px", "overflowY": "auto"},
                    style_cell={"fontSize": 12, "padding": "8px", "whiteSpace": "normal", "height": "auto"},
                    style_header={"backgroundColor": HT_BLUE, "color": "white", "fontWeight": "900"}
                )
            ]),
        ])

    # ----------------- Executive callback -----------------
    @app.callback(
        Output("exec_warning", "children"),
        Output("kpi_row", "children"),
        Output("g_expiry", "figure"),
        Output("g_risk", "figure"),
        Output("g_rent_region", "figure"),
        Output("tbl_sites", "data"),
        Input("f_expiry", "value"),
        Input("f_region", "value"),
        Input("f_status", "value"),
        Input("f_landlord", "value"),
        Input("f_risk", "value"),
        Input("f_text", "value"),
        Input("store_refresh_tick", "data"),
        prevent_initial_call=False
    )
    def update_exec(expiry, region, status, landlord, risk, textq, _tick):
        df = load_snapshot()
        f = df.copy()

        if expiry:
            years = [int(x) for x in expiry if str(x).isdigit()]
            f = f[pd.to_numeric(f["Expiry Year"], errors="coerce").isin(years)]
        if region:
            f = f[f["Region Code"].astype(str).isin([str(x) for x in region])]
        if status:
            f = f[f["Lease Status"].astype(str).isin([str(x) for x in status])]
        if landlord:
            f = f[f["Landlord Type"].astype(str).isin([str(x) for x in landlord])]
        if risk:
            f = f[f["Risk Band"].astype(str).isin([str(x) for x in risk])]

        if textq and str(textq).strip():
            t = str(textq).strip().lower()
            mask = (
                f["Contract Name"].astype(str).str.lower().str.contains(t, na=False) |
                f["Site City"].astype(str).str.lower().str.contains(t, na=False) |
                f["Region Code"].astype(str).str.lower().str.contains(t, na=False) |
                f["Landlord Type"].astype(str).str.lower().str.contains(t, na=False) |
                f["Landlord Key"].astype(str).str.lower().str.contains(t, na=False)
            )
            f = f[mask]

        warn_txt = f"Filtered rows: {len(f):,} / {len(df):,}"
        warn = html.Div(warn_txt, className="note") if len(f) else html.Div(warn_txt + " — ⚠ filters removed all rows", className="warn")

        def kpi(label, value, bar_color=HT_ORANGE):
            return html.Div(className="kpi", children=[
                html.Div(label, className="kpi-label"),
                html.Div(value, className="kpi-value"),
                html.Div(className="kpi-bar", children=[html.Div(style={"width": "62%", "background": bar_color})])
            ])

        avg_m = pd.to_numeric(f["Current Rent (Monthly)"], errors="coerce").mean()
        avg_y = pd.to_numeric(f["Current Rent (Yearly)"], errors="coerce").mean()
        avg_commit = pd.to_numeric(f["Years to End"], errors="coerce").mean()

        exp12 = int((pd.to_numeric(f["Years to End"], errors="coerce") <= 1).sum())
        exp24 = int((pd.to_numeric(f["Years to End"], errors="coerce") <= 2).sum())
        critical = int((f["Risk Band"].astype(str) == "Critical").sum())

        kpis = [
            kpi("Total Sites (Filtered)", f"{len(f):,}", HT_ORANGE),
            kpi("Avg Rent (Monthly)", f"{avg_m:,.2f}" if pd.notna(avg_m) else "N/A", HT_BLUE),
            kpi("Avg Rent (Yearly)", f"{avg_y:,.2f}" if pd.notna(avg_y) else "N/A", HT_BLUE2),
            kpi("Avg Years to End", f"{avg_commit:,.2f}" if pd.notna(avg_commit) else "N/A", "#22C55E"),
            kpi("Expiring ≤ 12 months", f"{exp12:,}", "#F59E0B"),
            kpi("Critical Risk Sites", f"{critical:,}  |  Expiring ≤24m: {exp24:,}", "#EF4444"),
        ]

        fig_exp = go.Figure()
        tmp = f.dropna(subset=["Contract end date"]).copy()
        if len(tmp):
            tmp["Year"] = pd.to_datetime(tmp["Contract end date"], errors="coerce").dt.year
            grp = tmp["Year"].value_counts().sort_index().reset_index()
            grp.columns = ["Year", "Sites"]
            fig_exp = px.bar(grp, x="Year", y="Sites")
            fig_exp.update_traces(marker_color=HT_ORANGE)
        fig_exp.update_layout(paper_bgcolor="white", plot_bgcolor="white", margin=dict(l=30, r=20, t=20, b=40))

        fig_risk = go.Figure()
        if len(f):
            grp = f["Risk Band"].fillna("Unknown").value_counts().reset_index()
            grp.columns = ["Risk Band", "Count"]
            fig_risk = px.pie(grp, names="Risk Band", values="Count")
        fig_risk.update_layout(paper_bgcolor="white", margin=dict(l=20, r=20, t=20, b=20))

        fig_rent = go.Figure()
        tmp2 = f.copy()
        tmp2["Current Rent (Monthly)"] = pd.to_numeric(tmp2["Current Rent (Monthly)"], errors="coerce")
        tmp2 = tmp2.dropna(subset=["Region Code", "Current Rent (Monthly)"])
        if len(tmp2):
            grp = tmp2.groupby("Region Code", as_index=False)["Current Rent (Monthly)"].mean().sort_values("Current Rent (Monthly)", ascending=False).head(20)
            fig_rent = px.bar(grp, x="Region Code", y="Current Rent (Monthly)")
            fig_rent.update_traces(marker_color=HT_BLUE)
        fig_rent.update_layout(paper_bgcolor="white", plot_bgcolor="white", margin=dict(l=30, r=20, t=20, b=40))

        return warn, kpis, fig_exp, fig_risk, fig_rent, f.head(5000).to_dict("records")

    # ----------------- Payments tab callback -----------------
    @app.callback(
        Output("g_pay_region", "figure"),
        Output("g_pay_trend", "figure"),
        Output("tbl_rent_steps", "data"),
        Output("tbl_rent_steps", "columns"),
        Input("tabs", "value"),
        Input("store_refresh_tick", "data"),
        prevent_initial_call=False
    )
    def update_payments(tab, _tick):
        if tab != "tab_pay":
            return no_update, no_update, no_update, no_update

        rs = load_rent_steps(limit=20000)

        fig1 = go.Figure()
        if len(rs):
            rs["Rent (Monthly)"] = pd.to_numeric(rs["Rent (Monthly)"], errors="coerce")
            grp = rs.dropna(subset=["Region Code", "Rent (Monthly)"]).groupby("Region Code", as_index=False)["Rent (Monthly)"].mean()
            grp = grp.sort_values("Rent (Monthly)", ascending=False).head(20)
            fig1 = px.bar(grp, x="Region Code", y="Rent (Monthly)")
            fig1.update_traces(marker_color=HT_ORANGE)
        fig1.update_layout(paper_bgcolor="white", plot_bgcolor="white", margin=dict(l=30, r=20, t=20, b=40))

        fig2 = go.Figure()
        if len(rs):
            rs["Valid From"] = pd.to_datetime(rs["Valid From"], errors="coerce")
            rs["Rent (Yearly)"] = pd.to_numeric(rs["Rent (Yearly)"], errors="coerce")
            j = rs.dropna(subset=["Valid From", "Rent (Yearly)", "Region Code"]).copy()
            if len(j):
                j["YearMonth"] = j["Valid From"].dt.to_period("M").dt.to_timestamp()
                top = j["Region Code"].value_counts().head(8).index.tolist()
                j = j[j["Region Code"].isin(top)]
                grp2 = j.groupby(["YearMonth", "Region Code"], as_index=False)["Rent (Yearly)"].mean()
                fig2 = px.line(grp2, x="YearMonth", y="Rent (Yearly)", color="Region Code", markers=True)
                fig2.update_layout(legend=dict(orientation="h", y=-0.25))
        fig2.update_layout(paper_bgcolor="white", plot_bgcolor="white", margin=dict(l=30, r=20, t=20, b=40))

        cols = [{"name": c, "id": c} for c in rs.columns]
        return fig1, fig2, rs.to_dict("records"), cols

    # ----------------- AI callbacks -----------------
    @app.callback(
        Output("store_ai_hist", "data"),
        Output("ai_hist_view", "children"),
        Output("ai_graph", "figure"),
        Output("ai_table", "data"),
        Output("ai_table", "columns"),
        Output("ai_sql", "children"),
        Output("ai_status", "children"),
        Input("ai_ask", "n_clicks"),
        Input("ai_clear", "n_clicks"),
        State("ai_q", "value"),
        State("store_ai_hist", "data"),
        prevent_initial_call=True
    )
    def ai_cb(n_ask, n_clear, q, hist):
        trig = (callback_context.triggered[0]["prop_id"].split(".")[0] if callback_context.triggered else "")
        hist = hist or []

        if trig == "ai_clear":
            fig = go.Figure()
            fig.update_layout(title="Cleared", paper_bgcolor="white", plot_bgcolor="white")
            return [], [], fig, [], [], "--", "✅ Cleared."

        if not q or not str(q).strip():
            return no_update, no_update, no_update, no_update, no_update, no_update, "Type a question first."

        plan = ai_plan(str(q).strip())
        sql = (plan.get("sql") or "").strip()
        answer = plan.get("answer", "")
        chart_spec = plan.get("chart", {"type": "none", "x": "", "y": "", "title": ""})

        df = pd.DataFrame()
        if sql:
            try:
                df = pd.read_sql_query(sql, conn)
            except Exception as e:
                answer = (answer + f"\n\nSQL failed: {e}").strip()
                sql = ""

        fig = chart_from_df(df, chart_spec)
        table_data = df.head(500).to_dict("records") if len(df) else []
        table_cols = [{"name": c, "id": c} for c in df.columns] if len(df.columns) else []

        hist.append({"q": q, "a": answer, "sql": sql})

        view = []
        for h in hist[-25:]:
            view.append(html.Div(
                style={"marginBottom": "12px", "paddingBottom": "10px", "borderBottom": "1px solid rgba(11,46,109,0.08)"},
                children=[
                    html.Div("You", className="note"),
                    html.Div(h["q"]),
                    html.Div("AI", className="note", style={"marginTop": "6px"}),
                    html.Div(h["a"], style={"whiteSpace": "pre-wrap"}),
                ]
            ))

        return hist, view, fig, table_data, table_cols, (sql if sql else "-- no SQL generated --"), "✅ Done."

    # ----------------- SQL Studio callback (FIXED) -----------------
    @app.callback(
        Output("sql_status", "children"),
        Output("sql_graph", "figure"),
        Output("sql_table", "data"),
        Output("sql_table", "columns"),
        Output("store_refresh_tick", "data"),
        Output("store_sql_state", "data"),
        Input("btn_sql_run", "n_clicks"),
        Input("btn_sql_write", "n_clicks"),
        State("sql_text", "value"),
        State("chk_confirm_write", "value"),
        State("store_refresh_tick", "data"),
        State("store_sql_state", "data"),
        prevent_initial_call=True
    )
    def sql_cb(n_run, n_write, sql_text, confirm, tick, sql_state):
        trig = (callback_context.triggered[0]["prop_id"].split(".")[0] if callback_context.triggered else "")
        tick = tick or 0
        sql_state = sql_state or {}

        sql = (sql_text or "").strip()
        if not sql:
            fig = go.Figure(); fig.update_layout(title="No SQL", paper_bgcolor="white", plot_bgcolor="white")
            new_state = {"sql_text": sql_text or "", "status": "Type SQL first.", "fig": fig.to_dict(), "data": [], "columns": []}
            return "Type SQL first.", fig, [], [], tick, new_state

        # Always persist current sql_text so it doesn't reset
        base_state = dict(sql_state)
        base_state["sql_text"] = sql_text or ""

        if trig == "btn_sql_run":
            if not is_safe_select(sql):
                fig = go.Figure(); fig.update_layout(title="Blocked", paper_bgcolor="white", plot_bgcolor="white")
                status = "❌ Only SELECT allowed with Run SELECT."
                new_state = {"sql_text": base_state["sql_text"], "status": status, "fig": fig.to_dict(), "data": [], "columns": []}
                return status, fig, [], [], tick, new_state

            t0 = datetime.now()
            try:
                df = pd.read_sql_query(sql, conn)
            except Exception as e:
                fig = go.Figure(); fig.update_layout(title="SQL error", paper_bgcolor="white", plot_bgcolor="white")
                status = f"❌ SQL failed: {e}"
                new_state = {"sql_text": base_state["sql_text"], "status": status, "fig": fig.to_dict(), "data": [], "columns": []}
                return status, fig, [], [], tick, new_state

            dt = (datetime.now() - t0).total_seconds()
            cols = [{"name": c, "id": c} for c in df.columns]
            data = df.head(2000).to_dict("records")

            fig = go.Figure()
            if len(df.columns) >= 2:
                x = df.columns[0]
                y = df.columns[1]
                yy = pd.to_numeric(df[y], errors="coerce")
                if yy.notna().any():
                    tmp = df.copy()
                    tmp[y] = yy
                    fig = px.bar(tmp.head(50), x=x, y=y)
                    fig.update_traces(marker_color=HT_ORANGE)
                else:
                    fig.update_layout(title="No numeric column for chart", paper_bgcolor="white", plot_bgcolor="white")

            fig.update_layout(paper_bgcolor="white", plot_bgcolor="white", margin=dict(l=30, r=20, t=30, b=40))
            status = f"✅ SELECT OK. Rows: {len(df):,} (showing up to 2,000). Time: {dt:.2f}s"

            new_state = {"sql_text": base_state["sql_text"], "status": status, "fig": fig.to_dict(), "data": data, "columns": cols}
            return status, fig, data, cols, tick, new_state

        if trig == "btn_sql_write":
            if "YES" not in (confirm or []):
                fig = go.Figure(); fig.update_layout(title="Write not confirmed", paper_bgcolor="white", plot_bgcolor="white")
                status = "❌ Tick confirmation checkbox before WRITE."
                new_state = {"sql_text": base_state["sql_text"], "status": status, "fig": fig.to_dict(), "data": [], "columns": []}
                return status, fig, [], [], tick, new_state

            if not is_allowed_write(sql):
                fig = go.Figure(); fig.update_layout(title="Write blocked", paper_bgcolor="white", plot_bgcolor="white")
                status = "❌ WRITE blocked. Allowed only on dim_contract_star/dim_contract/fact_rent_step/fact_condition/dim_landlord."
                new_state = {"sql_text": base_state["sql_text"], "status": status, "fig": fig.to_dict(), "data": [], "columns": []}
                return status, fig, [], [], tick, new_state

            bkp = backup_db(db_path)
            t0 = datetime.now()
            try:
                cur = conn.cursor()
                cur.execute("BEGIN")
                cur.executescript(sql)
                conn.commit()
            except Exception as e:
                conn.rollback()
                fig = go.Figure(); fig.update_layout(title="Write failed", paper_bgcolor="white", plot_bgcolor="white")
                status = f"❌ WRITE failed: {e}. Backup created: {os.path.basename(bkp)}"
                new_state = {"sql_text": base_state["sql_text"], "status": status, "fig": fig.to_dict(), "data": [], "columns": []}
                return status, fig, [], [], tick, new_state

            refresh_star_after_write(conn)
            tick += 1
            dt = (datetime.now() - t0).total_seconds()
            fig = go.Figure(); fig.update_layout(title="Write OK", paper_bgcolor="white", plot_bgcolor="white")
            status = f"✅ WRITE OK. Backup: {os.path.basename(bkp)}. Refreshed Star Schema. Time: {dt:.2f}s"

            new_state = {"sql_text": base_state["sql_text"], "status": status, "fig": fig.to_dict(), "data": [], "columns": []}
            return status, fig, [], [], tick, new_state

        fig = go.Figure(); fig.update_layout(title="Idle", paper_bgcolor="white", plot_bgcolor="white")
        status = "Idle."
        new_state = {"sql_text": base_state["sql_text"], "status": status, "fig": fig.to_dict(), "data": [], "columns": []}
        return status, fig, [], [], tick, new_state

    return app


# =============================================================================
# MAIN
# =============================================================================
def main():
    project_dir = SCRIPT_DIR

    contracts_path = os.path.join(project_dir, CONTRACTS_FILE)
    conditions_path = os.path.join(project_dir, CONDITIONS_FILE)

    if not os.path.exists(contracts_path) or not os.path.exists(conditions_path):
        raise FileNotFoundError(
            "Put this script in the SAME folder as Contracts.csv and Conditions.csv.\n"
            f"Current folder: {project_dir}"
        )

    conn, db_path = build_database(project_dir)
    app = build_app(conn, db_path)

    if OPEN_BROWSER:
        def _open():
            webbrowser.open_new(f"http://127.0.0.1:{APP_PORT}")
        threading.Timer(1.0, _open).start()

    # Dash 2.14+: run()
    app.run(debug=False, port=APP_PORT)


if __name__ == "__main__":
    main()
