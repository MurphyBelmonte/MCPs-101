from typing import Optional, Dict, List, Literal
from dataclasses import dataclass, field
from pathlib import Path
import os, math, re
import pandas as pd
from mcp.server.fastmcp import FastMCP

# If you already have a shared FastMCP instance, import it:
from app import mcp


# =========================
# Config & State
# =========================
@dataclass
class Schema:
    invoice_id: Optional[str] = None
    date: Optional[str] = None
    quantity: Optional[str] = None
    unit_price: Optional[str] = None
    line_total: Optional[str] = None
    customer: Optional[str] = None
    country: Optional[str] = None
    description: Optional[str] = None
    # internal note: we use normalized lowercase column names

@dataclass
class DataState:
    base_dir: Path = Path.home() / "Documents"
    path: Optional[Path] = None
    df: Optional[pd.DataFrame] = None
    last_mtime: Optional[float] = None
    schema: Schema = field(default_factory=Schema)
    sheet_name: Optional[str] = None

ds = DataState()

# =========================
# Helpers
# =========================
def _norm(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[\s\-_]+", " ", s)
    s = s.replace(".", "").replace("#","")
    return s

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_norm(c) for c in df.columns]
    return df

def _is_within_base(p: Path) -> bool:
    try:
        p.resolve().relative_to(ds.base_dir.resolve())
        return True
    except Exception:
        return False

def _walk(base: Path):
    for root, _, files in os.walk(base):
        for f in files:
            yield Path(root) / f

def _load_any(path: Path) -> pd.DataFrame:
    # Excel (try first sheet that looks tabular if sheet unknown)
    if path.suffix.lower() in {".xlsx",".xls"}:
        x = pd.ExcelFile(path)
        # Try the sheet that has the “richest” matching schema
        best_score, best_df, best_sheet = -1, None, None
        for sh in x.sheet_names:
            try:
                df_ = pd.read_excel(x, sheet_name=sh)
                df_ = _norm_cols(df_)
                score = _score_schema(df_.columns)
                if score > best_score:
                    best_score, best_df, best_sheet = score, df_, sh
            except Exception:
                continue
        if best_df is None:
            # fallback: first sheet
            best_df = _norm_cols(pd.read_excel(x, sheet_name=0))
            best_sheet = x.sheet_names[0]
        ds.sheet_name = best_sheet
        return best_df

    # CSV
    if path.suffix.lower() == ".csv":
        # let pandas sniff; user’s file must have headers
        df = pd.read_csv(path)
        return _norm_cols(df)

    raise ValueError("Unsupported file type. Use .xlsx/.xls/.csv")

# ------- schema inference -------
SYNONYMS = {
    "invoice_id":  ["invoice no","invoiceno","invoice","invoice number","orderid","order id","order no","billno","bill no","inv no","invno","document number"],
    "date":        ["invoicedate","invoice date","date","order date","document date","posting date"],
    "quantity":    ["quantity","qty","qty.","qnty","units","count"],
    "unit_price":  ["unitprice","unit price","price","rate","unit cost","cost"],
    "line_total":  ["linetotal","line total","amount","total","value","net amount","gross amount","subtotal"],
    "customer":    ["customerid","customer id","customer","client","account","buyer","party","sold-to","sold to","customer code","customer no"],
    "country":     ["country","region","market"],
    "description": ["description","item","product","sku name","name","details"],
}

def _score_schema(cols: List[str]) -> int:
    # simple score: count how many synonym groups have at least one match
    score = 0
    for key, syns in SYNONYMS.items():
        for s in syns:
            if s in cols:
                score += 1
                break
    return score

def _choose_best(cols: List[str], keys: List[str]) -> Optional[str]:
    # choose the first synonym that appears
    for k in keys:
        if k in cols:
            return k
    return None

def _infer_schema(df: pd.DataFrame) -> Schema:
    cols = list(df.columns)
    sch = Schema()
    sch.invoice_id = _choose_best(cols, SYNONYMS["invoice_id"])
    sch.date       = _choose_best(cols, SYNONYMS["date"])
    sch.quantity   = _choose_best(cols, SYNONYMS["quantity"])
    sch.unit_price = _choose_best(cols, SYNONYMS["unit_price"])
    sch.line_total = _choose_best(cols, SYNONYMS["line_total"])
    sch.customer   = _choose_best(cols, SYNONYMS["customer"])
    sch.country    = _choose_best(cols, SYNONYMS["country"])
    sch.description= _choose_best(cols, SYNONYMS["description"])
    return sch

def _ensure_loaded():
    if ds.path is None:
        raise ValueError("No data source set. Call set_data_source or search_files first.")
    if not ds.path.exists():
        raise FileNotFoundError(f"File not found: {ds.path}")
    mtime = ds.path.stat().st_mtime
    if ds.df is not None and ds.last_mtime == mtime:
        return
    df = _load_any(ds.path)

    # Try convert likely date column
    sch = _infer_schema(df)
    if sch.date and sch.date in df.columns:
        df[sch.date] = pd.to_datetime(df[sch.date], errors="coerce")

    # Try build a computed line_total if missing and qty+unit present
    if not sch.line_total and sch.quantity and sch.unit_price:
        try:
            df["__computed_total__"] = pd.to_numeric(df[sch.quantity], errors="coerce") * pd.to_numeric(df[sch.unit_price], errors="coerce")
            sch.line_total = "__computed_total__"
        except Exception:
            pass

    ds.df = df
    ds.last_mtime = mtime
    ds.schema = sch

def _month_bounds(month: str):
    start = pd.to_datetime(month + "-01", errors="raise")
    end = start + pd.offsets.MonthEnd(1)
    return start, end

def _date_range_bounds(date_range: str):
    if ".." in date_range:
        a,b = date_range.split("..",1)
        a1,_ = _month_bounds(a)
        b1,b2 = _month_bounds(b)
        return a1,b2
    else:
        return _month_bounds(date_range)

# =========================
# Tools
# =========================

@mcp.tool()
def set_base_dir(dir: str) -> str:
    """
    Restrict file search & reading to this directory (and its children).
    Example: C:\\Users\\Hi\\Documents\\AccountingData
    """
    p = Path(dir).expanduser().resolve()
    if not p.exists() or not p.is_dir():
        raise ValueError("Directory not found or not a folder.")
    ds.base_dir = p
    return f"Base directory set to: {ds.base_dir}"

@mcp.tool()
def search_files(
    name: str,
    extensions: Optional[List[str]] = None,
    max_results: int = 25,
    fuzzy: bool = True
) -> List[Dict]:
    """
    Search under base_dir for files by name.
    - name: supports '*' '?' wildcards; if fuzzy=True and no wildcard given, matches '*name*'
    - extensions: like ['.xlsx','.csv']
    """
    base = ds.base_dir
    if not base.exists():
        raise FileNotFoundError(f"Base dir does not exist: {base}")

    extset = None
    if extensions:
        extset = {e.lower() if e.startswith(".") else "."+e.lower() for e in extensions}

    pattern = name
    if fuzzy and not any(ch in name for ch in "*?"):
        pattern = f".*{re.escape(name)}.*"

    rx = re.compile(pattern, re.IGNORECASE)
    out = []
    for p in _walk(base):
        if extset and p.suffix.lower() not in extset:
            continue
        if rx.search(p.name):
            out.append({"path": str(p.resolve()), "name": p.name, "size_bytes": p.stat().st_size})
            if len(out) >= max_results:
                break
    return out

@mcp.tool()
def set_data_source(path: str) -> Dict:
    """
    Point the server at a specific Excel/CSV file inside base_dir.
    Returns detected sheet (if Excel) and the inferred schema.
    """
    p = Path(path).expanduser().resolve()
    if not _is_within_base(p):
        raise PermissionError("Path is outside the allowed base_dir. Use set_base_dir first if needed.")
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"Not found: {p}")

    ds.path = p
    ds.df = None
    ds.last_mtime = None
    _ensure_loaded()
    sch = ds.schema.__dict__.copy()
    return {"path": str(ds.path), "sheet": ds.sheet_name, "schema": sch, "rows": int(len(ds.df))}

@mcp.tool()
def show_detected_schema() -> Dict:
    """Return the current inferred schema and basic info."""
    _ensure_loaded()
    return {
        "path": str(ds.path),
        "sheet": ds.sheet_name,
        "schema": ds.schema.__dict__,
        "columns": list(ds.df.columns),
        "row_count": int(len(ds.df)),
    }

@mcp.tool()
def override_schema(mapping: Dict[str,str]) -> Dict:
    """
    Manually override any of: invoice_id, date, quantity, unit_price, line_total, customer, country, description.
    Provide column names EXACTLY as they appear in show_detected_schema().columns.
    Example: {"invoice_id":"invoice", "unit_price":"price"}
    """
    _ensure_loaded()
    valid = set(ds.df.columns)
    sch = ds.schema
    for k,v in mapping.items():
        if k not in sch.__dict__:
            raise ValueError(f"Unknown schema key: {k}")
        if v is not None and v not in valid:
            raise ValueError(f"Column '{v}' not found in file. Available: {sorted(valid)[:20]}...")
        setattr(sch, k, v)
    ds.schema = sch
    return {"ok": True, "schema": ds.schema.__dict__}

@mcp.tool()
def list_months(limit: int = 24) -> List[str]:
    """List distinct months 'YYYY-MM' present in the detected date column."""
    _ensure_loaded()
    sch = ds.schema
    if not sch.date or sch.date not in ds.df.columns:
        return []
    months = (ds.df[sch.date].dt.to_period("M").astype(str)).dropna().unique().tolist()
    months = sorted(months, reverse=True)
    return months[:limit]

@mcp.tool()
def get_invoices(
    date_range: Optional[str] = None,
    customer: Optional[str] = None,   # works with text or numeric ids coerced to string
    include_returns: bool = True,
    max_results: int = 200
) -> List[Dict]:
    """
    Aggregate lines into invoices using the inferred schema.
    - date_range: 'YYYY-MM' or 'YYYY-MM..YYYY-MM'
    - customer: optional equals filter on the mapped customer column (as string compare)
    - include_returns: if False and quantity is available, excludes rows with quantity < 0
    Returns: [{invoice_id, invoice_date, customer, total_amount, line_count, country?}]
    """
    _ensure_loaded()
    sch = ds.schema
    df = ds.df.copy()

    # filters
    if not include_returns and sch.quantity and sch.quantity in df.columns:
        df = df[pd.to_numeric(df[sch.quantity], errors="coerce").fillna(0) >= 0]

    if date_range and sch.date and sch.date in df.columns:
        start,end = _date_range_bounds(date_range)
        df = df[(df[sch.date] >= start) & (df[sch.date] <= end)]

    if customer and sch.customer and sch.customer in df.columns:
        df = df[df[sch.customer].astype(str) == str(customer)]

    if df.empty:
        return []

    # pick totals
    total_col = sch.line_total
    if total_col and total_col in df.columns:
        totals = pd.to_numeric(df[total_col], errors="coerce")
    else:
        if sch.quantity and sch.unit_price and sch.quantity in df.columns and sch.unit_price in df.columns:
            totals = pd.to_numeric(df[sch.quantity], errors="coerce") * pd.to_numeric(df[sch.unit_price], errors="coerce")
        else:
            # last resort: count lines
            totals = pd.Series(1.0, index=df.index)

    date_for_invoice = None
    if sch.date and sch.date in df.columns:
        date_for_invoice = sch.date

    group_key = sch.invoice_id if sch.invoice_id in df.columns else None
    if not group_key:
        # no invoice id – treat each row uniquely (still return something)
        df["__rowid__"] = range(len(df))
        group_key = "__rowid__"

    agg = {
        "total_amount": totals.groupby(df[group_key]).sum(),
        "line_count": df.groupby(group_key).size(),
    }

    out = pd.DataFrame(agg["total_amount"]).join(agg["line_count"])
    out = out.reset_index().rename(columns={"index": "invoice_id"})

    # add min date per invoice if available
    if date_for_invoice:
        mindate = df.groupby(group_key)[date_for_invoice].min().astype("datetime64[ns]")
        out = out.join(mindate.rename("invoice_date"), on=group_key)
        out["invoice_date"] = out["invoice_date"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # add a sample customer/country if present
    if sch.customer and sch.customer in df.columns:
        cust = df.groupby(group_key)[sch.customer].first()
        out = out.join(cust.rename("customer"), on=group_key)
    if sch.country and sch.country in df.columns:
        ctry = df.groupby(group_key)[sch.country].first()
        out = out.join(ctry.rename("country"), on=group_key)

    # rename invoice_id column if group_key != 'invoice_id'
    if group_key != "invoice_id":
        out = out.rename(columns={group_key: "invoice_id"})

    # order by invoice_date desc if present, else by total desc
    if "invoice_date" in out.columns:
        out = out.sort_values("invoice_date", ascending=False)
    else:
        out = out.sort_values("total_amount", ascending=False)

    return out.head(max_results).to_dict(orient="records")

@mcp.tool()
def invoice_lines(invoice_id: str) -> List[Dict]:
    """
    Return detail lines for one invoice id (or row surrogate if no invoice id column exists).
    """
    _ensure_loaded()
    sch = ds.schema
    df = ds.df

    key = sch.invoice_id if sch.invoice_id in df.columns else None
    if not key:
        # fall back to returning the first N rows (no invoice notion)
        return df.head(50).to_dict(orient="records")

    sub = df[df[key].astype(str) == str(invoice_id)].copy()
    if sub.empty:
        return []

    # enrich with computed total if necessary
    if sch.line_total and sch.line_total in sub.columns:
        pass
    elif sch.quantity and sch.unit_price and sch.quantity in sub.columns and sch.unit_price in sub.columns:
        sub["__computed_total__"] = pd.to_numeric(sub[sch.quantity], errors="coerce") * pd.to_numeric(sub[sch.unit_price], errors="coerce")

    # pretty date
    if sch.date and sch.date in sub.columns:
        sub[sch.date] = pd.to_datetime(sub[sch.date], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    # choose a useful subset to show
    candidate_cols = [sch.description, sch.quantity, sch.unit_price, sch.line_total or "__computed_total__",
                      sch.date, sch.customer, sch.country]
    chosen = [c for c in candidate_cols if c and c in sub.columns]
    if not chosen:
        chosen = list(sub.columns)[:10]

    return sub[chosen].to_dict(orient="records")

@mcp.tool()
def summarize_transactions(
    month: str,
    include_returns: bool = True,
    top_n_clients: int = 5
) -> Dict:
    """
    Summarize sales for YYYY-MM using detected schema.
    - If a line_total column exists, we sum it; else we compute qty * unit_price.
    - If include_returns=False and quantity exists, we exclude rows with quantity < 0.
    - Returns top customers by sales (if a customer column exists).
    """
    _ensure_loaded()
    sch = ds.schema
    df = ds.df.copy()

    if not sch.date or sch.date not in df.columns:
        return {"month": month, "message": "No usable date column detected."}

    start,end = _month_bounds(month)
    df = df[(df[sch.date] >= start) & (df[sch.date] <= end)]

    if not include_returns and sch.quantity and sch.quantity in df.columns:
        df = df[pd.to_numeric(df[sch.quantity], errors="coerce").fillna(0) >= 0]

    if df.empty:
        return {"month": month, "revenue": 0.0, "top_clients": [], "message": "No data for this month."}

    # totals
    if sch.line_total and sch.line_total in df.columns:
        totals = pd.to_numeric(df[sch.line_total], errors="coerce").fillna(0.0)
    elif sch.quantity and sch.unit_price and sch.quantity in df.columns and sch.unit_price in df.columns:
        totals = pd.to_numeric(df[sch.quantity], errors="coerce").fillna(0.0) * pd.to_numeric(df[sch.unit_price], errors="coerce").fillna(0.0)
    else:
        totals = pd.Series(1.0, index=df.index)  # not ideal, but returns something

    revenue = float(totals.sum())

    # top clients
    top_clients: List[Dict] = []
    if sch.customer and sch.customer in df.columns:
        tmp = df.copy()
        tmp["__totals__"] = totals
        grp = tmp.groupby(sch.customer)["__totals__"].sum().sort_values(ascending=False).head(top_n_clients)
        for k, v in grp.items():
            top_clients.append({"customer": str(k), "total": float(v)})

    natural = f"For {month}, revenue ${revenue:,.2f}."
    if top_clients:
        natural += " Top clients: " + ", ".join([f"{c['customer']} (${c['total']:,.2f})" for c in top_clients])

    return {
        "month": month,
        "revenue": revenue,
        "expenses": None,
        "profit": None,
        "top_clients": top_clients,
        "natural_language": natural
    }
