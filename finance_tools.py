from typing import Optional, Literal, List, Dict
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
from app import mcp

# If you already have a shared FastMCP instance in app.py, import it instead:
# from app import mcp
# =========================
# Data source and loader
# =========================
@dataclass
class RetailData:
    path: Optional[Path] = None
    df: Optional[pd.DataFrame] = None
    last_loaded_mtime: Optional[float] = None

ds = RetailData()

def _try_load():
    if ds.path is None:
        raise ValueError("No data source set. Call set_data_source(file_path) first.")

    p = ds.path
    if not p.exists():
        raise FileNotFoundError(f"Data source not found: {p}")

    mtime = p.stat().st_mtime
    if ds.last_loaded_mtime and mtime == ds.last_loaded_mtime:
        return

    # Expect the sheet name "Online Retail"
    df = pd.read_excel(p, sheet_name="Online Retail", dtype={
        "InvoiceNo": str,           # treat as string (some have leading zeros/cancellations)
        "StockCode": str,
        "Description": str,
        "Quantity": "Int64",        # allow NA
        "UnitPrice": "float",
        "CustomerID": "Int64",      # allow NA
        "Country": str
    })

    # Basic normalization
    # Parse dates
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")

    # Clean weird/blank invoice numbers
    df = df.dropna(subset=["InvoiceNo", "InvoiceDate", "UnitPrice", "Quantity"])

    # Compute line total (Quantity * UnitPrice); returns with negative QTY will reduce revenue
    df["LineTotal"] = df["Quantity"].astype("float") * df["UnitPrice"].astype("float")

    # Cache
    ds.df = df
    ds.last_loaded_mtime = mtime


# =========================
# Helper functions
# =========================
def _month_bounds(month: str):
    """month='YYYY-MM' -> (start_ts, end_ts) covering that month."""
    start = pd.to_datetime(month + "-01", errors="raise")
    end = start + pd.offsets.MonthEnd(1)
    return start, end

def _range_bounds(date_range: str):
    """
    Supported:
      - 'YYYY-MM' (single month)
      - 'YYYY-MM..YYYY-MM' inclusive range of months
    """
    if ".." in date_range:
        a, b = date_range.split("..", 1)
        a_start, _ = _month_bounds(a)
        b_start, b_end = _month_bounds(b)
        return a_start, b_end
    else:
        return _month_bounds(date_range)


# =========================
# Tools
# =========================
@mcp.tool()
def set_data_source(file_path: str) -> str:
    """
    Set the path to 'Online Retail.xlsx' (UCI dataset with sheet 'Online Retail').
    Example: C:\\path\\to\\Online Retail.xlsx
    """
    path = Path(file_path).expanduser().resolve()
    ds.path = path
    _try_load()
    return f"Data source set to: {ds.path}"

@mcp.tool()
def list_months(limit: int = 24) -> List[str]:
    """
    Return up to 'limit' distinct months present in the data as 'YYYY-MM', newest first.
    Helps users know what months they can query.
    """
    _try_load()
    df = ds.df
    months = (df["InvoiceDate"].dt.to_period("M").astype(str)).dropna().unique().tolist()
    # Sort descending
    months = sorted(months, reverse=True)
    return months[:limit]

@mcp.tool()
def get_invoices(
    date_range: Optional[str] = None,
    customer_id: Optional[int] = None,
    include_returns: bool = True,
    max_results: int = 200
) -> List[Dict]:
    """
    Aggregate line items into invoices.
    - date_range: 'YYYY-MM' or 'YYYY-MM..YYYY-MM'. If None, returns recent invoices (up to max_results).
    - customer_id: optional filter (e.g., 17850)
    - include_returns: if False, exclude lines where Quantity < 0
    - Returns: [{invoice_no, customer_id, country, invoice_date, total_amount, line_count}]
    """
    _try_load()
    df = ds.df.copy()

    # Filter by returns if requested
    if not include_returns:
        df = df[df["Quantity"] >= 0]

    # Filter by date range
    if date_range:
        start, end = _range_bounds(date_range)
        df = df[(df["InvoiceDate"] >= start) & (df["InvoiceDate"] <= end)]

    # Filter by customer
    if customer_id is not None:
        df = df[df["CustomerID"] == customer_id]

    if df.empty:
        return []

    # Group by invoice
    grp = df.groupby("InvoiceNo", dropna=True)
    inv = grp.agg(
        total_amount=("LineTotal", "sum"),
        invoice_date=("InvoiceDate", "min"),
        customer_id=("CustomerID", "first"),
        country=("Country", "first"),
        line_count=("InvoiceNo", "count")
    ).reset_index()

    # Sort newest first
    inv = inv.sort_values("invoice_date", ascending=False)

    # Format and cap
    inv["invoice_date"] = inv["invoice_date"].dt.strftime("%Y-%m-%d %H:%M:%S")
    out = inv.head(max_results).to_dict(orient="records")
    return out

@mcp.tool()
def summarize_transactions(
    month: str,
    top_n_clients: int = 5,
    include_returns: bool = True
) -> Dict:
    """
    Summarize revenue for a given month (YYYY-MM).
    - Revenue = sum(Quantity * UnitPrice) for that month.
      If include_returns=False, negative quantities (returns) are excluded.
    - Returns: revenue, (no expenses available in this dataset), and top clients by sales.
    """
    _try_load()
    df = ds.df.copy()

    # Month window
    start, end = _month_bounds(month)
    mask = (df["InvoiceDate"] >= start) & (df["InvoiceDate"] <= end)
    dfm = df.loc[mask]

    if not include_returns:
        dfm = dfm[dfm["Quantity"] >= 0]

    revenue = float(dfm["LineTotal"].sum()) if not dfm.empty else 0.0

    # Top clients by total sales (CustomerID may be NaN -> treat as unknown)
    if dfm.empty:
        top_clients = []
    else:
        tmp = dfm.copy()
        tmp["CustomerID"] = tmp["CustomerID"].fillna(-1).astype(int)
        top = (tmp.groupby("CustomerID")["LineTotal"]
                 .sum()
                 .sort_values(ascending=False)
                 .head(top_n_clients))
        # represent as [{'customer_id': 17850, 'total': 1234.56}, ...]
        top_clients = [{"customer_id": int(k), "total": float(v)} for k, v in top.items()]

    natural = (f"For {month}, revenue was ${revenue:,.2f}. "
               f"Top {top_n_clients} customers: " +
               (", ".join([f"{c['customer_id']} (${c['total']:,.2f})" for c in top_clients]) if top_clients else "—"))

    return {
        "month": month,
        "revenue": revenue,
        "expenses": None,   # not present in this dataset
        "profit": None,     # not available (no expenses)
        "top_clients": top_clients,
        "natural_language": natural
    }

@mcp.tool()
def invoice_lines(invoice_no: str) -> List[Dict]:
    """
    Return raw line items for a given invoice number:
    [{StockCode, Description, Quantity, UnitPrice, LineTotal, InvoiceDate, CustomerID, Country}]
    """
    _try_load()
    df = ds.df
    sub = df[df["InvoiceNo"] == str(invoice_no)].copy()
    if sub.empty:
        return []

    sub["InvoiceDate"] = sub["InvoiceDate"].dt.strftime("%Y-%m-%d %H:%M:%S")
    cols = ["StockCode", "Description", "Quantity", "UnitPrice", "LineTotal", "InvoiceDate", "CustomerID", "Country"]
    return sub[cols].to_dict(orient="records")
