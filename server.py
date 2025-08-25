# server.py
# ✅ Uses the shared FastMCP instance from app.py
from app import mcp
from docx import Document  # needs: python-docx
import os
import json
import base64
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# -----------------------------
# Notes demo (kept from yours)
# -----------------------------
NOTES_FILE = Path(__file__).with_name("notes.txt")

def _ensure_notes_file():
    NOTES_FILE.touch(exist_ok=True)

@mcp.tool()
def add(a: int, b: int) -> int:
    """Add two numbers"""
    return a + b

@mcp.tool()
def add_note(message: str) -> str:
    """Append a note to notes.txt"""
    _ensure_notes_file()
    NOTES_FILE.write_text((NOTES_FILE.read_text() if NOTES_FILE.exists() else "") + message + "\n", encoding="utf-8")
    return "Note added."

@mcp.tool()
def read_notes() -> str:
    """Read all notes (or 'No notes available.')"""
    _ensure_notes_file()
    content = NOTES_FILE.read_text(encoding="utf-8").strip()
    return content or "No notes available."

@mcp.resource("greeting://{name}")
def get_greeting(name: str) -> str:
    """Simple greeting resource"""
    return f"Hello, {name}!"

@mcp.resource("notes://latest")
def get_latest_note() -> str:
    """Return the most recent line from notes.txt (or 'No notes available.')"""
    _ensure_notes_file()
    lines = NOTES_FILE.read_text(encoding="utf-8").splitlines()
    return lines[-1] if lines else "No notes available."

@mcp.prompt()
def note_summary_prompt() -> str:
    """Prompt to summarize all notes"""
    _ensure_notes_file()
    content = NOTES_FILE.read_text(encoding="utf-8").strip()
    if not content:
        return "There are no notes to summarize."
    return f"Summarize these notes in bullet points, then a 2‑sentence tl;dr:\n\n{content}"

@mcp.prompt()
def greet_user(name: str, style: str = "friendly") -> str:
    """Prompt to greet a user in a style"""
    styles = {
        "friendly": "Write a warm, friendly greeting",
        "formal":   "Write a formal, professional greeting",
        "casual":   "Write a casual, relaxed greeting",
    }
    return f"{styles.get(style, styles['friendly'])} for someone named {name}."

# -----------------------------------
# File registry + reading tools
# -----------------------------------
_FILE_REGISTRY: Dict[str, Path] = {}

def _lazy_import(modname: str):
    import importlib
    return importlib.import_module(modname)

def _read_text_file(p: Path, max_bytes: int) -> str:
    data = p.read_bytes()[:max_bytes]
    return data.decode("utf-8", errors="replace")

def _read_json_file(p: Path, max_bytes: int) -> str:
    obj = json.loads(p.read_text(encoding="utf-8", errors="replace"))
    preview = json.dumps(obj, ensure_ascii=False, indent=2)
    return preview[:max_bytes]

def _read_csv_file(p: Path, max_bytes: int) -> str:
    try:
        pd = _lazy_import("pandas")
        df = pd.read_csv(p)
        return df.head(30).to_csv(index=False)[:max_bytes]
    except Exception as e:
        return f"[CSV preview unavailable: {e}]"

def _read_xlsx_file(p: Path, max_bytes: int) -> str:
    try:
        pd = _lazy_import("pandas")  # requires openpyxl for .xlsx
        df = pd.read_excel(p, sheet_name=None)
        out_lines = []
        for sheet, sdf in df.items():
            out_lines.append(f"=== Sheet: {sheet} (showing first 20 rows) ===")
            out_lines.append(sdf.head(20).to_csv(index=False))
        preview = "\n".join(out_lines)
        return preview[:max_bytes]
    except Exception as e:
        return f"[Excel preview unavailable: {e}]"

def _read_docx_text(path: str, max_bytes: int = 200_000) -> str:
    try:
        doc = Document(path)
        text = "\n".join(p.text for p in doc.paragraphs if p.text.strip())
        return text[:max_bytes]
    except Exception as e:
        return f"[DOCX parse error: {e}]"

def _read_pdf_file(p: Path, max_bytes: int) -> str:
    try:
        pypdf = _lazy_import("pypdf")
        reader = pypdf.PdfReader(str(p))
        chunks: List[str] = []
        for page in reader.pages[:20]:
            chunks.append(page.extract_text() or "")
            if sum(len(c) for c in chunks) >= max_bytes:
                break
        text = "\n".join(chunks)
        return text[:max_bytes]
    except Exception as e:
        return f"[PDF preview unavailable: {e}]"

def _read_any(p: Path, max_bytes: int = 200_000) -> Tuple[str, str]:
    ext = p.suffix.lower()
    if ext in (".txt", ".md", ".log", ".py", ".js", ".ts", ".html", ".css"):
        return ("text/plain", _read_text_file(p, max_bytes))
    if ext == ".json":
        return ("application/json", _read_json_file(p, max_bytes))
    if ext == ".csv":
        return ("text/csv", _read_csv_file(p, max_bytes))
    if ext in (".xlsx", ".xlsm", ".xls"):
        return ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", _read_xlsx_file(p, max_bytes))
    if ext == ".docx":
        return (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            _read_docx_text(str(p), max_bytes)
        )
    if ext == ".pdf":
        return ("application/pdf", _read_pdf_file(p, max_bytes))
    try:
        return ("application/octet-stream", _read_text_file(p, max_bytes))
    except Exception:
        return ("application/octet-stream", "[Preview unavailable for this file type]")

@mcp.tool()
def register_file(alias: str, file_path: str) -> str:
    """Register a file with a short alias for easy reuse."""
    p = Path(file_path).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"File not found: {p}")
    _FILE_REGISTRY[alias] = p
    return f"Registered '{alias}' -> {p}"

@mcp.tool()
def list_registered() -> List[Dict[str, str]]:
    """List {alias, path} for registered files."""
    return [{"alias": k, "path": str(v)} for k, v in _FILE_REGISTRY.items()]

@mcp.tool()
def read_file(file_or_alias: str, max_bytes: int = 200_000) -> Dict[str, str]:
    """
    Read a file (by alias or path) and return a preview payload:
    { 'path', 'type', 'size_bytes', 'preview' }
    """
    p = Path(_FILE_REGISTRY.get(file_or_alias, file_or_alias)).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"File not found: {p}")
    ftype, preview = _read_any(p, max_bytes=max_bytes)
    size = p.stat().st_size
    return {"path": str(p), "type": ftype, "size_bytes": str(size), "preview": preview}

# -----------------------------------
# NEW: File Vault (create & save)
# -----------------------------------
BASE_SAVE_DIR = Path(__file__).with_name("saved")
BASE_SAVE_DIR.mkdir(exist_ok=True)

_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")

def _sanitize_name(name: str) -> str:
    name = name.strip().replace(" ", "_")
    name = _SAFE_NAME.sub("-", name)
    # prevent directory traversal
    name = name.replace("..", "-")
    return name

def _ensure_subdir(subdir: Optional[str]) -> Path:
    if subdir:
        sd = BASE_SAVE_DIR / _sanitize_name(subdir)
        sd.mkdir(parents=True, exist_ok=True)
        return sd
    return BASE_SAVE_DIR

def _unique_path(dir_: Path, filename: str) -> Path:
    base = dir_ / _sanitize_name(filename)
    if not base.exists():
        return base
    stem = base.stem
    ext = base.suffix
    i = 1
    while True:
        candidate = dir_ / f"{stem} ({i}){ext}"
        if not candidate.exists():
            return candidate
        i += 1

def _auto_register_alias(p: Path, alias: Optional[str]):
    if alias:
        _FILE_REGISTRY[alias] = p
    else:
        # also register a default alias based on the stem (doesn't overwrite)
        key = p.stem
        if key not in _FILE_REGISTRY:
            _FILE_REGISTRY[key] = p

@mcp.tool()
def create_file(filename: str, subdir: Optional[str] = None, overwrite: bool = False, alias: Optional[str] = None) -> Dict[str, str]:
    """
    Create an empty file inside the 'saved' vault (optionally in a subfolder).
    If overwrite=False and name exists, a unique '(1)', '(2)', ... suffix is added.
    Returns: { 'path', 'created': 'true/false' }
    """
    target_dir = _ensure_subdir(subdir)
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = _sanitize_name(filename)
    path = (target_dir / filename) if overwrite else _unique_path(target_dir, filename)
    path.touch(exist_ok=True)
    _auto_register_alias(path, alias)
    return {"path": str(path), "created": "true"}

@mcp.tool()
def save_text(filename: str, content: str, subdir: Optional[str] = None, append: bool = False, alias: Optional[str] = None) -> Dict[str, str]:
    """
    Save text content to a file in the vault.
    - append=True to append, otherwise overwrite or create new unique file if name taken.
    Returns: { 'path', 'bytes_written' }
    """
    target_dir = _ensure_subdir(subdir)
    if append:
        path = target_dir / _sanitize_name(filename)
    else:
        path = _unique_path(target_dir, _sanitize_name(filename))
    data = content.encode("utf-8")
    target_dir.mkdir(parents=True, exist_ok=True)
    mode = "ab" if append and path.exists() else "wb"
    with open(path, mode) as f:
        f.write(data)
    _auto_register_alias(path, alias)
    return {"path": str(path.resolve()), "bytes_written": str(len(data))}

@mcp.tool()
def save_base64(filename: str, data_base64: str, subdir: Optional[str] = None, overwrite: bool = False, alias: Optional[str] = None) -> Dict[str, str]:
    """
    Save arbitrary bytes from a Base64 string to a file (images, PDFs, zips, etc.).
    Returns: { 'path', 'bytes_written' }
    """
    target_dir = _ensure_subdir(subdir)
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = _sanitize_name(filename)
    path = (target_dir / filename) if overwrite else _unique_path(target_dir, filename)
    raw = base64.b64decode(data_base64, validate=True)
    with open(path, "wb") as f:
        f.write(raw)
    _auto_register_alias(path, alias)
    return {"path": str(path.resolve()), "bytes_written": str(len(raw))}

@mcp.tool()
def save_data_url(filename: str, data_url: str, subdir: Optional[str] = None, overwrite: bool = False, alias: Optional[str] = None) -> Dict[str, str]:
    """
    Save from a data URL: 'data:<mime>;base64,<payload>'.
    Returns: { 'path', 'bytes_written', 'mime' }
    """
    if not data_url.startswith("data:") or ";base64," not in data_url:
        raise ValueError("Expected a data URL like 'data:<mime>;base64,<payload>'")
    header, b64 = data_url.split(";base64,", 1)
    mime = header[5:]  # drop 'data:'
    result = save_base64(filename=filename, data_base64=b64, subdir=subdir, overwrite=overwrite, alias=alias)
    result["mime"] = mime
    return result

@mcp.tool()
def where_is(file_or_alias: str) -> str:
    """
    Return the absolute path for a saved file or registered alias/absolute path.
    """
    if file_or_alias in _FILE_REGISTRY:
        return str(_FILE_REGISTRY[file_or_alias].resolve())
    p = Path(file_or_alias).expanduser()
    if not p.is_absolute():
        # assume it's inside the vault
        candidate = (BASE_SAVE_DIR / _sanitize_name(file_or_alias)).resolve()
        if candidate.exists():
            return str(candidate)
    return str(p.resolve())

@mcp.tool()
def list_saved(subdir: Optional[str] = None) -> List[str]:
    """
    List files inside the vault (optionally within a subfolder).
    """
    target_dir = _ensure_subdir(subdir)
    if not target_dir.exists():
        return []
    return [str(p.resolve()) for p in sorted(target_dir.iterdir()) if p.is_file()]

# -----------------------------------
# Summarization prompts
# -----------------------------------
@mcp.prompt()
def summarize_file_prompt(file_or_alias: str, max_chars: int = 50_000, style: str = "concise") -> str:
    """
    Build a summarization prompt for a given file.
    Use list_registered / register_file to manage aliases; can pass a full path too.
    """
    p = Path(_FILE_REGISTRY.get(file_or_alias, file_or_alias)).expanduser().resolve()
    if not p.exists():
        return f"File not found: {p}"
    _, preview = _read_any(p, max_bytes=max_chars)

    instructions = {
        "concise":  "Summarize the content in 5–10 bullet points, then provide a 2‑sentence TL;DR.",
        "detailed": "Produce a structured summary with headings, key findings, 3 actionable insights, and open questions."
    }
    guidance = instructions.get(style, instructions["concise"])

    return f"""{guidance}

Filename: {p.name}
If the content looks like data (CSV/Excel), describe the columns, notable ranges/anomalies, and 3 quick insights.

--- BEGIN CONTENT (truncated to {max_chars} chars) ---
{preview}
--- END CONTENT ---
"""

@mcp.tool()
def quick_summary(file_or_alias: str, max_chars: int = 10_000) -> str:
    """
    Lightweight, *non‑LLM* heuristic summary that returns a few lines.
    """
    p = Path(_FILE_REGISTRY.get(file_or_alias, file_or_alias)).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"File not found: {p}")
    ftype, preview = _read_any(p, max_bytes=max_chars)
    size = p.stat().st_size

    head = preview[:400]
    tail = preview[-400:] if len(preview) > 800 else ""
    snip = head + ("\n...\n" + tail if tail else "")

    return (
        f"Path: {p}\n"
        f"Type: {ftype}\n"
        f"Size: {size} bytes\n"
        f"Preview snippet:\n{snip}"
    )
