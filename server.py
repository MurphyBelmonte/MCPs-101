"""
FastMCP quickstart example.

cd to the `examples/snippets/clients` directory and run:
    uv run server fastmcp_quickstart stdio
"""

from app import mcp
import os
import finance_tools

# Create an MCP server
mcp = FastMCP("Demo")

mcp_server = FastMCP("AI Sticky Notes")
NOTES_FILE = os.path.join(os.path.dirname(__file__), "notes.txt")

def ensure_notes_file_exists():
    if not os.path.exists(NOTES_FILE):
        with open(NOTES_FILE, "w") as f:
            f.write("")

# Add an addition tool
@mcp.tool()
def add(a: int, b: int) -> int:
    """Add two numbers"""
    return a + b

@mcp.tool()
def add_note(message: str) -> str:
    """Add a note to the notes file."""
    ensure_notes_file_exists()
    with open(NOTES_FILE, "a") as f:
        f.write(message + "\n")
    return "Note added."
@mcp.tool()
def read_notes() -> str:
    """Read all notes from the notes file."""
    ensure_notes_file_exists()
    with open(NOTES_FILE, "r") as f:
        content = f.read().strip()
    return content or "No notes available."


# Add a dynamic greeting resource
@mcp.resource("greeting://{name}")
def get_greeting(name: str) -> str:
    """Get a personalized greeting"""
    return f"Hello, {name}!"

@mcp.resource("notes://latest")
def get_latest_note() -> str:
    """Get the latest note from the notes file."""
    ensure_notes_file_exists()
    with open(NOTES_FILE, "r") as f:
        lines = f.readlines()
    return lines[-1].strip() if lines else "No notes available."

@mcp.prompt()
def note_summary_prompt() -> str:
    """Generate a summary of all notes"""
    ensure_notes_file_exists()
    with open(NOTES_FILE, "r") as f:
        content = f.read().strip()
    if not content:
        return "There are no notes to summarize."
    return f"Please provide a concise summary of the following notes:\n{content}"

# Add a prompt
@mcp.prompt()
def greet_user(name: str, style: str = "friendly") -> str:
    """Generate a greeting prompt."""
    styles = {
        "friendly": "Please write a warm, friendly greeting",
        "formal": "Please write a formal, professional greeting",
        "casual": "Please write a casual, relaxed greeting",
    }

    return f"{styles.get(style, styles['friendly'])} for someone named {name}."
