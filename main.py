from app import mcp
import os

#create an mcp server
NOTES_FILE = os.path.join(os.path.dirname(__file__), "notes.txt")

def ensure_notes_file_exists():
    if not os.path.exists(NOTES_FILE):
        with open(NOTES_FILE, "w") as f:
            f.write("")

@mcp.tool()
def add_note(message: str) -> str:
    """Add a note to the notes file."""
    ensure_notes_file_exists()
    with open(NOTES_FILE, "a") as f:
        f.write(message + "\n")
    return "Note added."
