"""Generate local deployment secrets without committing or printing them."""
from pathlib import Path
import secrets

root = Path(__file__).resolve().parents[1]
environment = root / ".env"
try:
    with environment.open("x", encoding="utf-8") as stream:
        stream.write(f"DB_PASSWORD={secrets.token_urlsafe(32)}\n")
        stream.write(f"TASKFORGE_API_KEY={secrets.token_urlsafe(32)}\n")
        stream.write("TASKFORGE_PORT=18080\n")
    print("Created .env. API key is stored there; do not commit it.")
except FileExistsError:
    print(".env already exists; preserved existing configuration.")
