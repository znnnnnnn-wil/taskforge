"""Generate local deployment secrets without committing or printing them."""
from pathlib import Path
import secrets

root = Path(__file__).resolve().parents[1]
environment = root / ".env"
defaults = {
    "DB_PASSWORD": lambda: secrets.token_urlsafe(32),
    "TASKFORGE_API_KEY": lambda: secrets.token_urlsafe(32),
    "TASKFORGE_PORT": lambda: "18080",
    "RABBIT_PASSWORD": lambda: secrets.token_urlsafe(32),
}
try:
    with environment.open("x", encoding="utf-8") as stream:
        for name, generate in defaults.items():
            stream.write(f"{name}={generate()}\n")
    print("Created .env with local deployment secrets; do not commit it.")
except FileExistsError:
    existing = environment.read_text(encoding="utf-8")
    names = {line.split("=", 1)[0].strip() for line in existing.splitlines()
             if "=" in line and not line.lstrip().startswith("#")}
    missing = [name for name in defaults if name not in names]
    if missing:
        with environment.open("a", encoding="utf-8") as stream:
            if existing and not existing.endswith("\n"):
                stream.write("\n")
            for name in missing:
                stream.write(f"{name}={defaults[name]()}\n")
        print("Added missing deployment settings; preserved all existing values.")
    else:
        print(".env already exists; preserved existing configuration.")
