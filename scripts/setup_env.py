"""Generate a local environment without ever overwriting existing credentials."""
from pathlib import Path
import os
import secrets

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    path = ROOT / ".env"
    if path.exists():
        print(".env already exists; left unchanged")
        return
    template = (ROOT / ".env.example").read_text()
    secret_keys = {"CLICKHOUSE_ADMIN_PASSWORD", "CLICKHOUSE_PASSWORD", "REDIS_PASSWORD", "API_KEY"}
    lines = []
    for line in template.splitlines():
        key, sep, _ = line.partition("=")
        lines.append(f"{key}={secrets.token_hex(24)}" if sep and key in secret_keys else line)
    # Atomic create avoids accidentally replacing secrets during concurrent setup.
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as output:
        output.write("\n".join(lines) + "\n")
    print("Created .env (mode 0600). Synthetic mode is enabled; no live financial data is implied.")


if __name__ == "__main__":
    main()
