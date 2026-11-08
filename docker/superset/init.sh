#!/bin/bash
# Creates or upgrades the Superset metadata database, the admin user, and the default roles. Repeatable.
set -euo pipefail

for name in SUPERSET_SECRET_KEY SUPERSET_ADMIN_USERNAME SUPERSET_ADMIN_PASSWORD SUPERSET_ADMIN_EMAIL; do
  if [[ -z "${!name:-}" ]]; then
    echo "[superset-init] missing environment variable ${name}" >&2
    exit 1
  fi
done

superset db upgrade
if superset fab list-users 2>/dev/null | grep -q "username:${SUPERSET_ADMIN_USERNAME} "; then
  superset fab reset-password --username "$SUPERSET_ADMIN_USERNAME" --password "$SUPERSET_ADMIN_PASSWORD"
else
  superset fab create-admin --username "$SUPERSET_ADMIN_USERNAME" --password "$SUPERSET_ADMIN_PASSWORD" \
    --firstname Superset --lastname Admin --email "$SUPERSET_ADMIN_EMAIL"
fi
superset init
echo "[superset-init] metadata database ready"
