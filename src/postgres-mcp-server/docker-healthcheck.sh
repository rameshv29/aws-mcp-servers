#!/bin/bash
set -eo pipefail

# Check if the server is running and responding to health checks
if curl -f http://localhost:8000/health || wget -q -O - http://localhost:8000/health; then
  exit 0
fi

exit 1