#!/bin/bash
# Kills this project's node processes on every host listed in the config.
# Usage: ./cleanup.sh [axj22config.txt]

CONFIG="${1:-axj22config.txt}"
DIR="$(cd "$(dirname "$0")" && pwd)"

# Hosts = 2nd field of the node lines (the numberOfNodes lines after the globals line)
HOSTS=$(sed 's/#.*//' "$CONFIG" | awk 'NF { if (!n) { n = $1; next } if (c < n) { print $2; c++ } }' | sort -u)

for h in $HOSTS; do
    echo "cleaning $h"
    # [b]inary so pkill doesn't match the remote shell running this command
    ssh -n -o BatchMode=yes "$h" "pkill -u \$USER -f '$DIR/[b]inary'" 2>/dev/null
done
