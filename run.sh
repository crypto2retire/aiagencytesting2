#!/bin/bash
# Run the pipeline. Use this, not junk_removal_agents/examples.
cd "$(dirname "$0")"
echo ">>> Agency AI Pipeline"
echo ">>> Use: python3 main.py --help"
echo ""
exec python3 main.py "$@"
