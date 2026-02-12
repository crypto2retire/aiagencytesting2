#!/bin/bash
# Run the dashboard for remote access.
# Other devices on your network can connect via http://YOUR_IP:8501
# Find your IP: Mac/Linux: ipconfig getifaddr en0  or  hostname -I
cd "$(dirname "$0")"
echo ">>> Agency AI Dashboard (remote access)"
echo ">>> Other devices: http://<YOUR_IP>:8501  (run 'ipconfig getifaddr en0' on Mac or 'hostname -I' on Linux)"
echo ""
exec python3 -m streamlit run app.py --server.address 0.0.0.0 --server.port 8501 "$@"
