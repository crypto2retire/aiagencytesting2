# Agency AI — Primary Pipeline

**This is the canonical entry point.** Use this pipeline, not the standalone examples in `junk_removal_agents/examples/`.

## Pipeline Flow

```
Client → Researcher → Strategist → Review Dashboard → (Later: Publisher)
```

Each agent does one job, saves to the database, and exits. No agent talks to another — the database is the glue.

## Deploy to Streamlit Cloud

Git ready. See **[DEPLOY.md](DEPLOY.md)** for GitHub + Streamlit Community Cloud deployment.

## Quick Start

```bash
cd agency-ai
cp .env.example .env   # Fill in TAVILY_API_KEY, FIRECRAWL_API_KEY, ANTHROPIC_API_KEY
python3 -m pip install -r requirements.txt
python3 main.py --init-db
python3 -m streamlit run app.py
```

## Ollama (for Researcher extraction)

The Researcher uses **Ollama** to extract services, pricing, and gaps from scraped text. Without it, research is saved but extraction fields stay empty.

**Install on Mac:**
1. Download from [ollama.com/download](https://ollama.com/download) or `brew install ollama`
2. Open the Ollama app (it runs in the background and serves on port 11434)
3. Pull the model:
   ```bash
   ollama pull llama3.1:8b
   ```

**Verify:**
```bash
ollama run llama3.1:8b "Say hi"
```

### Domain-conditioned model (optional)

For higher-quality SEO keyword extraction, create the `junk-removal-seo` model locally:

```bash
ollama create junk-removal-seo -f Modelfile
```

The `Modelfile` at the project root specializes `llama3.1:8b` for junk removal commercial-intent keywords. This step is manual — do not automate this command.

If Ollama isn't running, the Researcher still runs but returns empty extraction fields.

## Run Agents

```bash
# 1. Add client via dashboard (Add Client page), or via DB

# 2. Full pipeline (Researcher → Strategist)
python3 main.py YOUR_CLIENT_ID --city "Phoenix AZ"

# Or run steps separately:
python3 main.py --researcher-only YOUR_CLIENT_ID --city "Phoenix AZ"
python3 main.py --strategist-only YOUR_CLIENT_ID

# 3. Review drafts in dashboard (Review Dashboard → Content Drafts)
```

## Structure

- `main.py` — connects agents via CLI
- `app.py` — Streamlit dashboard (onboarding + QC)
- `agents/researcher.py` — market research, saves to DB
- `agents/strategist.py` — scoring + drafting + differentiation, saves to DB
- `database.py` — SQLAlchemy models

## Remote Access

To work from home or let others test:

**Same network (Wi‑Fi / LAN):**
```bash
# Run with remote binding
./run_remote.sh
# or
python3 -m streamlit run app.py --server.address 0.0.0.0
```

Then open `http://YOUR_IP:8501` from another device. Get your IP:
- Mac: `ipconfig getifaddr en0`
- Linux: `hostname -I | awk '{print $1}'`

**From the internet** (e.g. testers not on your network):
- **ngrok**: `ngrok http 8501` → use the URL it gives you
- **Cloud**: Deploy to [Streamlit Community Cloud](https://share.streamlit.io/) or Railway, Render, etc. (set env vars for API keys)

**Security:** The app has no built-in auth. On shared or public networks, use a VPN or restrict access.

## Database

- Default: SQLite (`agency_ai.db`)
- For Postgres: set `DATABASE_URL` in `.env`, run `docker-compose up -d`
- After schema changes: `rm agency_ai.db && python3 main.py --init-db`
