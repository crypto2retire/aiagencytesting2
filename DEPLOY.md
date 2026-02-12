# Deploy to Streamlit Community Cloud

## 1. Push to GitHub

```bash
cd agency-ai
git init
git add .
git commit -m "Initial commit: Agency AI dashboard"
```

Create a new repo on [GitHub](https://github.com/new):
- Name: `agency-ai` (or your choice)
- Public
- Do not add README (you already have one)

```bash
git remote add origin https://github.com/YOUR_USERNAME/agency-ai.git
git branch -M main
git push -u origin main
```

## 2. Deploy on Streamlit Cloud

1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Sign in with GitHub
3. **New app** → Select your repo, branch `main`, main file `app.py`
4. Click **Advanced settings** and add secrets:

```toml
TAVILY_API_KEY = "your-tavily-key"
FIRECRAWL_API_KEY = "your-firecrawl-key"
ANTHROPIC_API_KEY = "your-anthropic-key"
```

5. Deploy. Your app will be live at `https://YOUR-APP.streamlit.app`

## 3. Database on Streamlit Cloud

Streamlit Cloud uses ephemeral storage. Data resets on redeploy. For persistence:
- Use a hosted database (e.g. free Postgres on [Neon](https://neon.tech) or [Supabase](https://supabase.com))
- Set `DATABASE_URL` in Streamlit secrets

## 4. Note on Ollama

The Researcher uses **Ollama** locally for extraction. On Streamlit Cloud, Ollama is not available. The app will run, but research extraction may fall back to Claude. Ensure `ANTHROPIC_API_KEY` is set for full functionality.
