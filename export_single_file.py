#!/usr/bin/env python3
"""
Generate agency_ai_export.py — a single self-contained file.
Run: python export_single_file.py
Output: agency_ai_export.py (copy anywhere, run with: python agency_ai_export.py)
"""

import base64
from pathlib import Path

BASE = Path(__file__).resolve().parent
FILES = [
    "config.py",
    "database.py",
    "services_taxonomy.py",
    "keyword_filter.py",
    "main.py",
    "app.py",
    "agents/__init__.py",
    "agents/tavily_client.py",
    "agents/firecrawl_client.py",
    "agents/ollama_client.py",
    "agents/keyword_extractor.py",
    "agents/keyword_classifier.py",
    "agents/opportunity_scorer.py",
    "agents/researcher.py",
    "agents/strategist.py",
]

HEADER = '''#!/usr/bin/env python3
"""
Agency AI — Single-file export. Copy anywhere and run.
  python agency_ai_export.py              -> start Streamlit dashboard
  python agency_ai_export.py extract      -> extract to ./agency_ai_extracted/
  python agency_ai_export.py --init-db
  python agency_ai_export.py ctc --city "Phoenix AZ"
"""
import base64
import os
import subprocess
import sys
from pathlib import Path

BUNDLE = {
'''

FOOTER = '''}

def extract(target: Path):
    for rel, b64 in BUNDLE.items():
        content = base64.b64decode(b64).decode("utf-8") if isinstance(b64, str) else b64
        dst = target / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(content, encoding="utf-8")
    (target / "requirements.txt").write_text(
        "python-dotenv>=1.0.0\\nsqlalchemy>=2.0.0\\nstreamlit>=1.28.0\\n"
        "tavily-python>=0.5.0\\nfirecrawl-py>=1.0.0\\nrequests>=2.28.0\\n",
        encoding="utf-8"
    )
    print(f"Extracted to {target}")

def main():
    out = Path.cwd() / "agency_ai_extracted"
    args = sys.argv[1:]
    if args and args[0] == "extract":
        extract(out)
        return 0
    if not (out / "main.py").exists():
        extract(out)
    os.chdir(out)
    sys.path.insert(0, str(out))
    if not args or args[0] in ("run", "start"):
        return subprocess.call([sys.executable, "-m", "streamlit", "run", "app.py"] + (args[1:] or []))
    return subprocess.call([sys.executable, "main.py"] + args)

if __name__ == "__main__":
    sys.exit(main() or 0)
'''


def main():
    bundle = {}
    for rel in FILES:
        p = BASE / rel
        if p.exists():
            content = p.read_text(encoding="utf-8")
            b64 = base64.b64encode(content.encode("utf-8")).decode("ascii")
            bundle[rel] = b64
        else:
            print(f"Warning: {rel} not found")

    out_file = BASE / "agency_ai_export.py"
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(HEADER)
        for rel, b64 in bundle.items():
            # Wrap long base64 lines for readability (optional)
            f.write(f'    "{rel}": {repr(b64)},\n')
        f.write(FOOTER)

    # Fix: BUNDLE values are base64 strings, _d expects bytes. Update FOOTER.
    # Actually repr(b64) gives a string. At runtime we need to decode. _d expects base64 bytes.
    # Let me fix: _d(b) decodes base64 - we pass a string. base64.b64decode accepts both str and bytes.
    # So _d(b64) where b64 is str works. Good.

    print(f"Written {out_file} ({out_file.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
