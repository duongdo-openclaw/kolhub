#!/usr/bin/env python3
import html
import re
import sqlite3
from datetime import datetime, timedelta
from urllib.request import Request, urlopen

DB = "/home/mrbom/.openclaw/workspace/kolhub/data/kolhub.db"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"

OG_DESC_RE = re.compile(r"<meta[^>]+property=[\"']og:description[\"'][^>]+content=[\"']([^\"']+)[\"']", re.I)
META_DESC_RE = re.compile(r"<meta[^>]+name=[\"']description[\"'][^>]+content=[\"']([^\"']+)[\"']", re.I)
JSON_DESC_RE = re.compile(r'"description"\s*:\s*\{\s*"simpleText"\s*:\s*"(.*?)"\s*\}', re.I | re.S)


def fetch(url: str, timeout: int = 20) -> str:
    req = Request(url, headers={"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    with urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "ignore")


def clean_text(s: str) -> str:
    t = html.unescape((s or "").replace("\\n", " ").replace("\\u0026", "&"))
    t = re.sub(r"\s+", " ", t).strip()
    return t


def take_short(s: str, max_len: int = 170) -> str:
    s = clean_text(s)
    if len(s) <= max_len:
        return s
    cut = s[:max_len].rsplit(" ", 1)[0].strip()
    return (cut or s[:max_len]).strip() + "…"


def youtube_about_text(handle: str) -> str:
    h = (handle or "").strip().lstrip("@")
    if not h:
        return ""
    # Prefer /about first, fallback to channel home.
    urls = [f"https://www.youtube.com/@{h}/about", f"https://www.youtube.com/@{h}"]
    for u in urls:
        try:
            html_doc = fetch(u)
        except Exception:
            continue
        for rx in (OG_DESC_RE, META_DESC_RE, JSON_DESC_RE):
            m = rx.search(html_doc)
            if m:
                txt = take_short(m.group(1))
                if txt and txt.lower() not in {"youtube", ""}:
                    return txt
    return ""


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    rows = cur.execute(
        """
        select id, name, youtube_handle
        from profiles
        where trim(ifnull(youtube_handle,'')) != ''
          and trim(ifnull(bio,'')) = ''
        order by youtube_subs desc
        limit 150
        """
    ).fetchall()

    updated = failed = 0
    for pid, name, handle in rows:
        try:
            bio = youtube_about_text(handle)
        except Exception:
            bio = ""

        if bio:
            now = (datetime.utcnow() + timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S")
            cur.execute("update profiles set bio=?, updated_at=? where id=?", (bio, now, pid))
            updated += 1
        else:
            failed += 1

    conn.commit()
    remain = cur.execute(
        """
        select count(*) from profiles
        where trim(ifnull(youtube_handle,'')) != ''
          and trim(ifnull(bio,'')) = ''
        """
    ).fetchone()[0]
    print({"processed": len(rows), "updated": updated, "failed": failed, "remaining_missing_bio_youtube": remain})


if __name__ == "__main__":
    main()
