#!/usr/bin/env python3
import re
import sqlite3
import urllib.parse
from datetime import datetime, timedelta
from urllib.request import Request, urlopen

DB = "/home/mrbom/.openclaw/workspace/kolhub/data/kolhub.db"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"

YT_AVATAR_RE = re.compile(r"\\\"avatar\\\":\\\{\\\"thumbnails\\\":\\\[(.*?)\\\]\\\}", re.I | re.S)
YT_URL_RE = re.compile(r"\\\"url\\\":\\\"(https:[^\\\"]+)\\\"", re.I)
FB_OG_RE = re.compile(r"<meta[^>]+property=[\"']og:image[\"'][^>]+content=[\"']([^\"']+)[\"']", re.I)

PLACEHOLDER_MARKERS = ["ui-avatars.com", "pravatar", "picsum", "placehold", "randomuser.me"]


def fetch(url: str, timeout: int = 20) -> str:
    req = Request(url, headers={"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    with urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "ignore")


def is_placeholder(url: str) -> bool:
    low = (url or "").strip().lower()
    return (not low) or any(x in low for x in PLACEHOLDER_MARKERS) or low.startswith("data:")


def youtube_avatar(handle: str) -> str:
    h = (handle or "").strip().lstrip("@")
    if not h:
        return ""
    html = fetch(f"https://www.youtube.com/@{h}")
    m = YT_AVATAR_RE.search(html)
    if not m:
        return ""
    urls = YT_URL_RE.findall(m.group(1))
    return urls[-1] if urls else ""


def tiktok_avatar(handle: str) -> str:
    h = (handle or "").strip().lstrip("@")
    if not h:
        return ""
    # lightweight resolver fallback (can fail for some handles)
    return f"https://unavatar.io/tiktok/{urllib.parse.quote(h, safe='')}"


def facebook_avatar(handle: str) -> str:
    h = (handle or "").strip().lstrip("@")
    if not h:
        return ""
    html = fetch(f"https://www.facebook.com/{urllib.parse.quote(h, safe='')}")
    m = FB_OG_RE.search(html)
    return m.group(1) if m else ""


def update_row(cur, pid: int, image_url: str, source: str, needs_verify: int, note: str):
    now = (datetime.utcnow() + timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S")
    cur.execute(
        """
        update profiles
        set image_url=?, avatar_source=?, avatar_needs_verify=?, avatar_note=?, updated_at=?
        where id=?
        """,
        (image_url, source, needs_verify, note, now, pid),
    )


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cols = [r[1] for r in cur.execute("pragma table_info(profiles)").fetchall()]
    if "avatar_source" not in cols:
        cur.execute("alter table profiles add column avatar_source text default ''")
    if "avatar_needs_verify" not in cols:
        cur.execute("alter table profiles add column avatar_needs_verify integer default 0")
    if "avatar_note" not in cols:
        cur.execute("alter table profiles add column avatar_note text default ''")
    conn.commit()

    rows = cur.execute(
        """
        select id, name, image_url, tiktok_handle, youtube_handle, facebook_handle
        from profiles
        where avatar_needs_verify = 1
           or trim(ifnull(image_url,'')) = ''
           or lower(ifnull(image_url,'')) like '%randomuser.me%'
        order by (tiktok_followers + youtube_subs + instagram_followers + facebook_followers) desc
        limit 120
        """
    ).fetchall()

    done = {"youtube": 0, "tiktok": 0, "facebook": 0, "search": 0, "failed": 0}

    for pid, name, image_url, tk, yt, fb in rows:
        current = image_url or ""
        if not is_placeholder(current):
            continue

        updated = False

        # 1) YouTube direct
        if yt and not updated:
            try:
                img = youtube_avatar(yt)
                if img:
                    update_row(cur, pid, img, "youtube_direct", 0, "auto daily: youtube")
                    done["youtube"] += 1
                    updated = True
            except Exception:
                pass

        # 2) TikTok resolver (needs verify)
        if tk and not updated:
            try:
                img = tiktok_avatar(tk)
                if img:
                    update_row(cur, pid, img, "tiktok_resolver", 1, "auto daily: tiktok resolver, verify")
                    done["tiktok"] += 1
                    updated = True
            except Exception:
                pass

        # 3) Facebook direct (often blocked; mark verify)
        if fb and not updated:
            try:
                img = facebook_avatar(fb)
                if img:
                    update_row(cur, pid, img, "facebook_direct", 1, "auto daily: facebook direct, verify")
                    done["facebook"] += 1
                    updated = True
            except Exception:
                pass

        if not updated:
            update_row(cur, pid, current, "search_needed", 1, "need manual/search verify")
            done["failed"] += 1

    conn.commit()

    remain = cur.execute(
        """
        select count(*) from profiles
        where avatar_needs_verify = 1
           or trim(ifnull(image_url,'')) = ''
           or lower(ifnull(image_url,'')) like '%randomuser.me%'
        """
    ).fetchone()[0]
    print({"updated": done, "remaining_need_verify": remain})


if __name__ == "__main__":
    main()
