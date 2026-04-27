import html
import json
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import parse_qs, quote_plus
from urllib.request import Request, urlopen
import hashlib

TZ = timezone(timedelta(hours=7))
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "kolhub.db"

LIST_COLUMNS = [
    "id",
    "name",
    "image_url",
    "type",
    "category",
    "tags",
    "tiktok_handle",
    "youtube_handle",
    "instagram_handle",
    "facebook_handle",
    "tiktok_followers",
    "youtube_subs",
    "instagram_followers",
    "facebook_followers",
    "updated_at",
]

FULL_COLUMNS = [
    *LIST_COLUMNS,
    "bio",
    "assets_public",
    "notes",
    "created_at",
]

DEFAULT_ASSETS = [
    "iPhone quay video",
    "Micro không dây",
    "Đèn key light",
    "Tripod",
    "Gimbal",
    "Laptop dựng video",
    "Tai nghe kiểm âm",
    "Bàn phím cơ",
    "Camera phụ",
    "Phần mềm edit",
]

CATEGORIES_10 = [
    "Beauty",
    "Food",
    "Tech",
    "Finance",
    "Lifestyle",
    "Gaming",
    "Education",
    "Mom & Baby",
    "Travel",
    "Sports",
]


def now_iso() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")


def db():
    uri = f"file:{DB_PATH}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def clamp_int(value: str, default: int, min_val: int, max_val: int) -> int:
    try:
        n = int(value)
    except Exception:
        n = default
    return max(min_val, min(max_val, n))


def fmt_num(n: int) -> str:
    try:
        return f"{int(n):,}".replace(",", ".")
    except Exception:
        return "0"


def json_response(start_response, obj, status="200 OK", cache_control="no-store"):
    body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    headers = [
        ("Content-Type", "application/json; charset=utf-8"),
        ("Content-Length", str(len(body))),
        ("Cache-Control", cache_control),
    ]
    start_response(status, headers)
    return [body]


def html_response(start_response, html_text, status="200 OK"):
    body = html_text.encode("utf-8")
    headers = [
        ("Content-Type", "text/html; charset=utf-8"),
        ("Content-Length", str(len(body))),
        ("Cache-Control", "no-store"),
    ]
    start_response(status, headers)
    return [body]


def proxied_image_url(url: str, fallback_seed: str = "kolhub") -> str:
    raw = (url or "").strip()
    # Treat placeholder avatar services as "no real image" to avoid demo-looking UI.
    # These often get abused / look fake, so we prefer our own deterministic SVG.
    low = raw.lower()
    if (
        not raw
        or "ui-avatars.com" in low
        or "pravatar" in low
        or "picsum" in low
        or "placehold" in low
        or low.startswith("data:")
    ):
        raw = ""
    return f"/img?url={quote_plus(raw)}&seed={quote_plus(fallback_seed)}"


def render_svg_avatar(seed: str) -> bytes:
    s = (seed or "KOL").strip()
    parts = [x for x in s.replace("_", " ").replace("-", " ").split() if x]
    initials = "".join([p[0].upper() for p in parts[:2]]) or "K"
    palette = ["#4F46E5", "#7C3AED", "#0EA5E9", "#0891B2", "#334155", "#1D4ED8"]
    idx = int(hashlib.md5(s.encode("utf-8")).hexdigest(), 16) % len(palette)
    bg = palette[idx]
    svg = f"""<svg xmlns='http://www.w3.org/2000/svg' width='512' height='512' viewBox='0 0 512 512'>
  <rect width='512' height='512' rx='64' fill='{bg}'/>
  <text x='50%' y='52%' dominant-baseline='middle' text-anchor='middle' fill='#E6EEFF' font-family='Inter,Segoe UI,Arial,sans-serif' font-size='180' font-weight='700'>{html.escape(initials)}</text>
</svg>"""
    return svg.encode("utf-8")


def parse_assets(value):
    if not value:
        return []
    try:
        data = json.loads(value)
        if isinstance(data, list):
            return [str(x).strip() for x in data if str(x).strip()]
    except Exception:
        pass
    return [x.strip() for x in str(value).split(",") if x.strip()]


def normalize_handle(h):
    if not h:
        return ""
    s = str(h).strip()
    if not s:
        return ""
    if s.startswith("http://") or s.startswith("https://"):
        return s
    return s.lstrip("@")


def avatar_verification_status(row) -> tuple[bool, str]:
    """Heuristic verification for admin/UI.

    verified=True when avatar URL looks like it came from the platform/CDN/resolver,
    not from placeholders.
    """
    url = (row.get("image_url") or "").strip()
    low = url.lower()
    if not url:
        return (False, "missing")

    # Placeholder / demo providers
    if any(x in low for x in ["ui-avatars.com", "pravatar", "picsum", "placehold", "randomuser.me"]) or low.startswith("data:"):
        return (False, "placeholder")

    # High-confidence sources
    if any(x in low for x in [
        "yt3.ggpht.com",
        "yt3.googleusercontent.com",
        "googleusercontent.com",
        "tiktokcdn",
        "unavatar.io",
        "fbcdn.net",
        "cdninstagram",
    ]):
        return (True, "verified")

    # Unknown source
    return (False, "unknown")


def make_recent_posts(row):
    posts = []
    tiktok = normalize_handle(row.get("tiktok_handle"))
    youtube = normalize_handle(row.get("youtube_handle"))
    instagram = normalize_handle(row.get("instagram_handle"))
    facebook = normalize_handle(row.get("facebook_handle"))

    if tiktok:
        posts.append(
            {
                "platform": "TikTok",
                "url": tiktok if tiktok.startswith("http") else f"https://www.tiktok.com/@{tiktok}",
                "label": f"@{tiktok}" if not tiktok.startswith("http") else "TikTok",
            }
        )
    if youtube:
        posts.append(
            {
                "platform": "YouTube",
                "url": youtube if youtube.startswith("http") else f"https://www.youtube.com/@{youtube}",
                "label": f"@{youtube}" if not youtube.startswith("http") else "YouTube",
            }
        )
    if instagram:
        posts.append(
            {
                "platform": "Instagram",
                "url": instagram if instagram.startswith("http") else f"https://www.instagram.com/{instagram}/",
                "label": f"@{instagram}" if not instagram.startswith("http") else "Instagram",
            }
        )
    if facebook:
        posts.append(
            {
                "platform": "Facebook",
                "url": facebook if facebook.startswith("http") else f"https://www.facebook.com/{facebook}",
                "label": facebook if not facebook.startswith("http") else "Facebook",
            }
        )
    return posts[:4]


def make_social_links(row):
    links = []
    for platform, key, pattern in [
        ("TikTok", "tiktok_handle", "https://www.tiktok.com/@{}"),
        ("YouTube", "youtube_handle", "https://www.youtube.com/@{}"),
        ("Instagram", "instagram_handle", "https://www.instagram.com/{}/"),
        ("Facebook", "facebook_handle", "https://www.facebook.com/{}"),
    ]:
        handle = normalize_handle(row.get(key))
        if not handle:
            continue
        if handle.startswith("http"):
            links.append({"platform": platform, "url": handle, "handle": platform})
        else:
            links.append({"platform": platform, "url": pattern.format(handle), "handle": f"@{handle}"})
    return links


def render_home(start_response):
    with db() as conn:
        featured = [
            dict(r)
            for r in conn.execute(
                """
                select id,name,image_url,type,category,
                       tiktok_followers,youtube_subs,instagram_followers,facebook_followers
                from profiles
                order by (tiktok_followers + youtube_subs + instagram_followers + facebook_followers) desc
                limit 8
                """
            ).fetchall()
        ]

        top_tiktok = [
            dict(r)
            for r in conn.execute(
                """
                select id,name,image_url,type,category,tiktok_followers as v
                from profiles
                where (tiktok_handle is not null and trim(tiktok_handle) != '')
                order by tiktok_followers desc
                limit 30
                """
            ).fetchall()
        ]
        top_youtube = [
            dict(r)
            for r in conn.execute(
                """
                select id,name,image_url,type,category,youtube_subs as v
                from profiles
                where (youtube_handle is not null and trim(youtube_handle) != '')
                order by youtube_subs desc
                limit 30
                """
            ).fetchall()
        ]
        top_facebook = [
            dict(r)
            for r in conn.execute(
                """
                select id,name,image_url,type,category,facebook_followers as v
                from profiles
                where (facebook_handle is not null and trim(facebook_handle) != '')
                order by facebook_followers desc
                limit 30
                """
            ).fetchall()
        ]

    cards = []
    for p in featured:
        total = (p.get("tiktok_followers") or 0) + (p.get("youtube_subs") or 0) + (p.get("instagram_followers") or 0) + (p.get("facebook_followers") or 0)
        avatar = proxied_image_url(p.get("image_url") or "", p.get("name") or "KOL")
        cards.append(
            f"""
            <a class='f-card' href='/profile/{p.get("id")}'>
              <img loading='lazy' src='{html.escape(avatar)}' alt='{html.escape(p.get("name") or "KOL")}' onerror="this.onerror=null;this.src='/img?seed=kolhub-fallback';" />
              <div class='f-body'>
                <div class='f-name'>{html.escape(p.get("name") or "")}</div>
                <div class='f-meta'>
                  <span>{html.escape(p.get("type") or "KOL")}</span>
                  <span>{html.escape(p.get("category") or "General")}</span>
                </div>
                <div class='f-total'>{fmt_num(total)} followers tổng</div>
              </div>
            </a>
            """
        )

    chips = "".join([f"<span class='chip'>{html.escape(x)}</span>" for x in CATEGORIES_10])

    def top_list_html(items, label):
        li = []
        for r in items:
            avatar = proxied_image_url(r.get("image_url") or "", r.get("name") or "KOL")
            li.append(
                f"""
                <a class='t-item' href='/profile/{r.get("id")}' title='{html.escape(r.get("name") or '')}'>
                  <img src='{html.escape(avatar)}' alt='{html.escape(r.get("name") or '')}' onerror="this.onerror=null;this.src='/img?seed=top-fallback';"/>
                  <div class='t-name'>{html.escape(r.get("name") or '')}</div>
                  <div class='t-val'>{fmt_num(r.get('v') or 0)}</div>
                </a>
                """
            )
        return f"<div class='topbox'><div class='topbox-h'>{html.escape(label)}</div><div class='t-grid'>{''.join(li) or '<div class=\'empty\'>Chưa có dữ liệu</div>'}</div></div>"

    top_sections = "".join(
        [
            top_list_html(top_tiktok, "Top follow TikTok (30)"),
            top_list_html(top_youtube, "Top follow YouTube (30)"),
            top_list_html(top_facebook, "Top follow Facebook (30)"),
        ]
    )

    page = f"""
<!doctype html>
<html lang='vi'>
<head>
  <meta charset='utf-8'/>
  <meta name='viewport' content='width=device-width,initial-scale=1'/>
  <title>KOL Hub VN</title>
  <style>
    :root{{
      --bg:#080b14; --bg2:#0f1526; --text:#e9eeff; --muted:rgba(233,238,255,.72);
      --muted2:rgba(233,238,255,.55); --border:rgba(255,255,255,.12); --panel:rgba(255,255,255,.05);
      --acc:#8b5cf6; --acc2:#06b6d4;
    }}
    *{{box-sizing:border-box}} body{{margin:0;font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto;color:var(--text);
      background:radial-gradient(1000px 520px at 15% 8%, rgba(139,92,246,.30), transparent 60%),
                 radial-gradient(900px 520px at 88% 12%, rgba(6,182,212,.22), transparent 58%),
                 linear-gradient(180deg,var(--bg),var(--bg2));}}
    a{{color:inherit;text-decoration:none}}
    .wrap{{max-width:1120px;margin:0 auto;padding:0 22px}}
    .nav{{position:sticky;top:0;z-index:20;backdrop-filter:blur(10px);background:rgba(8,11,20,.58);border-bottom:1px solid var(--border)}}
    .nav-in{{display:flex;align-items:center;justify-content:space-between;padding:13px 0;gap:12px}}
    .brand{{font-weight:800;letter-spacing:.02em;display:flex;align-items:center;gap:10px}}
    .dot{{width:10px;height:10px;border-radius:999px;background:linear-gradient(90deg,var(--acc),var(--acc2))}}
    .btn{{display:inline-flex;align-items:center;justify-content:center;padding:10px 14px;border-radius:999px;border:1px solid var(--border);background:var(--panel)}}
    .btn.pri{{border:0;background:linear-gradient(90deg,var(--acc),#4f46e5)}}

    .hero{{padding:72px 0 28px}}
    .hero-grid{{display:grid;grid-template-columns:1.15fr .85fr;gap:22px}}
    @media(max-width:920px){{.hero-grid{{grid-template-columns:1fr}}.hero{{padding-top:44px}}}}
    .panel{{border:1px solid var(--border);background:var(--panel);border-radius:20px}}
    .hero-main{{padding:24px}}
    h1{{margin:0;font-size:46px;line-height:1.05}}
    @media(max-width:560px){{h1{{font-size:34px}}}}
    .lead{{margin-top:14px;color:var(--muted);line-height:1.75}}
    .chips{{display:flex;flex-wrap:wrap;gap:8px;margin-top:14px}}
    .chip{{font-size:12px;padding:7px 10px;border-radius:999px;border:1px solid var(--border);color:var(--muted)}}

    .side{{padding:20px}}
    .kpi{{display:grid;grid-template-columns:repeat(3,1fr);gap:9px;margin-top:14px}}
    .kpi .it{{padding:12px;border-radius:12px;border:1px solid var(--border);background:rgba(255,255,255,.04)}}
    .kpi .n{{font-size:19px;font-weight:800}} .kpi .t{{font-size:11px;color:var(--muted2);margin-top:5px}}

    .sec{{padding:16px 0 44px}}
    .sec-h{{display:flex;align-items:end;justify-content:space-between;gap:10px;flex-wrap:wrap}}
    .sec h2{{margin:0;font-size:26px}} .sec p{{margin:8px 0 0;color:var(--muted);max-width:760px;line-height:1.7}}
    .f-grid{{margin-top:16px;display:grid;grid-template-columns:repeat(4,1fr);gap:12px}}
    @media(max-width:1080px){{.f-grid{{grid-template-columns:repeat(3,1fr)}}}}
    @media(max-width:780px){{.f-grid{{grid-template-columns:repeat(2,1fr)}}}}
    @media(max-width:520px){{.f-grid{{grid-template-columns:1fr}}}}
    .f-card{{border:1px solid var(--border);border-radius:16px;overflow:hidden;background:rgba(255,255,255,.04);transition:all .18s ease}}
    .f-card:hover{{transform:translateY(-2px);border-color:rgba(139,92,246,.5)}}
    .f-card img{{width:100%;aspect-ratio:1/1;object-fit:cover;display:block}}
    .f-body{{padding:12px}}
    .f-name{{font-weight:800;line-height:1.3}}
    .f-meta{{margin-top:8px;display:flex;gap:8px;flex-wrap:wrap;font-size:11px;color:var(--muted2)}}
    .f-meta span{{padding:4px 7px;border:1px solid var(--border);border-radius:999px}}
    .f-total{{margin-top:10px;font-size:12px;color:#c5d0ff}}

    .topwrap{{padding:0 0 46px}}
    .top3{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}}
    @media(max-width:980px){{.top3{{grid-template-columns:1fr}}}}
    .topbox{{border:1px solid var(--border);background:rgba(255,255,255,.04);border-radius:18px;padding:14px}}
    .topbox-h{{font-weight:900;margin-bottom:10px}}
    .t-grid{{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}}
    .t-item{{display:grid;grid-template-columns:44px 1fr auto;gap:10px;align-items:center;
      padding:10px;border-radius:14px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.03)}}
    .t-item img{{width:44px;height:44px;border-radius:12px;object-fit:cover;border:1px solid rgba(255,255,255,.10)}}
    .t-name{{font-weight:800;font-size:13px;line-height:1.2}}
    .t-val{{font-weight:900;color:#c5d0ff;font-size:12px}}
    .empty{{color:var(--muted2);font-size:13px}}

    .foot{{padding:24px 0 36px;border-top:1px solid var(--border);color:var(--muted2);font-size:13px}}
  </style>
</head>
<body>
  <div class='nav'>
    <div class='wrap'>
      <div class='nav-in'>
        <div class='brand'><span class='dot'></span> KOL Hub VN</div>
        <div style='display:flex;gap:10px;flex-wrap:wrap'>
          <a class='btn' href='/explore'>Khám phá KOList</a>
          <a class='btn pri' href='/profile/first'>Top Profile</a>
        </div>
      </div>
    </div>
  </div>

  <div class='hero'>
    <div class='wrap'>
      <div class='hero-grid'>
        <div class='panel hero-main'>
          <h1>Kho dữ liệu KOL/KOC Việt Nam,<br/>tập trung <span style='background:linear-gradient(90deg,var(--acc),var(--acc2));-webkit-background-clip:text;background-clip:text;color:transparent'>highlight profile chất</span>.</h1>
          <div class='lead'>Home giữ vai trò giới thiệu và spotlight các KOL nổi bật. Trang KOList để search/filter sâu theo lĩnh vực & nền tảng.</div>
          <div class='chips'>{chips}</div>
          <div style='display:flex;gap:10px;flex-wrap:wrap;margin-top:18px'>
            <a class='btn pri' href='/explore'>Mở KOList</a>
          </div>
        </div>

        <div class='panel side'>
          <div style='font-weight:800'>Tập dữ liệu hiện tại</div>
          <div style='margin-top:8px;color:var(--muted2);font-size:14px;line-height:1.65'>Ưu tiên web mượt: chạy trước dataset gọn (~300), spotlight profile mạnh, sau đó mở rộng crawl theo 10 ngành.</div>
          <div class='kpi'>
            <div class='it'><div class='n'>~300</div><div class='t'>KOL/KOC</div></div>
            <div class='it'><div class='n'>10</div><div class='t'>Ngành</div></div>
            <div class='it'><div class='n'>4</div><div class='t'>Platform</div></div>
          </div>
        </div>
      </div>

      <div class='sec'>
        <div class='sec-h'>
          <div>
            <h2>Highlighted KOL</h2>
            <p>Ít nhưng nổi bật: ưu tiên profile có follower mạnh và nhiều nền tảng để đội marketing ra quyết định nhanh.</p>
          </div>
          <a class='btn' href='/explore'>Khám phá KOList</a>
        </div>
        <div class='f-grid'>
          {''.join(cards)}
        </div>
      </div>

      <div class='topwrap'>
        <div class='sec-h'>
          <div>
            <h2>Top follow theo nền tảng</h2>
            <p>Ưu tiên profile có handle rõ (đặc biệt TikTok). Mỗi box lấy top 30.</p>
          </div>
        </div>
        <div class='top3'>
          {top_sections}
        </div>
      </div>
    </div>
  </div>

  <div class='foot'>
    <div class='wrap'>KOL Hub VN — internal tool.</div>
  </div>
</body>
</html>
"""
    return html_response(start_response, page)


def render_profile(start_response, pid: int):
    with db() as conn:
        row = conn.execute(f"select {', '.join(FULL_COLUMNS)} from profiles where id = ?", [pid]).fetchone()
        if not row:
            return json_response(start_response, {"ok": False, "error": "Not found"}, "404 Not Found")

        p = dict(row)

        v_ok, v_reason = avatar_verification_status(p)
        p["avatar_verified"] = bool(v_ok)
        p["avatar_verify_reason"] = v_reason

        similar = [
            dict(r)
            for r in conn.execute(
                """
                select id,name,image_url,type,category,
                       (tiktok_followers + youtube_subs + instagram_followers + facebook_followers) as total
                from profiles
                where id != ?
                order by total desc
                limit 4
                """,
                [pid],
            ).fetchall()
        ]

    assets = parse_assets(p.get("assets_public"))
    if len(assets) < 10:
        assets = assets + [x for x in DEFAULT_ASSETS if x not in assets]
    assets = assets[:10]

    posts = make_recent_posts(p)
    socials = make_social_links(p)

    total = (p.get("tiktok_followers") or 0) + (p.get("youtube_subs") or 0) + (p.get("instagram_followers") or 0) + (p.get("facebook_followers") or 0)

    post_html = "".join(
        [
            f"""
            <a class='p-card' href='{html.escape(x['url'])}' target='_blank'>
              <div class='p-top'><span class='badge'>{html.escape(x['platform'])}</span></div>
              <div class='p-title'>{html.escape(x['label'])}</div>
              <div class='p-link'>Mở content gốc ↗</div>
            </a>
            """
            for x in posts
        ]
    ) or "<div class='empty'>Chưa có link content.</div>"

    asset_html = "".join([f"<div class='asset'>{html.escape(x)}</div>" for x in assets])

    social_html = "".join(
        [f"<a class='s-btn' href='{html.escape(s['url'])}' target='_blank'>{html.escape(s['platform'])}: {html.escape(s['handle'])}</a>" for s in socials]
    ) or "<div class='empty'>Chưa có social links.</div>"

    rec_html = "".join(
        [
            f"""
            <a class='r-card' href='/profile/{r.get('id')}'>
              <img src='{html.escape(proxied_image_url(r.get('image_url') or '', r.get('name') or 'KOL'))}' alt='{html.escape(r.get('name') or '')}' onerror="this.onerror=null;this.src='/img?seed=rec-fallback';"/>
              <div class='r-body'>
                <div class='r-name'>{html.escape(r.get('name') or '')}</div>
                <div class='r-meta'>{html.escape(r.get('type') or '')} • {fmt_num(r.get('total') or 0)}</div>
              </div>
            </a>
            """
            for r in similar
        ]
    ) or "<div class='empty'>Chưa có gợi ý.</div>"

    avatar = proxied_image_url(p.get("image_url") or "", p.get("name") or "KOL")

    page = f"""
<!doctype html>
<html lang='vi'>
<head>
  <meta charset='utf-8'/>
  <meta name='viewport' content='width=device-width,initial-scale=1'/>
  <title>{html.escape(p.get('name') or 'Profile')} — KOL Hub VN</title>
  <style>
    :root{{--bg:#0a1020;--text:#e9eeff;--muted:rgba(233,238,255,.72);--muted2:rgba(233,238,255,.58);--border:rgba(255,255,255,.12);--panel:rgba(255,255,255,.05);--acc:#8b5cf6;--acc2:#06b6d4}}
    *{{box-sizing:border-box}} body{{margin:0;font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto;color:var(--text);
      background:radial-gradient(900px 460px at 10% 0%, rgba(139,92,246,.28), transparent 58%),linear-gradient(180deg,#080c16,#0f1628)}}
    a{{color:inherit;text-decoration:none}}
    .wrap{{max-width:1120px;margin:0 auto;padding:0 20px}}
    .top{{position:sticky;top:0;z-index:22;backdrop-filter:blur(10px);background:rgba(8,12,22,.62);border-bottom:1px solid var(--border)}}
    .top-in{{display:flex;align-items:center;justify-content:space-between;padding:12px 0;gap:10px;flex-wrap:wrap}}
    .btn{{display:inline-flex;align-items:center;justify-content:center;padding:9px 14px;border-radius:999px;border:1px solid var(--border);background:var(--panel)}}

    .hero{{padding:26px 0 12px}}
    .hero-card{{border:1px solid var(--border);background:var(--panel);border-radius:20px;padding:18px;display:grid;grid-template-columns:180px 1fr;gap:18px}}
    @media(max-width:780px){{.hero-card{{grid-template-columns:1fr}}}}
    .avatar{{width:100%;aspect-ratio:1/1;border-radius:16px;object-fit:cover;border:1px solid var(--border)}}
    .name{{font-size:32px;line-height:1.1;margin:0}}
    .sub{{margin-top:8px;color:var(--muted);font-size:14px}}
    .stats{{margin-top:14px;display:grid;grid-template-columns:repeat(5,1fr);gap:8px}}
    @media(max-width:980px){{.stats{{grid-template-columns:repeat(2,1fr)}}}}
    .st{{padding:10px;border:1px solid var(--border);border-radius:12px;background:rgba(255,255,255,.04)}}
    .st .n{{font-weight:800;font-size:15px}} .st .t{{margin-top:5px;font-size:11px;color:var(--muted2)}}
    .socials{{margin-top:12px;display:flex;flex-wrap:wrap;gap:8px}}
    .s-btn{{padding:7px 10px;border-radius:999px;border:1px solid var(--border);background:rgba(255,255,255,.04);font-size:12px;color:var(--muted)}}

    .sec{{padding:18px 0}}
    .sec h2{{margin:0 0 10px;font-size:22px}}
    .grid2{{display:grid;grid-template-columns:1fr 1fr;gap:14px}}
    @media(max-width:920px){{.grid2{{grid-template-columns:1fr}}}}
    .box{{border:1px solid var(--border);background:var(--panel);border-radius:16px;padding:14px}}

    .posts{{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}}
    @media(max-width:680px){{.posts{{grid-template-columns:1fr}}}}
    .p-card{{border:1px solid var(--border);border-radius:12px;background:rgba(255,255,255,.04);padding:12px;display:block}}
    .badge{{font-size:11px;padding:4px 7px;border:1px solid var(--border);border-radius:999px;color:var(--muted2)}}
    .p-title{{margin-top:9px;font-weight:700}} .p-link{{margin-top:8px;font-size:12px;color:#b8c5ff}}

    .assets{{display:grid;grid-template-columns:repeat(2,1fr);gap:8px}}
    @media(max-width:560px){{.assets{{grid-template-columns:1fr}}}}
    .asset{{padding:9px 10px;border-radius:10px;border:1px solid var(--border);background:rgba(255,255,255,.04);font-size:13px}}

    .recs{{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}}
    @media(max-width:980px){{.recs{{grid-template-columns:repeat(2,1fr)}}}}
    @media(max-width:560px){{.recs{{grid-template-columns:1fr}}}}
    .r-card{{border:1px solid var(--border);border-radius:14px;overflow:hidden;background:rgba(255,255,255,.04)}}
    .r-card img{{display:block;width:100%;aspect-ratio:1/1;object-fit:cover}}
    .r-body{{padding:10px}} .r-name{{font-weight:700}} .r-meta{{margin-top:6px;font-size:12px;color:var(--muted2)}}

    .empty{{font-size:13px;color:var(--muted2)}}
  </style>
</head>
<body>
  <div class='top'>
    <div class='wrap'>
      <div class='top-in'>
        <div style='display:flex;gap:8px;flex-wrap:wrap'>
          <a class='btn' href='/'>← Home</a>
          <a class='btn' href='/explore'>Khám phá KOLhub</a>
        </div>
      </div>
    </div>
  </div>

  <div class='wrap'>
    <div class='hero'>
      <div class='hero-card'>
        <div><img class='avatar' src='{html.escape(avatar)}' alt='{html.escape(p.get('name') or 'KOL')}' onerror="this.onerror=null;this.src='/img?seed=profile-fallback';"/></div>
        <div>
          <h1 class='name'>{html.escape(p.get('name') or '')}</h1>
          <div class='sub'>{html.escape(p.get('type') or 'KOL')} • {html.escape(p.get('category') or 'General')} • Cập nhật: {html.escape(p.get('updated_at') or '-')}</div>
          <div class='sub' style='margin-top:6px'>Avatar verify: <b style='color:{'#34d399' if p.get('avatar_verified') else '#fbbf24'}'>{'VERIFIED' if p.get('avatar_verified') else 'NEEDS VERIFY'}</b> <span style='color:var(--muted2)'>({html.escape(p.get('avatar_verify_reason') or '-')})</span></div>
          <div class='sub' style='margin-top:10px'>{html.escape((p.get('bio') or 'Chưa có mô tả ngắn cho profile này.'))}</div>

          <div class='stats'>
            <div class='st'><div class='n'>{fmt_num(total)}</div><div class='t'>Tổng followers</div></div>
            <div class='st'><div class='n'>{fmt_num(p.get('tiktok_followers') or 0)}</div><div class='t'>TikTok</div></div>
            <div class='st'><div class='n'>{fmt_num(p.get('youtube_subs') or 0)}</div><div class='t'>YouTube</div></div>
            <div class='st'><div class='n'>{fmt_num(p.get('instagram_followers') or 0)}</div><div class='t'>Instagram</div></div>
            <div class='st'><div class='n'>{fmt_num(p.get('facebook_followers') or 0)}</div><div class='t'>Facebook</div></div>
          </div>

          <div class='socials'>{social_html}</div>
        </div>
      </div>
    </div>

    <div class='sec'>
      <div class='grid2'>
        <div class='box'>
          <h2>Content gần đây</h2>
          <div class='posts'>{post_html}</div>
        </div>
        <div class='box'>
          <h2>10 món đồ KOL đang dùng</h2>
          <div class='assets'>{asset_html}</div>
        </div>
      </div>
    </div>

    <div class='sec'>
      <div class='box'>
        <h2>Gợi ý KOL tương tự</h2>
        <div class='recs'>{rec_html}</div>
      </div>
    </div>
  </div>
</body>
</html>
"""
    return html_response(start_response, page)


def render_explore(start_response, q: str = "", category: str = "", platform: str = "", min_followers: int = 0, sort: str = "followers"):
    search = (q or "").strip().lower()
    category = (category or "").strip()
    category_value = category if category else "all"
    platform = (platform or "").strip().lower()
    min_followers = max(0, int(min_followers or 0))
    sort = (sort or "followers").strip().lower()
    if sort not in {"followers", "newest", "name"}:
        sort = "followers"

    where = []
    params = []

    if search:
        where.append("lower(name) like ?")
        params.append(f"%{search}%")

    if category_value and category_value.lower() != "all":
        where.append("lower(category) = ?")
        params.append(category_value.lower())

    if platform in {"tiktok", "youtube", "instagram", "facebook"}:
        col = {
            "tiktok": "tiktok_followers",
            "youtube": "youtube_subs",
            "instagram": "instagram_followers",
            "facebook": "facebook_followers",
        }[platform]
        where.append(f"{col} >= ?")
        params.append(min_followers)
    elif min_followers > 0:
        where.append("(tiktok_followers + youtube_subs + instagram_followers + facebook_followers) >= ?")
        params.append(min_followers)

    if sort == "name":
        order_sql = "name asc"
    elif sort == "newest":
        order_sql = "updated_at desc"
    else:
        order_sql = {
            "tiktok": "tiktok_followers desc",
            "youtube": "youtube_subs desc",
            "instagram": "instagram_followers desc",
            "facebook": "facebook_followers desc",
        }.get(platform, "(tiktok_followers + youtube_subs + instagram_followers + facebook_followers) desc")

    where_sql = f" where {' and '.join(where)}" if where else ""

    with db() as conn:
        rows = [
            dict(r)
            for r in conn.execute(
                f"""
                select {', '.join(LIST_COLUMNS)} from profiles
                {where_sql}
                order by {order_sql}
                limit 120
                """,
                params,
            ).fetchall()
        ]

    cards = []
    for p in rows:
        total = (p.get("tiktok_followers") or 0) + (p.get("youtube_subs") or 0) + (p.get("instagram_followers") or 0) + (p.get("facebook_followers") or 0)
        img = proxied_image_url(p.get("image_url") or "", p.get("name") or "KOL")
        cards.append(
            f"""
            <a class='card' href='/profile/{p.get("id")}'>
              <div class='row'>
                <img class='avatar' src='{html.escape(img)}' alt='{html.escape(p.get("name") or "KOL")}' onerror="this.onerror=null;this.src='/img?seed=kolhub-list-fallback'"/>
                <div>
                  <div class='name'>{html.escape(p.get("name") or "")}</div>
                  <div class='meta'>
                    <span class='tag'>{html.escape(p.get("type") or "")}</span>
                    <span class='tag'>{html.escape(p.get("category") or "General")}</span>
                    <span class='tag'>Total {fmt_num(total)}</span>
                  </div>
                </div>
              </div>
              <div class='meta' style='margin-top:10px'>
                <span class='tag'>TikTok {fmt_num(p.get("tiktok_followers") or 0)}</span>
                <span class='tag'>YouTube {fmt_num(p.get("youtube_subs") or 0)}</span>
                <span class='tag'>IG {fmt_num(p.get("instagram_followers") or 0)}</span>
                <span class='tag'>FB {fmt_num(p.get("facebook_followers") or 0)}</span>
              </div>
            </a>
            """
        )

    category_options = "".join(
        [
            f"<option value='{html.escape(c)}' {'selected' if c == category_value else ''}>{html.escape('Tất cả lĩnh vực' if c == 'all' else c)}</option>"
            for c in (["all"] + CATEGORIES_10)
        ]
    )
    platform_options = "".join(
        [
            f"<option value='{k}' {'selected' if k == platform else ''}>{v}</option>"
            for k, v in [("", "Tất cả nền tảng"), ("tiktok", "TikTok"), ("youtube", "YouTube"), ("instagram", "Instagram"), ("facebook", "Facebook")]
        ]
    )
    sort_options = "".join(
        [
            f"<option value='{k}' {'selected' if k == sort else ''}>{v}</option>"
            for k, v in [("followers", "Top followers"), ("newest", "Mới cập nhật"), ("name", "Tên A-Z")]
        ]
    )

    chip_items = []
    for k, label in [("", "Tất cả"), ("tiktok", "TikTok"), ("youtube", "YouTube"), ("instagram", "Instagram"), ("facebook", "Facebook")]:
        href = (
            "/explore?"
            + "&".join(
                [
                    f"q={quote_plus(q or '')}",
                    f"category={quote_plus(category_value)}",
                    f"platform={quote_plus(k)}",
                    f"min_followers={int(min_followers or 0)}",
                    f"sort={quote_plus(sort)}",
                ]
            )
        )
        cls = "pchip active" if ((not platform and k == "") or platform == k) else "pchip"
        chip_items.append(f"<a class='{cls}' href='{href}'>{html.escape(label)}</a>")
    platform_chips = "".join(chip_items)

    p_map = {"tiktok": "TikTok", "youtube": "YouTube", "instagram": "Instagram", "facebook": "Facebook"}
    s_map = {"followers": "Top followers", "newest": "Mới cập nhật", "name": "Tên A-Z"}
    active = []
    if q:
        active.append(f"<span class='achip'>Tên: {html.escape(q)}</span>")
    if category_value and category_value.lower() != "all":
        active.append(f"<span class='achip'>Lĩnh vực: {html.escape(category_value)}</span>")
    if platform:
        active.append(f"<span class='achip'>Nền tảng: {html.escape(p_map.get(platform, platform))}</span>")
    if min_followers > 0:
        active.append(f"<span class='achip'>≥ {fmt_num(min_followers)} followers</span>")
    if sort != "followers":
        active.append(f"<span class='achip'>Sort: {html.escape(s_map.get(sort, sort))}</span>")
    active_html = (
        f"<div class='active-filters'><div class='achips'>{''.join(active)}</div><a class='clear' href='/explore'>Reset</a></div>"
        if active
        else ""
    )

    page = f"""
<!doctype html>
<html lang='vi'>
<head>
  <meta charset='utf-8'/>
  <meta name='viewport' content='width=device-width,initial-scale=1'/>
  <title>KOList — KOL Hub VN</title>
  <style>
    body{{margin:0;font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto;background:#0b1020;color:#eaf0ff}}
    a{{color:inherit;text-decoration:none}}
    .container{{max-width:1120px;margin:0 auto;padding:0 18px}}
    .top{{position:sticky;top:0;background:rgba(11,16,32,.86);backdrop-filter:blur(10px);border-bottom:1px solid rgba(255,255,255,.10);z-index:10}}
    .top-inner{{display:flex;align-items:center;gap:12px;padding:12px 0;flex-wrap:wrap}}
    .nav3{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;width:100%}}
    .btn{{height:42px;padding:0 14px;border-radius:12px;border:1px solid rgba(255,255,255,.16);background:rgba(255,255,255,.06);color:#fff;cursor:pointer;display:flex;align-items:center;justify-content:center;font-weight:700}}
    .btn.active{{background:linear-gradient(90deg,#7c3aed,#2563eb);border-color:transparent}}
    .filters{{display:grid;grid-template-columns:2fr 1fr 1fr 1fr 1fr auto;gap:10px;width:100%}}
    @media(max-width:980px){{.filters{{grid-template-columns:1fr 1fr}}}}
    .input,.select{{height:42px;border-radius:12px;padding:0 12px;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.06);color:#fff;outline:none}}
    .chips-platform{{display:flex;gap:8px;flex-wrap:wrap;width:100%}}
    .pchip{{padding:8px 12px;border-radius:999px;border:1px solid rgba(255,255,255,.16);background:rgba(255,255,255,.05);font-size:12px;color:rgba(234,240,255,.9)}}
    .pchip.active{{background:linear-gradient(90deg,#7c3aed,#2563eb);border-color:transparent;color:#fff}}
    .active-filters{{display:flex;align-items:center;justify-content:space-between;gap:10px;width:100%;padding:2px 0 4px;flex-wrap:wrap}}
    .achips{{display:flex;gap:8px;flex-wrap:wrap}}
    .achip{{font-size:12px;padding:6px 10px;border-radius:999px;border:1px solid rgba(255,255,255,.16);background:rgba(255,255,255,.05);color:rgba(234,240,255,.85)}}
    .clear{{font-size:12px;padding:6px 10px;border:1px solid rgba(255,255,255,.16);border-radius:999px;background:rgba(255,255,255,.05)}}
    .grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;padding:18px 0 40px}}
    @media(max-width:980px){{
      .grid{{grid-template-columns:1fr}}
      .chips-platform{{overflow:auto;white-space:nowrap;flex-wrap:nowrap;padding-bottom:2px}}
      .pchip{{flex:0 0 auto}}
    }}
    .card{{border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.05);border-radius:16px;padding:14px}}
    .row{{display:flex;gap:12px;align-items:center}}
    .avatar{{width:64px;height:64px;aspect-ratio:1/1;border-radius:12px;object-fit:cover;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.04)}}
    .name{{font-weight:800}}
    .meta{{margin-top:8px;font-size:12px;color:rgba(234,240,255,.65);display:flex;gap:10px;flex-wrap:wrap}}
    .tag{{border:1px solid rgba(255,255,255,.12);padding:4px 8px;border-radius:999px}}
    .empty{{padding:28px 0;color:rgba(234,240,255,.7)}}
  </style>
</head>
<body>
  <div class='top'>
    <div class='container'>
      <div class='top-inner'>
        <div class='nav3'>
          <a class='btn' href='/'>Home</a>
          <a class='btn active' href='/explore'>KOList</a>
          <a class='btn' href='/profile/first'>Top Profile</a>
        </div>
        <form class='filters' method='get' action='/explore'>
          <input name='q' value='{html.escape(q or '')}' class='input' placeholder='Search theo tên...'/>
          <input name='min_followers' value='{int(min_followers or 0)}' type='number' min='0' step='1000' class='input' placeholder='Min followers'/>
          <select name='category' class='select'>{category_options}</select>
          <select name='platform' class='select'>{platform_options}</select>
          <select name='sort' class='select'>{sort_options}</select>
          <button class='btn' type='submit'>Lọc</button>
        </form>
        <div class='chips-platform'>{platform_chips}</div>
        {active_html}
      </div>
    </div>
  </div>

  <div class='container'>
    <div class='grid'>
      {''.join(cards) or "<div class='empty'>Chưa có KOL phù hợp.</div>"}
    </div>
  </div>
</body>
</html>
"""
    return html_response(start_response, page)


def app(environ, start_response):
    path = environ.get("PATH_INFO", "/")
    method = environ.get("REQUEST_METHOD", "GET").upper()

    if path == "/api/health":
        return json_response(
            start_response,
            {"ok": True, "ts": now_iso(), "db_exists": DB_PATH.exists()},
            cache_control="public, max-age=30, s-maxage=30",
        )

    if path == "/img":
        qs = parse_qs(environ.get("QUERY_STRING", ""))
        raw = (qs.get("url", [""])[0] or "").strip()
        seed = (qs.get("seed", ["KOL Hub"])[0] or "KOL Hub").strip()[:80]

        if raw.startswith("http://") or raw.startswith("https://"):
            try:
                req = Request(raw, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://kolhubvnn.vercel.app/"})
                with urlopen(req, timeout=8) as r:
                    body = r.read()
                    ctype = (r.headers.get("Content-Type") or "image/jpeg").split(";")[0].strip()
                if ctype.startswith("image/"):
                    start_response(
                        "200 OK",
                        [
                            ("Content-Type", ctype),
                            ("Content-Length", str(len(body))),
                            ("Cache-Control", "public, max-age=86400, s-maxage=86400, stale-while-revalidate=604800"),
                        ],
                    )
                    return [body]
            except Exception:
                pass

        body = render_svg_avatar(seed)
        start_response(
            "200 OK",
            [
                ("Content-Type", "image/svg+xml; charset=utf-8"),
                ("Content-Length", str(len(body))),
                ("Cache-Control", "public, max-age=86400, s-maxage=86400, stale-while-revalidate=604800"),
            ],
        )
        return [body]

    if path == "/api/profiles":
        if method != "GET":
            return json_response(start_response, {"ok": False, "error": "Method not allowed"}, "405 Method Not Allowed")

        qs = parse_qs(environ.get("QUERY_STRING", ""))
        q = (qs.get("q", [""])[0] or "").strip().lower()
        platform = (qs.get("platform", [""])[0] or "").strip().lower()
        category = (qs.get("category", [""])[0] or "").strip().lower()
        min_followers = clamp_int((qs.get("min_followers", ["0"])[0] or "0"), 0, 0, 10**9)
        limit = clamp_int((qs.get("limit", ["50"])[0] or "50"), 50, 1, 200)
        page = clamp_int((qs.get("page", ["1"])[0] or "1"), 1, 1, 10000)
        sort = (qs.get("sort", ["updated"])[0] or "updated").strip().lower()
        view = (qs.get("view", ["list"])[0] or "list").strip().lower()

        offset = (page - 1) * limit
        where = []
        params = []

        if q:
            where.append("lower(name) like ?")
            params.append(f"%{q}%")

        if category:
            where.append("lower(category) = ?")
            params.append(category)

        if platform:
            if platform == "tiktok":
                where.append("tiktok_followers >= ?")
                params.append(min_followers)
            elif platform == "youtube":
                where.append("youtube_subs >= ?")
                params.append(min_followers)
            elif platform == "instagram":
                where.append("instagram_followers >= ?")
                params.append(min_followers)
            elif platform == "facebook":
                where.append("facebook_followers >= ?")
                params.append(min_followers)

        columns = FULL_COLUMNS if view == "full" else LIST_COLUMNS
        select_cols = ", ".join(columns)

        if sort == "followers":
            order_sql = "(tiktok_followers + youtube_subs + instagram_followers + facebook_followers) desc"
        elif sort == "name":
            order_sql = "name asc"
        else:
            order_sql = "updated_at desc"

        where_sql = f" where {' and '.join(where)}" if where else ""
        list_sql = f"select {select_cols} from profiles{where_sql} order by {order_sql} limit ? offset ?"
        count_sql = f"select count(1) as total from profiles{where_sql}"

        with db() as conn:
            total = conn.execute(count_sql, params).fetchone()[0]
            rows = [dict(r) for r in conn.execute(list_sql, [*params, limit, offset]).fetchall()]

        for p in rows:
            v_ok, v_reason = avatar_verification_status(p)
            p["avatar_verified"] = bool(v_ok)
            p["avatar_verify_reason"] = v_reason

        return json_response(
            start_response,
            {
                "ok": True,
                "profiles": rows,
                "paging": {
                    "page": page,
                    "limit": limit,
                    "total": total,
                    "has_next": page * limit < total,
                },
            },
            cache_control="public, max-age=20, s-maxage=20, stale-while-revalidate=60",
        )

    if path == "/api/profile":
        pid = clamp_int((parse_qs(environ.get("QUERY_STRING", "")).get("id", ["0"])[0] or "0"), 0, 0, 10**9)
        if pid <= 0:
            return json_response(start_response, {"ok": False, "error": "Missing id"}, "400 Bad Request")

        with db() as conn:
            row = conn.execute(f"select {', '.join(FULL_COLUMNS)} from profiles where id = ?", [pid]).fetchone()
        if not row:
            return json_response(start_response, {"ok": False, "error": "Not found"}, "404 Not Found")

        p = dict(row)
        v_ok, v_reason = avatar_verification_status(p)
        p["avatar_verified"] = bool(v_ok)
        p["avatar_verify_reason"] = v_reason
        assets = parse_assets(p.get("assets_public"))
        if len(assets) < 10:
            assets = assets + [x for x in DEFAULT_ASSETS if x not in assets]
        p["top_assets"] = assets[:10]
        p["recent_posts"] = make_recent_posts(p)
        p["social_links"] = make_social_links(p)
        return json_response(start_response, {"ok": True, "profile": p}, cache_control="public, max-age=20, s-maxage=20")

    if path == "/profile/first":
        with db() as conn:
            r = conn.execute(
                """
                select id from profiles
                order by (tiktok_followers + youtube_subs + instagram_followers + facebook_followers) desc
                limit 1
                """
            ).fetchone()
        if not r:
            return json_response(start_response, {"ok": False, "error": "No profiles"}, "404 Not Found")
        path = f"/profile/{int(r[0])}"

    if path.startswith("/profile/"):
        try:
            pid = int(path.split("/")[-1])
        except Exception:
            return json_response(start_response, {"ok": False, "error": "Not found"}, "404 Not Found")
        return render_profile(start_response, pid)

    if path == "/":
        return render_home(start_response)

    if path == "/explore":
        qs = parse_qs(environ.get("QUERY_STRING", ""))
        q = (qs.get("q", [""])[0] or "").strip()
        category = (qs.get("category", [""])[0] or "").strip()
        platform = (qs.get("platform", [""])[0] or "").strip()
        sort = (qs.get("sort", ["followers"])[0] or "followers").strip()
        min_followers = clamp_int((qs.get("min_followers", ["0"])[0] or "0"), 0, 0, 10**9)
        return render_explore(start_response, q, category=category, platform=platform, min_followers=min_followers, sort=sort)

    return json_response(start_response, {"ok": False, "error": "Not found"}, "404 Not Found")
