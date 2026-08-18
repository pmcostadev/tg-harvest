import os, asyncio, random, string
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError, PeerFloodError, UserPrivacyRestrictedError,
    UserIsBlockedError, UserIdInvalidError, InputUserDeactivatedError,
)
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch
from psycopg_pool import AsyncConnectionPool

VERSION = "v4"

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION = os.environ["TG_SESSION"]
GROUP = os.environ["TG_GROUP"]
MODE = os.getenv("MODE", "listen")
DB_URL = os.environ["DATABASE_URL"]

PAGE = int(os.getenv("PAGE_SIZE", "100"))
DELAY = float(os.getenv("DELAY", "8"))
JITTER = float(os.getenv("JITTER", "4"))

INVITE_LINK = os.getenv("INVITE_LINK", "")
INVITE_TEXT = os.getenv("INVITE_TEXT", "")
INVITE_DAILY_CAP = int(os.getenv("INVITE_DAILY_CAP", "15"))
INVITE_DELAY = float(os.getenv("INVITE_DELAY", "180"))
INVITE_JITTER = float(os.getenv("INVITE_JITTER", "120"))
ACTIVES_ONLY = os.getenv("ACTIVES_ONLY", "false").lower() == "true"
# Only DM people harvested from this group. Keeps messages honest when the
# database holds members from several groups.
INVITE_SCOPE_GROUP = os.getenv("INVITE_SCOPE_GROUP", "true").lower() == "true"

client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)
pool = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS members (
  user_id BIGINT PRIMARY KEY,
  username TEXT, first_name TEXT, last_name TEXT,
  is_bot BOOLEAN, source TEXT,
  seen_at TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE members ADD COLUMN IF NOT EXISTS group_name TEXT;
CREATE INDEX IF NOT EXISTS members_group_idx ON members (group_name);
CREATE TABLE IF NOT EXISTS outreach (
  user_id BIGINT PRIMARY KEY,
  status TEXT,
  detail TEXT,
  sent_at TIMESTAMPTZ DEFAULT now()
);
"""

# Backfill: everything already in the table came from the first group.
BACKFILL = "UPDATE members SET group_name = %s WHERE group_name IS NULL"

INSERT = """
INSERT INTO members (user_id, username, first_name, last_name, is_bot, source, group_name)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username
"""


async def save(users, source):
    rows = [(u.id, u.username, u.first_name, u.last_name,
             bool(getattr(u, "bot", False)), source, GROUP) for u in users]
    async with pool.connection() as con:
        async with con.cursor() as cur:
            await cur.executemany(INSERT, rows)


async def mark(user_id, status, detail=""):
    async with pool.connection() as con:
        await con.execute(
            """INSERT INTO outreach (user_id, status, detail) VALUES (%s, %s, %s)
               ON CONFLICT (user_id) DO UPDATE SET status = EXCLUDED.status,
               detail = EXCLUDED.detail, sent_at = now()""",
            (user_id, status, detail[:300]),
        )


async def count_saved():
    async with pool.connection() as con:
        cur = await con.execute(
            "SELECT count(*) FROM members WHERE group_name = %s", (GROUP,))
        return (await cur.fetchone())[0]


async def nap(base=None, jit=None):
    base = DELAY if base is None else base
    jit = JITTER if jit is None else jit
    await asyncio.sleep(base + random.random() * jit)


async def idle():
    while True:
        await asyncio.sleep(3600)


# ---------------------------------------------------------------- scrape

async def page_through(ch, query, seen):
    offset, found = 0, 0
    while True:
        try:
            res = await client(GetParticipantsRequest(
                ch, ChannelParticipantsSearch(query), offset, PAGE, hash=0))
        except FloodWaitError as e:
            print(f"[floodwait] sleeping {e.seconds}s", flush=True)
            await asyncio.sleep(e.seconds + 10)
            continue
        if not res.users:
            return found
        fresh = [u for u in res.users if u.id not in seen]
        seen.update(u.id for u in fresh)
        if fresh:
            await save(fresh, f"scrape:{query or '_'}")
        found += len(fresh)
        offset += len(res.users)
        print(f"[{query or '(all)'}] offset={offset} new={found} total={len(seen)}", flush=True)
        await nap()


async def scrape():
    ch = await client.get_entity(GROUP)
    total_members = getattr(ch, "participants_count", None)
    print(f"target: {GROUP} | reported members: {total_members}", flush=True)
    seen = set()
    await page_through(ch, "", seen)
    print(f"plain pass done: {len(seen)} unique", flush=True)
    if total_members and len(seen) < total_members * 0.9:
        for q in string.ascii_lowercase + string.digits:
            got = await page_through(ch, q, seen)
            print(f"prefix '{q}': +{got} (total {len(seen)})", flush=True)
    print(f"DONE: {len(seen)} unique from {GROUP}, "
          f"{await count_saved()} rows tagged to this group", flush=True)
    await idle()


# ---------------------------------------------------------------- invite

async def invite():
    if not INVITE_TEXT:
        print("ERROR: set INVITE_TEXT first", flush=True)
        return await idle()

    filters = []
    params = []
    if INVITE_SCOPE_GROUP:
        filters.append("AND m.group_name = %s")
        params.append(GROUP)
    if ACTIVES_ONLY:
        filters.append("AND m.source = 'listen'")
    extra = " ".join(filters)
    params.append(INVITE_DAILY_CAP)

    async with pool.connection() as con:
        cur = await con.execute(
            f"""
            SELECT m.user_id, m.username, m.first_name,
                   CASE WHEN m.source = 'listen' THEN 0 ELSE 1 END AS rank
            FROM members m
            LEFT JOIN outreach o ON o.user_id = m.user_id
            WHERE m.is_bot = false
              AND m.username IS NOT NULL
              AND o.user_id IS NULL
              {extra}
            ORDER BY rank, m.seen_at DESC
            LIMIT %s
            """,
            tuple(params),
        )
        queue = await cur.fetchall()
        cur = await con.execute("SELECT count(*) FROM outreach WHERE status = 'sent'")
        already = (await cur.fetchone())[0]

    if not queue:
        print("nothing left to contact", flush=True)
        return await idle()

    scope = GROUP if INVITE_SCOPE_GROUP else "ALL GROUPS"
    print(f"[{VERSION}] queue: {len(queue)} from {scope} | sent all-time: {already} "
          f"| ~{INVITE_DELAY}-{INVITE_DELAY + INVITE_JITTER}s apart", flush=True)

    sent = skipped = 0
    for user_id, username, first_name, _rank in queue:
        body = (INVITE_TEXT
                .replace("{name}", (first_name or "").strip())
                .replace("{link}", INVITE_LINK))
        try:
            await client.send_message(f"@{username}", body,
                                      link_preview=bool(INVITE_LINK))
            await mark(user_id, "sent")
            sent += 1
            print(f"sent -> @{username} ({sent}/{len(queue)})", flush=True)
        except PeerFloodError:
            await mark(user_id, "aborted", "PeerFloodError")
            print("!! PeerFloodError: Telegram flagged this as spam. STOPPING.", flush=True)
            print("!! Wait 24-48h before trying again.", flush=True)
            break
        except FloodWaitError as e:
            print(f"[floodwait] {e.seconds}s", flush=True)
            await asyncio.sleep(e.seconds + 10)
            continue
        except UserPrivacyRestrictedError:
            await mark(user_id, "skipped", "privacy settings")
            skipped += 1
            print(f"skip -> @{username} (privacy)", flush=True)
        except UserIsBlockedError:
            await mark(user_id, "skipped", "blocked")
            skipped += 1
            print(f"skip -> @{username} (blocked)", flush=True)
        except (UserIdInvalidError, InputUserDeactivatedError):
            await mark(user_id, "skipped", "dead account")
            skipped += 1
            print(f"skip -> @{username} (dead)", flush=True)
        except ValueError as e:
            await mark(user_id, "skipped", f"unresolvable: {e}")
            skipped += 1
            print(f"skip -> @{username} (cannot resolve)", flush=True)
        except Exception as e:
            await mark(user_id, "error", str(e))
            print(f"error -> @{username}: {type(e).__name__}: {e}", flush=True)
        await nap(INVITE_DELAY, INVITE_JITTER)

    print(f"RUN DONE: {sent} sent, {skipped} skipped", flush=True)
    await idle()


# ---------------------------------------------------------------- listen

async def listen():
    ch = await client.get_entity(GROUP)

    @client.on(events.NewMessage(chats=ch))
    async def _(ev):
        s = await ev.get_sender()
        if s and not getattr(s, "bot", False):
            await save([s], "listen")
            print(f"+ {s.id} @{s.username}", flush=True)

    print(f"listening on {GROUP}...", flush=True)
    await client.run_until_disconnected()


async def main():
    global pool
    print(f"=== tg-harvest {VERSION} starting | MODE={MODE} | GROUP={GROUP} ===", flush=True)
    pool = AsyncConnectionPool(DB_URL, min_size=1, max_size=3, open=False)
    await pool.open()
    async with pool.connection() as con:
        await con.execute(SCHEMA)
        # Tag pre-v4 rows with the first group they came from.
        await con.execute(BACKFILL, (os.getenv("BACKFILL_GROUP", GROUP),))
    await client.start()
    if MODE == "scrape":
        await scrape()
    elif MODE == "invite":
        await invite()
    else:
        await listen()


asyncio.run(main())
