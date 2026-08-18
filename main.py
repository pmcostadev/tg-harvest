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

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION = os.environ["TG_SESSION"]
GROUP = os.environ["TG_GROUP"]
MODE = os.getenv("MODE", "listen")
DB_URL = os.environ["DATABASE_URL"]

# scrape pacing
PAGE = int(os.getenv("PAGE_SIZE", "100"))
DELAY = float(os.getenv("DELAY", "8"))
JITTER = float(os.getenv("JITTER", "4"))

# invite pacing
INVITE_LINK = os.getenv("INVITE_LINK", "")
INVITE_TEXT = os.getenv("INVITE_TEXT", "")
INVITE_DAILY_CAP = int(os.getenv("INVITE_DAILY_CAP", "25"))
INVITE_DELAY = float(os.getenv("INVITE_DELAY", "90"))
INVITE_JITTER = float(os.getenv("INVITE_JITTER", "60"))

client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)
pool = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS members (
  user_id BIGINT PRIMARY KEY,
  username TEXT, first_name TEXT, last_name TEXT,
  is_bot BOOLEAN, source TEXT,
  seen_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS outreach (
  user_id BIGINT PRIMARY KEY,
  status TEXT,
  detail TEXT,
  sent_at TIMESTAMPTZ DEFAULT now()
);
"""

INSERT = """
INSERT INTO members (user_id, username, first_name, last_name, is_bot, source)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username
"""


async def save(users, source):
    rows = [(u.id, u.username, u.first_name, u.last_name,
             bool(getattr(u, "bot", False)), source) for u in users]
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
        cur = await con.execute("SELECT count(*) FROM members")
        return (await cur.fetchone())[0]


async def nap(base=None, jit=None):
    base = DELAY if base is None else base
    jit = JITTER if jit is None else jit
    await asyncio.sleep(base + random.random() * jit)


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
    print(f"pacing: {PAGE}/request, ~{DELAY}-{DELAY + JITTER}s between requests", flush=True)

    seen = set()
    await page_through(ch, "", seen)
    print(f"plain pass done: {len(seen)} unique", flush=True)

    if total_members and len(seen) < total_members * 0.9:
        print("gap detected, running prefix passes", flush=True)
        for q in string.ascii_lowercase + string.digits:
            got = await page_through(ch, q, seen)
            print(f"prefix '{q}': +{got} (total {len(seen)})", flush=True)

    print(f"DONE: {len(seen)} unique members, {await count_saved()} rows in db", flush=True)
    print("switch MODE back to listen when you are ready", flush=True)
    await idle()


# ---------------------------------------------------------------- invite

async def invite():
    """One-time migration DM. Each person decides for themselves whether to join."""
    if not INVITE_LINK or not INVITE_TEXT:
        print("ERROR: set INVITE_LINK and INVITE_TEXT first", flush=True)
        return await idle()
    if "{link}" not in INVITE_TEXT:
        print("ERROR: INVITE_TEXT must contain {link}", flush=True)
        return await idle()

    # Actives first: people who actually talk are the ones worth moving.
    async with pool.connection() as con:
        cur = await con.execute(
            """
            SELECT m.user_id, m.username, m.first_name,
                   CASE WHEN m.source = 'listen' THEN 0 ELSE 1 END AS rank
            FROM members m
            LEFT JOIN outreach o ON o.user_id = m.user_id
            WHERE m.is_bot = false
              AND m.username IS NOT NULL
              AND o.user_id IS NULL
            ORDER BY rank, m.seen_at DESC
            LIMIT %s
            """,
            (INVITE_DAILY_CAP,),
        )
        queue = await cur.fetchall()

    if not queue:
        print("nothing left to contact today", flush=True)
        return await idle()

    print(f"queue: {len(queue)} people | cap {INVITE_DAILY_CAP}/run "
          f"| ~{INVITE_DELAY}-{INVITE_DELAY + INVITE_JITTER}s apart", flush=True)

    sent = skipped = 0
    for user_id, username, first_name, _rank in queue:
        body = INVITE_TEXT.replace("{name}", first_name or "").replace("{link}", INVITE_LINK)
        try:
            await client.send_message(user_id, body, link_preview=True)
            await mark(user_id, "sent")
            sent += 1
            print(f"sent -> @{username} ({sent}/{len(queue)})", flush=True)
        except PeerFloodError:
            # Telegram's explicit spam warning. Stop, do not push through it.
            await mark(user_id, "aborted", "PeerFloodError")
            print("!! PeerFloodError: Telegram flagged this as spam. Stopping.", flush=True)
            print("!! Wait at least 24-48h. If it repeats, the message itself is the problem.", flush=True)
            break
        except FloodWaitError as e:
            print(f"[floodwait] {e.seconds}s", flush=True)
            await asyncio.sleep(e.seconds + 10)
            continue
        except UserPrivacyRestrictedError:
            await mark(user_id, "skipped", "privacy settings")
            skipped += 1
        except UserIsBlockedError:
            await mark(user_id, "skipped", "blocked")
            skipped += 1
        except (UserIdInvalidError, InputUserDeactivatedError):
            await mark(user_id, "skipped", "dead account")
            skipped += 1
        except Exception as e:
            await mark(user_id, "error", str(e))
            print(f"error -> @{username}: {e}", flush=True)
        await nap(INVITE_DELAY, INVITE_JITTER)

    print(f"RUN DONE: {sent} sent, {skipped} skipped", flush=True)
    print("redeploy tomorrow for the next batch, or set MODE=listen", flush=True)
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

    print("listening...", flush=True)
    await client.run_until_disconnected()


async def idle():
    while True:
        await asyncio.sleep(3600)


async def main():
    global pool
    pool = AsyncConnectionPool(DB_URL, min_size=1, max_size=3, open=False)
    await pool.open()
    async with pool.connection() as con:
        await con.execute(SCHEMA)
    await client.start()
    if MODE == "scrape":
        await scrape()
    elif MODE == "invite":
        await invite()
    else:
        await listen()


asyncio.run(main())
