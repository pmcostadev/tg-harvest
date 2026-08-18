import os, asyncio, random, string
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch
from psycopg_pool import AsyncConnectionPool

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION = os.environ["TG_SESSION"]
GROUP = os.environ["TG_GROUP"]
MODE = os.getenv("MODE", "listen")
DB_URL = os.environ["DATABASE_URL"]

# Tuning knobs for scrape mode.
PAGE = int(os.getenv("PAGE_SIZE", "100"))        # members per request (max 200)
DELAY = float(os.getenv("DELAY", "8"))           # base seconds between requests
JITTER = float(os.getenv("JITTER", "4"))         # random extra 0..JITTER seconds

client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)
pool = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS members (
  user_id BIGINT PRIMARY KEY,
  username TEXT, first_name TEXT, last_name TEXT,
  is_bot BOOLEAN, source TEXT,
  seen_at TIMESTAMPTZ DEFAULT now()
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


async def count_saved():
    async with pool.connection() as con:
        cur = await con.execute("SELECT count(*) FROM members")
        row = await cur.fetchone()
        return row[0]


async def nap():
    await asyncio.sleep(DELAY + random.random() * JITTER)


async def page_through(ch, query, seen):
    """Walk one search query to exhaustion. Returns how many new users it found."""
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
        label = query or "(all)"
        print(f"[{label}] offset={offset} new={found} total={len(seen)}", flush=True)
        await nap()


async def scrape():
    ch = await client.get_entity(GROUP)
    total_members = getattr(ch, "participants_count", None)
    print(f"target: {GROUP} | reported members: {total_members}", flush=True)
    print(f"pacing: {PAGE}/request, ~{DELAY}-{DELAY + JITTER}s between requests", flush=True)

    seen = set()
    await page_through(ch, "", seen)
    print(f"plain pass done: {len(seen)} unique", flush=True)

    # Only bother with the alphabet trick if the plain pass clearly hit a wall.
    if total_members and len(seen) < total_members * 0.9:
        print("gap detected, running prefix passes", flush=True)
        for q in string.ascii_lowercase + string.digits:
            got = await page_through(ch, q, seen)
            print(f"prefix '{q}': +{got} (total {len(seen)})", flush=True)

    print(f"DONE: {len(seen)} unique members, {await count_saved()} rows in db", flush=True)
    print("switch MODE back to listen when you are ready", flush=True)
    # Idle instead of exiting, so Railway does not restart-loop the scrape.
    while True:
        await asyncio.sleep(3600)


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


async def main():
    global pool
    pool = AsyncConnectionPool(DB_URL, min_size=1, max_size=3, open=False)
    await pool.open()
    async with pool.connection() as con:
        await con.execute(SCHEMA)
    await client.start()
    await (scrape() if MODE == "scrape" else listen())


asyncio.run(main())
