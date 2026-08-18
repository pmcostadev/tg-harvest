import os, asyncio, string
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


async def scrape():
    ch = await client.get_entity(GROUP)
    seen, total = set(), 0
    # empty query first, then a-z / 0-9 prefixes: each gets its own ~10k budget
    for q in [""] + list(string.ascii_lowercase + string.digits):
        offset = 0
        while True:
            try:
                res = await client(GetParticipantsRequest(
                    ch, ChannelParticipantsSearch(q), offset, 200, hash=0))
            except FloodWaitError as e:
                print(f"floodwait {e.seconds}s", flush=True)
                await asyncio.sleep(e.seconds + 5)
                continue
            if not res.users:
                break
            fresh = [u for u in res.users if u.id not in seen]
            seen.update(u.id for u in fresh)
            if fresh:
                await save(fresh, f"scrape:{q or '_'}")
            total += len(fresh)
            offset += len(res.users)
            print(f"q='{q}' offset={offset} unique={total}", flush=True)
            await asyncio.sleep(2)
    print(f"done: {total} unique members", flush=True)


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
