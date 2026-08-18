# tg-harvest

Telegram group member harvester. Telethon (MTProto user client) + Postgres, runs as a background worker on Railway.

## Deploy

1. Railway -> New Project -> Deploy from GitHub repo -> this repo
2. Right-click canvas -> Database -> Add PostgreSQL
3. App service -> Variables -> Raw Editor:

```
TG_API_ID=...
TG_API_HASH=...
TG_SESSION=...
TG_GROUP=@group_username
MODE=listen
DATABASE_URL=${{Postgres.DATABASE_URL}}
```

4. Settings -> Deploy -> Custom Start Command: `python main.py`
5. Do NOT generate a domain. This is a worker, not a web service.

## Modes

- `MODE=listen` — records every non-bot account that posts. No rate limits, runs forever, gives you active members.
- `MODE=scrape` — one-shot bulk pull of the participant list. Paginated with a-z prefixes to get past Telegram's ~10k cap. Expect FloodWait.

Upserts on `user_id`, so re-running is safe.

## Notes

- Never commit `TG_SESSION`. It is full login access to the account.
- `phone` is not collected: Telegram hides it for ~100% of users by default.
