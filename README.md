# Temp Voice Bot

Members join a "creator" voice channel (e.g. "➕ Criar sala 2v2") and get their own
temporary room, which is deleted when everyone leaves. You can have as many creators
as you want, each with its own user limit and room name. The bot's replies are in Portuguese.

## 1. Create the bot on Discord

1. Go to https://discord.com/developers/applications → **New Application**.
2. Open **Bot** → **Reset Token** → copy the token. Treat it like a password: never post it.
3. On the same page, turn **Public Bot** off so only you can add it to servers.
4. No privileged intents are needed. Leave all three switches off.

## 2. Invite it to your server

Replace `YOUR_APP_ID` (shown under **General Information**) and open the link:

```
https://discord.com/oauth2/authorize?client_id=YOUR_APP_ID&scope=bot+applications.commands&permissions=286262288
```

That grants only: View Channels, Manage Channels, Manage Permissions, Connect and Move Members.
If you don't want a "just joined" announcement, turn off system welcome messages before inviting it.

## 3. Run it

Needs Python 3.10+.

```bash
pip install -r requirements.txt
export DISCORD_TOKEN="paste-your-token-here"     # Windows PowerShell: $env:DISCORD_TOKEN="..."
python bot.py
```

The "PyNaCl is not installed" warning is harmless: the bot doesn't play audio.
Settings are saved to `data.json` next to the bot, so they survive restarts.

The bot only works while this program is running, so it needs a machine that stays on:
your own PC, a Raspberry Pi, or a small VPS / free cloud tier.

## 4. Set up the creators (admins, needs "Manage Server")

Create a voice channel for each match type, inside the category where you want the
rooms to appear, then run:

```
/criador adicionar canal:➕ Criar sala 2v2 limite:4  nome:2v2 #{n}
/criador adicionar canal:➕ Criar sala 3v3 limite:6  nome:3v3 #{n}
/criador adicionar canal:➕ Criar sala 5v5 limite:10 nome:5v5 #{n}
```

- `{n}` becomes the room number (2v2 #1, 2v2 #2…), `{user}` becomes the creator's name.
- New rooms copy the creator channel's permissions, so whoever can see the creator can see the rooms.
- `/criador listar` shows all creators, `/criador remover` stops one.

## Commands for room owners

| Command | What it does |
|---|---|
| `/sala renomear` | Rename the room (Discord allows 2 renames per 10 min) |
| `/sala limite` | Change the max players |
| `/sala trancar` / `/sala destrancar` | Lock / unlock the room |
| `/sala permitir` | Let a specific person in while locked |
| `/sala expulsar` | Remove someone and block them from rejoining (can't be used on mods) |
| `/sala assumir` | Take ownership if the owner left |
| `/sala transferir` | Give ownership to someone in the room |

## Safety

- The token is only read from an environment variable, never stored in the code.
- The bot requests no privileged intents: it can't read messages or the member list.
- Room owners get no Discord permissions; all changes go through the bot's checks.
- Each user can create at most one room every 10 seconds to prevent spam.
