"""
Temp voice bot v2: join a "creator" (lobby) voice channel -> get your own temporary room.
Any number of creators, each with its own user limit and room name. Rooms are deleted when empty.

What v2 does about Discord's channel-creation rate limit:
- Works around a discord.py bug that made the bot refuse to create channels for ~24h after
  creating a single one (see patch_ratelimit below).
- An empty room lingers for GRACE_SECONDS (visible, not hidden) and is handed to the next
  person who joins the same creator, so quick leave/rejoin does not cost a delete + create.
- If Discord really blocks creation, the bot says so, stops retrying until the block lifts,
  and keeps empty rooms around for reuse in the meantime.
- Every channel creation and every 429 response is logged with the raw rate-limit headers.

Safety notes:
- Token comes from the DISCORD_TOKEN environment variable or token.txt (never hard-coded).
- Only the intents it needs (guilds + voice states). No privileged intents.
- Room owners never get Discord permissions on the channel; every change goes through
  the bot's own commands.
- Admin commands require "Manage Server".
"""

import asyncio
import json
import logging
import os
import random
import time
from collections import defaultdict
from pathlib import Path

import aiohttp
import discord
from discord import app_commands

TOKEN = os.environ.get("DISCORD_TOKEN")
DATA_FILE = Path(os.environ.get("DATA_FILE", Path(__file__).with_name("data.json")))
GRACE_SECONDS = float(os.environ.get("GRACE_SECONDS", 40))  # how long an empty room stays before deletion

# Easter egg: chance (0.05 = 5%) that a new room gets one of these names instead of the normal one.
# {n} = room number, {user} = name of whoever created it. Leave the list empty to turn it off.
SPECIAL_NAME_CHANCE = 0.01
SPECIAL_NAME_MESSAGE = "👀"  # sent in the room's chat when a special name is rolled ("" = don't send)
SPECIAL_NAMES = [
    "Toca do Amor 🐺",
    "Plantação de Kouve 🌱",
    "Shrimp fritinho 🦐",
    "Rápida do besouro 🪲",
    "Matrix do PH 😎",
    "Safadeza do Bellion 🫦",
    "Golpe de Estado?! ⛓️‍💥",
]
CREATE_COOLDOWN_SECONDS = 10  # anti-spam: one room per user every 10s
NOTICE_COOLDOWN_SECONDS = 15  # at most one chat notice per creator in this interval
NOTICE_LIFETIME_SECONDS = 60  # notices delete themselves

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tempvoice")

# discord.RateLimited is NOT a subclass of HTTPException, so API calls must catch both
API_ERRORS = (discord.HTTPException, discord.RateLimited)


# ---------- discord.py rate-limit workaround ----------

def patch_ratelimit() -> None:
    """Work around a discord.py bug (present in 2.7.1 and in master as of this writing).

    When the client is built with max_ratelimit_timeout, Ratelimit.acquire() raises RateLimited
    whenever the bucket's reset time is further away than that timeout, without checking whether
    the bucket still has requests left. Channel creation uses a ~24h bucket, so after one
    successful create every further create failed locally with "Retry in 8xxxx seconds", although
    Discord had not refused anything. Here: if the bucket still has requests left, just take one.
    """
    from discord.http import Ratelimit

    if getattr(Ratelimit.acquire, "_tempvoice_patched", False):
        return
    original = Ratelimit.acquire

    async def acquire(self) -> None:
        if self.remaining > 0 and not self.is_expired():
            self._last_request = self._loop.time()
            self.remaining -= 1
            self.outgoing += 1
            return
        await original(self)

    acquire._tempvoice_patched = True
    Ratelimit.acquire = acquire


async def ratelimit_patch_works() -> bool:
    """Self-test, so a future discord.py that changes its internals is noticed in the log."""
    try:
        from discord.http import Ratelimit

        bucket = Ratelimit(30.0)
        bucket.limit, bucket.remaining = 5, 4
        bucket.expires = asyncio.get_running_loop().time() + 86400  # far-away reset, requests left
        await bucket.acquire()
        return bucket.remaining == 3
    except Exception:  # RateLimited (bug still active) or changed internals
        return False


try:
    patch_ratelimit()
except Exception:
    log.exception("Could not apply the discord.py rate-limit workaround")


# ---------- storage ----------

def load_data() -> dict:
    data = {}
    if DATA_FILE.exists():
        try:
            data = json.loads(DATA_FILE.read_text("utf-8"))
        except (OSError, json.JSONDecodeError):
            log.exception("Could not read %s, starting empty", DATA_FILE)
    # anything else in an old file (e.g. v1's "pool") is dropped
    return {"creators": data.get("creators", {}), "rooms": data.get("rooms", {})}


def save_data(data: dict) -> None:
    tmp = DATA_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2), "utf-8")
    tmp.replace(DATA_FILE)  # atomic write, file never ends up half-written


def fmt_duration(seconds: float) -> str:
    minutes = max(1, round(seconds / 60))
    return f"{minutes} min" if minutes < 90 else f"{minutes / 60:.1f} h"


# ---------- bot ----------

class TempVoiceBot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.none()
        intents.guilds = True
        intents.voice_states = True
        trace = aiohttp.TraceConfig()
        trace.on_request_end.append(self.on_http_response)
        super().__init__(
            intents=intents,
            max_messages=None,  # no message cache: less RAM
            max_ratelimit_timeout=30.0,  # never hang for minutes on a rate limit; raise RateLimited instead
            http_trace=trace,
        )
        self.tree = app_commands.CommandTree(self)
        self.data = load_data()
        self.cooldowns: dict[int, float] = {}  # member id -> monotonic time of last room
        self.blocked_until: dict[int, float] = {}  # guild id -> monotonic time creation may be retried
        self.timers: dict[str, asyncio.Task] = {}  # room id -> pending deletion of an empty room
        self.locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)  # one join at a time per creator
        self.last_notice: dict[int, float] = {}
        self.background: set[asyncio.Task] = set()
        self.created = 0  # channels created since start
        self.last_create_info = "nenhuma criação desde que o bot ligou"
        self.patch_ok = False

    async def setup_hook(self) -> None:
        self.patch_ok = await ratelimit_patch_works()
        if not self.patch_ok:
            log.error("discord.py rate-limit workaround is NOT active; channel creation may stop after one room")
        self.tree.add_command(CreatorGroup(self))
        self.tree.add_command(RoomGroup(self))
        await self.tree.sync()

    async def on_ready(self) -> None:
        log.info("Logged in as %s (discord.py %s, grace %.0fs)", self.user, discord.__version__, GRACE_SECONDS)
        await self.cleanup()

    # -- diagnostics --

    async def on_http_response(self, session, ctx, params) -> None:
        """Log the raw rate-limit headers of every channel creation and every 429."""
        response, path = params.response, params.url.path
        is_create = params.method == "POST" and "/guilds/" in path and path.endswith("/channels")
        if not is_create and response.status != 429:
            return
        h = response.headers
        info = (
            f"{params.method} {path} -> {response.status} | limit={h.get('X-RateLimit-Limit')} "
            f"remaining={h.get('X-RateLimit-Remaining')} reset_after={h.get('X-RateLimit-Reset-After')} "
            f"bucket={h.get('X-RateLimit-Bucket')} scope={h.get('X-RateLimit-Scope')} "
            f"global={h.get('X-RateLimit-Global')} retry_after={h.get('Retry-After')}"
        )
        if is_create:
            self.last_create_info = info
        log.log(logging.WARNING if response.status == 429 else logging.INFO, "Rate-limit headers: %s", info)

    # -- helpers --

    def is_room(self, channel) -> bool:
        return channel is not None and str(channel.id) in self.data["rooms"]

    def is_creator(self, channel) -> bool:
        return channel is not None and str(channel.id) in self.data["creators"]

    def humans_in(self, channel) -> list[discord.Member]:
        return [m for m in channel.members if not m.bot]

    def block_left(self, guild_id: int) -> float:
        """Seconds until channel creation may be tried again (0 = not blocked)."""
        return max(0.0, self.blocked_until.get(guild_id, 0) - time.monotonic())

    def spawn(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self.background.add(task)
        task.add_done_callback(self.background.discard)
        return task

    def room_name(self, cfg: dict, member, n: int) -> tuple[str, bool]:
        """Returns (name, special): special=True when the easter-egg name was rolled."""
        template, special = cfg["name"], False
        if SPECIAL_NAMES and random.random() < SPECIAL_NAME_CHANCE:
            template, special = random.choice(SPECIAL_NAMES), True  # easter egg
        return template.replace("{user}", member.display_name).replace("{n}", str(n))[:100], special

    async def send_eyes(self, room) -> None:
        """Easter egg: drop a 👀 in the room's chat. Best effort, silently skipped without permission."""
        if not SPECIAL_NAME_MESSAGE:
            return
        try:
            perms = room.permissions_for(room.guild.me)
            if perms.view_channel and perms.send_messages:
                await room.send(SPECIAL_NAME_MESSAGE)
        except Exception:
            log.info("Could not send the easter-egg message in %s", room.id, exc_info=True)

    def next_number(self, creator_id: str) -> int:
        used = {r.get("n") for r in self.data["rooms"].values() if r.get("creator") == creator_id}
        n = 1
        while n in used:
            n += 1
        return n

    async def notify(self, lobby, text: str) -> None:
        """Best effort: post a short self-deleting notice in the creator channel's built-in chat."""
        now = time.monotonic()
        if now - self.last_notice.get(lobby.id, -1e9) < NOTICE_COOLDOWN_SECONDS:
            return
        try:
            perms = lobby.permissions_for(lobby.guild.me)
            if not (perms.view_channel and perms.send_messages):
                return
            self.last_notice[lobby.id] = now
            await lobby.send(
                text,
                delete_after=NOTICE_LIFETIME_SECONDS,
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=True),
            )
        except Exception:  # never let a notice break the voice flow
            log.info("Could not post a notice in %s", lobby.id, exc_info=True)

    async def turn_away(self, member, lobby, reason: str, notice: str) -> None:
        """Disconnect someone the bot cannot give a room to, and tell them why."""
        try:
            await member.move_to(None, reason=reason)
        except API_ERRORS:
            pass
        await self.notify(lobby, f"{member.mention} {notice}")

    # -- empty rooms: linger for a while, then delete --

    def linger(self, channel) -> None:
        room_id = str(channel.id)
        if room_id not in self.timers:
            self.timers[room_id] = self.spawn(self.delete_later(channel))

    def keep(self, room_id: str) -> None:
        timer = self.timers.pop(room_id, None)
        if timer is not None:
            timer.cancel()

    async def delete_later(self, channel) -> None:
        room_id = str(channel.id)
        await asyncio.sleep(GRACE_SECONDS)
        # while Discord refuses new channels, empty rooms are kept so they can be reused
        while self.block_left(channel.guild.id) and self.data["rooms"].get(room_id, {}).get("creator") in self.data["creators"]:
            await asyncio.sleep(GRACE_SECONDS or 5)
        # from here to the delete call nothing awaits, so a reuse cannot grab a room being deleted
        self.timers.pop(room_id, None)
        if room_id not in self.data["rooms"] or self.humans_in(channel):
            return
        self.data["rooms"].pop(room_id)
        save_data(self.data)
        try:
            await channel.delete(reason="Sala temporária vazia")
        except discord.NotFound:
            pass
        except API_ERRORS:
            log.exception("Could not delete room %s", room_id)

    async def cleanup(self) -> None:
        """After a (re)connect: forget channels that no longer exist, schedule empty rooms for deletion."""
        for room_id in list(self.data["rooms"]):
            channel = self.get_channel(int(room_id))
            if channel is None:
                self.data["rooms"].pop(room_id, None)
                self.keep(room_id)
            elif not self.humans_in(channel):
                self.linger(channel)
        for creator_id in list(self.data["creators"]):
            if self.get_channel(int(creator_id)) is None:
                self.data["creators"].pop(creator_id, None)
        save_data(self.data)
        # people who were waiting in a creator while the bot was offline
        for creator_id in list(self.data["creators"]):
            lobby = self.get_channel(int(creator_id))
            for member in self.humans_in(lobby) if lobby else []:
                await self.enter_lobby(member, lobby)

    # -- events --

    async def on_voice_state_update(self, member, before, after) -> None:
        if before.channel == after.channel:
            return
        if self.is_room(after.channel) and not member.bot:
            self.keep(str(after.channel.id))
        if self.is_room(before.channel) and not self.humans_in(before.channel):
            self.linger(before.channel)
        if self.is_creator(after.channel) and not member.bot:
            await self.enter_lobby(member, after.channel)

    async def on_guild_channel_delete(self, channel) -> None:
        room_id = str(channel.id)
        self.keep(room_id)
        changed = self.data["rooms"].pop(room_id, None) is not None
        changed |= self.data["creators"].pop(room_id, None) is not None
        if changed:
            save_data(self.data)

    # -- giving someone a room --

    async def enter_lobby(self, member, lobby) -> None:
        if not (member.voice and member.voice.channel == lobby):
            return
        wait = CREATE_COOLDOWN_SECONDS - (time.monotonic() - self.cooldowns.get(member.id, -1e9))
        if wait > 0:
            await self.turn_away(member, lobby, "Criando salas rápido demais",
                                 f"calma! Espere {wait:.0f}s antes de criar outra sala.")
            return
        async with self.locks[lobby.id]:
            if not self.is_creator(lobby) or not (member.voice and member.voice.channel == lobby):
                return  # left (or creator removed) while waiting for the lock
            self.cooldowns[member.id] = time.monotonic()
            if len(self.cooldowns) > 5000:
                self.cooldowns.clear()
            if not await self.reuse_room(member, lobby):
                await self.create_room(member, lobby)

    async def reuse_room(self, member, lobby) -> bool:
        """Hand over a room of this creator that emptied a moment ago, instead of creating a channel."""
        creator_id = str(lobby.id)
        cfg = self.data["creators"][creator_id]
        free = [rid for rid in self.timers if self.data["rooms"].get(rid, {}).get("creator") == creator_id]
        free.sort(key=lambda rid: self.data["rooms"][rid]["owner"] != member.id)  # their own old room first
        for room_id in free:
            room = self.get_channel(int(room_id))
            if room is None or self.humans_in(room):
                continue
            info = self.data["rooms"][room_id]
            self.keep(room_id)  # taken before any await, so nobody else gets it
            same_owner = info["owner"] == member.id
            info["owner"] = member.id
            save_data(self.data)
            try:
                if not same_owner and (room.overwrites != lobby.overwrites or room.user_limit != cfg["limit"]):
                    await room.edit(overwrites=lobby.overwrites, user_limit=cfg["limit"], reason=f"Sala de {member}")
                await member.move_to(room, reason="Sala temporária (reaproveitada)")
            except API_ERRORS as e:
                log.info("Could not reuse room %s: %s", room_id, e)
                if self.is_room(room) and not self.humans_in(room):
                    self.linger(room)
                if member.voice and member.voice.channel == lobby:
                    continue  # still waiting: try another room, or create one
                return True  # they left; nothing more to do
            name, special = self.room_name(cfg, member, info.get("n", 1))
            if not same_owner and room.name != name:
                # renames are limited to 2 per 10 min: don't wait
                self.spawn(self.rename_quietly(room, name, special))
            log.info("Reused room %s for %s", room_id, member.id)
            return True
        return False

    async def rename_quietly(self, room, name: str, special: bool = False) -> None:
        try:
            await room.edit(name=name, reason="Sala reaproveitada")
        except API_ERRORS:
            return
        if special:
            await self.send_eyes(room)

    async def create_room(self, member, lobby) -> None:
        guild = lobby.guild
        left = self.block_left(guild.id)
        if left:
            await self.turn_away(member, lobby, "Limite do Discord para criar canais", self.blocked_text(left))
            return

        creator_id = str(lobby.id)
        cfg = self.data["creators"][creator_id]
        n = self.next_number(creator_id)
        category = guild.get_channel(cfg["category"]) if cfg.get("category") else None
        if not isinstance(category, discord.CategoryChannel):
            category = lobby.category  # no category chosen, or it was deleted: use the lobby's
        name, special = self.room_name(cfg, member, n)
        try:
            room = await guild.create_voice_channel(
                name=name,
                category=category,
                overwrites=lobby.overwrites,  # same visibility rules as the creator
                user_limit=cfg["limit"],
                bitrate=min(lobby.bitrate, guild.bitrate_limit),
                reason=f"Sala temporária de {member}",
            )
        except API_ERRORS as e:
            if isinstance(e, discord.RateLimited) or e.status == 429:
                # Discord refuses new channels for a while: stop trying until it lifts
                # (a 429 that is not a normal rate limit, e.g. from Cloudflare, has no retry time: wait 10 min)
                wait = getattr(e, "retry_after", None) or float(e.response.headers.get("Retry-After", 600))
                self.blocked_until[guild.id] = time.monotonic() + wait
                log.warning("Channel creation blocked in guild %s for %.0fs (workaround active: %s, "
                            "created since start: %d). Last create: %s",
                            guild.id, wait, self.patch_ok, self.created, self.last_create_info)
                await self.turn_away(member, lobby, "Limite do Discord para criar canais", self.blocked_text(wait))
                return
            log.error("Could not create room in guild %s: %s", guild.id, e)
            why = ("o bot está sem permissão para criar canais aqui." if isinstance(e, discord.Forbidden)
                   else "o servidor ou a categoria atingiu o limite de canais?")
            await self.turn_away(member, lobby, "Não foi possível criar a sala",
                                 f"não consegui criar a sala: {why} Avise a moderação.")
            return

        self.created += 1
        self.data["rooms"][str(room.id)] = {"owner": member.id, "creator": creator_id, "n": n}
        save_data(self.data)
        try:
            await member.move_to(room, reason="Sala temporária")
        except API_ERRORS:
            self.linger(room)  # left before the room was ready: keep it for the next person for a while
            return
        if special:
            self.spawn(self.send_eyes(room))

    def blocked_text(self, seconds: float) -> str:
        return (f"o Discord limitou a criação de canais por enquanto. Tente de novo em ~{fmt_duration(seconds)} "
                f"ou entre em uma sala que já existe.")


# ---------- admin commands: /criador ----------

class CreatorGroup(app_commands.Group):
    def __init__(self, bot: TempVoiceBot) -> None:
        super().__init__(
            name="criador",
            description="Configurar canais de criação de sala",
            default_permissions=discord.Permissions(manage_guild=True),
            guild_only=True,
        )
        self.bot = bot

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("Você precisa de **Gerenciar Servidor**.", ephemeral=True)
            return False
        return True

    @app_commands.command(name="adicionar", description="Transforma um canal de voz em criador de salas")
    @app_commands.describe(
        canal="Canal de voz que as pessoas entram para criar a sala",
        limite="Máximo de pessoas na sala (0 = sem limite)",
        nome="Nome da sala. Use {n} para o número e {user} para o nome de quem criou",
        categoria="Categoria onde as salas serão criadas (padrão: a mesma do canal criador)",
    )
    async def adicionar(
        self,
        interaction: discord.Interaction,
        canal: discord.VoiceChannel,
        limite: app_commands.Range[int, 0, 99],
        nome: app_commands.Range[str, 1, 90] = "Sala de {user}",
        categoria: discord.CategoryChannel | None = None,
    ) -> None:
        if self.bot.is_room(canal):
            await interaction.response.send_message("Esse canal é uma sala, não pode ser criador.", ephemeral=True)
            return
        self.bot.data["creators"][str(canal.id)] = {
            "limit": limite, "name": nome, "category": categoria.id if categoria else None,
        }
        save_data(self.bot.data)
        onde = f" na categoria **{categoria.name}**" if categoria else ""
        await interaction.response.send_message(
            f"✅ {canal.mention} agora cria salas `{nome}` com limite **{limite or 'nenhum'}**{onde}.", ephemeral=True
        )

    @app_commands.command(name="remover", description="Para de usar um canal como criador")
    async def remover(self, interaction: discord.Interaction, canal: discord.VoiceChannel) -> None:
        if self.bot.data["creators"].pop(str(canal.id), None) is None:
            await interaction.response.send_message("Esse canal não é um criador.", ephemeral=True)
            return
        save_data(self.bot.data)
        await interaction.response.send_message(f"🗑️ {canal.mention} não cria mais salas.", ephemeral=True)

    @app_commands.command(name="listar", description="Mostra os criadores deste servidor")
    async def listar(self, interaction: discord.Interaction) -> None:
        lines = []
        for cid, cfg in self.bot.data["creators"].items():
            ch = interaction.guild.get_channel(int(cid))
            if ch is not None:
                cat = interaction.guild.get_channel(cfg["category"]) if cfg.get("category") else None
                onde = f", categoria **{cat.name}**" if cat else ""
                lines.append(f"• {ch.mention}: limite {cfg['limit'] or 'nenhum'}, nome `{cfg['name']}`{onde}")
        await interaction.response.send_message("\n".join(lines) or "Nenhum criador configurado.", ephemeral=True)

    @app_commands.command(name="status", description="Diagnóstico: salas abertas e limite do Discord")
    async def status(self, interaction: discord.Interaction) -> None:
        bot, guild = self.bot, interaction.guild
        rooms = [rid for rid in bot.data["rooms"] if guild.get_channel(int(rid)) is not None]
        left = bot.block_left(guild.id)
        lines = [
            f"Salas abertas: **{len(rooms)}** ({sum(r in bot.timers for r in rooms)} vazias aguardando reuso/exclusão)",
            f"Canais criados desde que o bot ligou: **{bot.created}**",
            f"Criação de canais: {'⛔ bloqueada por ~' + fmt_duration(left) if left else '✅ liberada'}",
            f"Correção do discord.py: {'✅ ativa' if bot.patch_ok else '⚠️ inativa'} (discord.py {discord.__version__})",
            f"Última criação: `{bot.last_create_info}`",
        ]
        await interaction.response.send_message("\n".join(lines), ephemeral=True)


# ---------- owner commands: /sala ----------

class RoomGroup(app_commands.Group):
    def __init__(self, bot: TempVoiceBot) -> None:
        super().__init__(name="sala", description="Gerenciar a sua sala temporária", guild_only=True)
        self.bot = bot

    async def get_room(self, interaction: discord.Interaction, must_own: bool = True):
        voice = interaction.user.voice
        channel = voice.channel if voice else None
        if not self.bot.is_room(channel):
            await interaction.response.send_message("Entre na sua sala temporária primeiro.", ephemeral=True)
            return None
        if must_own and self.bot.data["rooms"][str(channel.id)]["owner"] != interaction.user.id:
            await interaction.response.send_message(
                "Só o dono da sala pode fazer isso. Se o dono saiu, use `/sala assumir`.", ephemeral=True
            )
            return None
        return channel

    async def edit(self, interaction, channel, success: str, **kwargs) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            await channel.edit(**kwargs, reason=f"/sala por {interaction.user}")
            await interaction.followup.send(success, ephemeral=True)
        except discord.RateLimited as e:
            await interaction.followup.send(
                f"O Discord limita alterações em canais (renomear: 2 vezes a cada 10 min). "
                f"Tente de novo em ~{fmt_duration(e.retry_after)}.", ephemeral=True
            )
        except discord.HTTPException:
            await interaction.followup.send("Não consegui alterar a sala.", ephemeral=True)

    @app_commands.command(name="renomear", description="Muda o nome da sua sala")
    async def renomear(self, interaction: discord.Interaction, nome: app_commands.Range[str, 1, 100]) -> None:
        if channel := await self.get_room(interaction):
            await self.edit(interaction, channel, f"✏️ Sala renomeada para **{nome}**.", name=nome)

    @app_commands.command(name="limite", description="Muda o máximo de pessoas (0 = sem limite)")
    async def limite(self, interaction: discord.Interaction, limite: app_commands.Range[int, 0, 99]) -> None:
        if channel := await self.get_room(interaction):
            await self.edit(interaction, channel, f"👥 Limite: **{limite or 'nenhum'}**.", user_limit=limite)

    @app_commands.command(name="trancar", description="Ninguém novo pode entrar")
    async def trancar(self, interaction: discord.Interaction) -> None:
        if channel := await self.get_room(interaction):
            everyone = channel.overwrites_for(interaction.guild.default_role)
            everyone.connect = False
            owner = channel.overwrites_for(interaction.user)
            owner.connect = True
            overwrites = {**channel.overwrites, interaction.guild.default_role: everyone, interaction.user: owner}
            await self.edit(interaction, channel, "🔒 Sala trancada.", overwrites=overwrites)

    @app_commands.command(name="destrancar", description="Qualquer um pode entrar de novo")
    async def destrancar(self, interaction: discord.Interaction) -> None:
        if channel := await self.get_room(interaction):
            everyone = channel.overwrites_for(interaction.guild.default_role)
            everyone.connect = None
            overwrites = {**channel.overwrites, interaction.guild.default_role: everyone}
            await self.edit(interaction, channel, "🔓 Sala destrancada.", overwrites=overwrites)

    @app_commands.command(name="permitir", description="Deixa alguém entrar mesmo com a sala trancada")
    async def permitir(self, interaction: discord.Interaction, membro: discord.Member) -> None:
        if channel := await self.get_room(interaction):
            ow = channel.overwrites_for(membro)
            ow.connect = True
            await self.edit(interaction, channel, f"✅ {membro.mention} pode entrar.",
                            overwrites={**channel.overwrites, membro: ow})

    @app_commands.command(name="expulsar", description="Tira alguém da sua sala e impede que volte")
    async def expulsar(self, interaction: discord.Interaction, membro: discord.Member) -> None:
        channel = await self.get_room(interaction)
        if not channel:
            return
        if membro.id == interaction.user.id or membro.bot:
            await interaction.response.send_message("Não dá para expulsar esse membro.", ephemeral=True)
            return
        if channel.permissions_for(membro).move_members:
            await interaction.response.send_message("Não dá para expulsar a moderação.", ephemeral=True)
            return
        ow = channel.overwrites_for(membro)
        ow.connect = False
        await self.edit(interaction, channel, f"👢 {membro.mention} foi expulso da sala.",
                        overwrites={**channel.overwrites, membro: ow})
        if membro.voice and membro.voice.channel == channel:
            try:
                await membro.move_to(None, reason=f"Expulso da sala por {interaction.user}")
            except API_ERRORS:
                pass

    @app_commands.command(name="assumir", description="Vira dono da sala se o dono saiu")
    async def assumir(self, interaction: discord.Interaction) -> None:
        channel = await self.get_room(interaction, must_own=False)
        if not channel:
            return
        room = self.bot.data["rooms"][str(channel.id)]
        if any(m.id == room["owner"] for m in channel.members):
            await interaction.response.send_message("O dono ainda está na sala.", ephemeral=True)
            return
        room["owner"] = interaction.user.id
        save_data(self.bot.data)
        await interaction.response.send_message("👑 Agora você é o dono da sala.", ephemeral=True)

    @app_commands.command(name="transferir", description="Passa a sala para outra pessoa que está nela")
    async def transferir(self, interaction: discord.Interaction, membro: discord.Member) -> None:
        channel = await self.get_room(interaction)
        if not channel:
            return
        if membro.bot or not (membro.voice and membro.voice.channel == channel):
            await interaction.response.send_message("Essa pessoa precisa estar na sala.", ephemeral=True)
            return
        self.bot.data["rooms"][str(channel.id)]["owner"] = membro.id
        save_data(self.bot.data)
        await interaction.response.send_message(f"👑 {membro.mention} agora é o dono da sala.")


if __name__ == "__main__":
    # Fallback for hosts without environment variables: a token.txt file next to bot.py
    token_file = Path(__file__).with_name("token.txt")
    if not TOKEN and token_file.exists():
        TOKEN = token_file.read_text("utf-8").strip()
    if not TOKEN:
        raise SystemExit("Set DISCORD_TOKEN or put the token in token.txt next to bot.py.")
    TempVoiceBot().run(TOKEN, log_handler=None)
