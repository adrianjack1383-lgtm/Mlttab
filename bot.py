import os
import re
import asyncio
import random
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set

from sqlalchemy import Column, Integer, String, BigInteger, Text, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker

from telethon import TelegramClient, events, Button
from telethon.errors import UserAlreadyParticipantError, SessionPasswordNeededError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.functions.account import UpdateProfileRequest  # NEW
from telethon.sessions import StringSession

# ---------- Environment ----------
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
OWNER_ID: int = 6474515118
DATABASE_URL = os.environ.get("DATABASE_URL", "")

if not API_ID or not API_HASH or not BOT_TOKEN:
    raise RuntimeError("API_ID / API_HASH / BOT_TOKEN must be set as environment variables.")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL must be set for persistent storage.")

# ---------- Database Models ----------
Base = declarative_base()

class ProfileModel(Base):
    __tablename__ = "profiles"
    owner_id = Column(BigInteger, primary_key=True)
    timer_type = Column(String, default="fixed")
    timer_value = Column(Integer, default=5)

class AccountModel(Base):
    __tablename__ = "accounts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(BigInteger, nullable=False)
    phone = Column(String, nullable=False)
    api_id = Column(Integer, nullable=False)
    api_hash = Column(String, nullable=False)
    password = Column(String, nullable=True)
    session_string = Column(Text, nullable=False)

class SourceChannelModel(Base):
    __tablename__ = "source_channels"
    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(BigInteger, nullable=False)
    channel = Column(String, nullable=False)

class MessageModel(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(BigInteger, nullable=False)
    text = Column(Text, nullable=False)

class TargetChatModel(Base):
    __tablename__ = "target_chats"
    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(BigInteger, nullable=False)
    phone = Column(String, nullable=False)
    chat_id = Column(BigInteger, nullable=False)

class SpecialUserModel(Base):
    __tablename__ = "special_users"
    user_id = Column(BigInteger, primary_key=True)

# ---------- Database Helpers ----------
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"ssl": "require"},
)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def save_profile(owner_id: int, profile_data: 'ProfileData'):
    async with AsyncSessionLocal() as session:
        # Profile
        stmt = select(ProfileModel).where(ProfileModel.owner_id == owner_id)
        p = (await session.execute(stmt)).scalar_one_or_none()
        if not p:
            p = ProfileModel(owner_id=owner_id)
            session.add(p)
        p.timer_type = profile_data.timer_type
        p.timer_value = profile_data.timer_value

        # Accounts
        await session.execute(AccountModel.__table__.delete().where(AccountModel.owner_id == owner_id))
        for acc in profile_data.accounts:
            client = profile_data.user_clients.get(acc.phone)
            session_str = acc.session_string or ""
            if client:
                extracted = extract_session_string(client)
                if extracted:
                    session_str = extracted
            session.add(AccountModel(
                owner_id=owner_id,
                phone=acc.phone,
                api_id=acc.api_id,
                api_hash=acc.api_hash,
                password=acc.password,
                session_string=session_str or ""
            ))

        # Source channels
        await session.execute(SourceChannelModel.__table__.delete().where(SourceChannelModel.owner_id == owner_id))
        for ch in profile_data.source_channels:
            session.add(SourceChannelModel(owner_id=owner_id, channel=ch))

        # Messages
        await session.execute(MessageModel.__table__.delete().where(MessageModel.owner_id == owner_id))
        for msg in profile_data.messages:
            session.add(MessageModel(owner_id=owner_id, text=msg))

        # Target chats
        await session.execute(TargetChatModel.__table__.delete().where(TargetChatModel.owner_id == owner_id))
        for phone, chat_set in profile_data.target_chats.items():
            for chat_id in chat_set:
                session.add(TargetChatModel(owner_id=owner_id, phone=phone, chat_id=chat_id))

        await session.commit()

async def load_profile(owner_id: int) -> Optional['ProfileData']:
    async with AsyncSessionLocal() as session:
        stmt = select(ProfileModel).where(ProfileModel.owner_id == owner_id)
        profile = (await session.execute(stmt)).scalar_one_or_none()
        if not profile:
            return None

        stmt = select(AccountModel).where(AccountModel.owner_id == owner_id)
        accounts = (await session.execute(stmt)).scalars().all()

        stmt = select(SourceChannelModel).where(SourceChannelModel.owner_id == owner_id)
        channels = (await session.execute(stmt)).scalars().all()

        stmt = select(MessageModel).where(MessageModel.owner_id == owner_id)
        messages = (await session.execute(stmt)).scalars().all()

        stmt = select(TargetChatModel).where(TargetChatModel.owner_id == owner_id)
        targets = (await session.execute(stmt)).scalars().all()

        # Build ProfileData
        pd = ProfileData()
        pd.timer_type = profile.timer_type
        pd.timer_value = profile.timer_value

        # Accounts
        for acc_model in accounts:
            cfg = AccountConfig(
                api_id=acc_model.api_id,
                api_hash=acc_model.api_hash,
                phone=acc_model.phone,
                password=acc_model.password,
                session_string=acc_model.session_string or ""
            )
            pd.accounts.append(cfg)

            # Create client from session string
            client = TelegramClient(StringSession(acc_model.session_string), cfg.api_id, cfg.api_hash)
            await client.connect()
            if not await client.is_user_authorized():
                print(f"Session invalid for {cfg.phone}, skipping.")
                continue
            pd.user_clients[cfg.phone] = client
            pd.client_to_phone[client] = cfg.phone
            client_owner[client] = owner_id
            setup_user_handlers(client, owner_id)

        # Source channels
        pd.source_channels = [ch.channel for ch in channels]
        # Messages
        pd.messages = [msg.text for msg in messages]
        # Target chats
        for t in targets:
            if t.phone not in pd.target_chats:
                pd.target_chats[t.phone] = set()
            pd.target_chats[t.phone].add(t.chat_id)

        # Re-join source channels to fill source_channel_ids
        for client in pd.user_clients.values():
            for chan_str in pd.source_channels:
                await join_source_channel(client, chan_str, owner_id)

        return pd

async def load_all_profiles():
    global SPECIAL_USERS
    # Load special users
    async with AsyncSessionLocal() as session:
        stmt = select(SpecialUserModel)
        specials = (await session.execute(stmt)).scalars().all()
        SPECIAL_USERS = {su.user_id for su in specials}

    # Load owner and all specials
    all_users = {OWNER_ID} | SPECIAL_USERS
    for uid in all_users:
        pd = await load_profile(uid)
        if pd is None:
            # Create new profile entry
            async with AsyncSessionLocal() as session:
                session.add(ProfileModel(owner_id=uid))
                await session.commit()
            pd = await load_profile(uid)
        if pd:
            profiles[uid] = pd

async def add_target_chat_db(owner_id: int, phone: str, chat_id: int):
    async with AsyncSessionLocal() as session:
        stmt = select(TargetChatModel).where(
            TargetChatModel.owner_id == owner_id,
            TargetChatModel.phone == phone,
            TargetChatModel.chat_id == chat_id
        )
        exists = (await session.execute(stmt)).scalar_one_or_none()
        if not exists:
            session.add(TargetChatModel(owner_id=owner_id, phone=phone, chat_id=chat_id))
            await session.commit()

async def remove_target_chat_db(owner_id: int, phone: str, chat_id: int):
    async with AsyncSessionLocal() as session:
        stmt = select(TargetChatModel).where(
            TargetChatModel.owner_id == owner_id,
            TargetChatModel.phone == phone,
            TargetChatModel.chat_id == chat_id
        )
        obj = (await session.execute(stmt)).scalar_one_or_none()
        if obj:
            await session.delete(obj)
            await session.commit()

async def add_special_user_db(user_id: int):
    async with AsyncSessionLocal() as session:
        session.add(SpecialUserModel(user_id=user_id))
        await session.commit()
    SPECIAL_USERS.add(user_id)

async def remove_special_user_db(user_id: int):
    async with AsyncSessionLocal() as session:
        stmt = select(SpecialUserModel).where(SpecialUserModel.user_id == user_id)
        obj = (await session.execute(stmt)).scalar_one_or_none()
        if obj:
            await session.delete(obj)
            await session.commit()
    SPECIAL_USERS.discard(user_id)

# ---------- Core Data Structures ----------
@dataclass
class AccountConfig:
    api_id: int
    api_hash: str
    phone: str
    password: Optional[str] = None
    session_string: str = ""

@dataclass
class ProfileData:
    accounts: List[AccountConfig] = field(default_factory=list)
    user_clients: Dict[str, TelegramClient] = field(default_factory=dict)
    client_to_phone: Dict[TelegramClient, str] = field(default_factory=dict)
    source_channels: List[str] = field(default_factory=list)
    source_channel_ids: Set[int] = field(default_factory=set)
    target_chats: Dict[str, Set[int]] = field(default_factory=dict)
    messages: List[str] = field(default_factory=list)
    timer_type: str = "fixed"
    timer_value: int = 5
    sending_active: bool = False
    send_tasks: List[asyncio.Task] = field(default_factory=list)

profiles: Dict[int, ProfileData] = {}
SPECIAL_USERS: Set[int] = set()
client_owner: Dict[TelegramClient, int] = {}
user_states: Dict[int, str] = {}
pending_account: Dict[int, Dict] = {}

STATE_NONE = ""
STATE_ACC_API_ID = "ACC_API_ID"
STATE_ACC_API_HASH = "ACC_API_HASH"
STATE_ACC_PHONE = "ACC_PHONE"
STATE_ACC_CODE = "ACC_CODE"
STATE_ACC_2FA = "ACC_2FA"
STATE_WAIT_ACCOUNT_REMOVE = "WAIT_ACCOUNT_REMOVE"
STATE_WAIT_CHANNEL_ADD = "WAIT_CHANNEL_ADD"
STATE_WAIT_CHANNEL_REMOVE = "WAIT_CHANNEL_REMOVE"
STATE_WAIT_MESSAGE_ADD = "WAIT_MESSAGE_ADD"
STATE_WAIT_MESSAGE_REMOVE = "WAIT_MESSAGE_REMOVE"
STATE_WAIT_TIMER_VALUE = "WAIT_TIMER_VALUE"
STATE_WAIT_SPECIAL_ADD = "WAIT_SPECIAL_ADD"
STATE_WAIT_SPECIAL_REMOVE = "WAIT_SPECIAL_REMOVE"
STATE_WAIT_GROUP_ADD = "WAIT_GROUP_ADD"
STATE_WAIT_BIO = "WAIT_BIO"  # NEW

TELEGRAM_LINK_REGEX = re.compile(r"(https?://t\.me/[^\s]+)")

def log(prefix: str, msg: str):
    print(f"[{prefix}] {msg}")


def extract_session_string(client: TelegramClient) -> str:
    """Return a Telegram string session for persistence.

    Telethon's SQLite session object does not always expose a string via
    `session.save()`, so we try the recommended StringSession conversion first
    and fall back to other available methods.
    """
    candidates = []
    try:
        candidates.append(StringSession.save(client.session))
    except Exception:
        pass
    try:
        candidates.append(client.session.save())
    except Exception:
        pass
    try:
        candidates.append(getattr(client.session, 'string', None))
    except Exception:
        pass

    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""

def get_profile(owner_id: int) -> ProfileData:
    if owner_id not in profiles:
        profiles[owner_id] = ProfileData()
    return profiles[owner_id]

def set_state(user_id: int, state: str):
    if state:
        user_states[user_id] = state
    else:
        user_states.pop(user_id, None)

def get_state(user_id: int) -> str:
    return user_states.get(user_id, STATE_NONE)

def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

def is_allowed_user(user_id: int) -> bool:
    return user_id == OWNER_ID or user_id in SPECIAL_USERS

def check_admin(event) -> bool:
    return is_allowed_user(event.sender_id)

# ---------- Core Functions ----------
def register_target_chat(client: TelegramClient, chat_id: int):
    owner_id = client_owner.get(client)
    if owner_id is None:
        return
    profile = get_profile(owner_id)
    phone = profile.client_to_phone.get(client)
    if not phone:
        return
    if phone not in profile.target_chats:
        profile.target_chats[phone] = set()
    profile.target_chats[phone].add(chat_id)
    asyncio.create_task(add_target_chat_db(owner_id, phone, chat_id))
    log(f"{owner_id}/{phone}", f"Registered target chat: {chat_id}")

async def join_by_link(client: TelegramClient, link: str):
    owner_id = client_owner.get(client)
    if owner_id is None:
        return
    me = await client.get_me()
    tag = f"{owner_id}/{me.username or me.id}"
    link = link.strip()
    log(tag, f"Trying to join by link: {link}")

    if "joinchat/" in link or "t.me/+" in link:
        if "joinchat/" in link:
            code = link.split("joinchat/")[1]
        else:
            code = link.split("t.me/+")[1]
        code = code.split("?")[0]
        try:
            res = await client(ImportChatInviteRequest(code))
            chat_id = None
            if hasattr(res, "chats") and res.chats:
                chat_id = res.chats[0].id
            if chat_id is not None:
                register_target_chat(client, chat_id)
                log(tag, f"Joined private chat (target): {chat_id}")
        except UserAlreadyParticipantError:
            log(tag, "Already participant (private).")
            # Try to get entity via link and register it
            try:
                entity = await client.get_entity(link)
                register_target_chat(client, entity.id)
                log(tag, f"Already participant, registered private chat: {entity.id}")
            except Exception as e2:
                log(tag, f"Failed to get entity for already-participant private: {e2}")
        except Exception as e:
            log(tag, f"Failed to join by private invite: {e}")
        return

    try:
        entity = await client.get_entity(link)
        await client(JoinChannelRequest(entity))
        register_target_chat(client, entity.id)
        log(tag, f"Joined public chat (target): {entity.id}")
    except UserAlreadyParticipantError:
        log(tag, "Already participant (public).")
        try:
            entity = await client.get_entity(link)
            register_target_chat(client, entity.id)
        except Exception as e2:
            log(tag, f"Failed to get entity for already-participant: {e2}")
    except Exception as e:
        log(tag, f"Failed to join public link: {e}")

async def join_source_channel(client: TelegramClient, chan_str: str, owner_id: int):
    profile = get_profile(owner_id)
    me = await client.get_me()
    tag = f"{owner_id}/{me.username or me.id}"
    chan_str = chan_str.strip()

    try:
        if chan_str.startswith("https://t.me/"):
            body = chan_str.split("https://t.me/")[1]
            if body.startswith("joinchat/") or body.startswith("+"):
                code = body.split("joinchat/")[-1] if "joinchat/" in body else body[1:]
                code = code.split("?")[0]
                res = await client(ImportChatInviteRequest(code))
                entity = res.chats[0] if res.chats else None
            else:
                username = body.split("/")[0]
                entity = await client.get_entity(username)
                await client(JoinChannelRequest(entity))
        else:
            username = chan_str.lstrip("@")
            entity = await client.get_entity(username)
            await client(JoinChannelRequest(entity))

        if entity is None:
            log(tag, f"Could not get entity for source channel: {chan_str}")
            return None

        profile.source_channel_ids.add(entity.id)
        log(tag, f"Joined source channel: {chan_str} (id={entity.id})")
        return entity

    except UserAlreadyParticipantError:
        try:
            if chan_str.startswith("https://t.me/"):
                body = chan_str.split("https://t.me/")[1]
                if body.startswith("joinchat/") or body.startswith("+"):
                    log(tag, f"Already in source (private link): {chan_str}")
                    return None
                username = body.split("/")[0]
                entity = await client.get_entity(username)
            else:
                username = chan_str.lstrip("@")
                entity = await client.get_entity(username)
            profile.source_channel_ids.add(entity.id)
            log(tag, f"Already in source channel: {chan_str} (id={entity.id})")
            return entity
        except Exception as e2:
            log(tag, f"Failed after already-participant for source: {e2}")
            return None
    except Exception as e:
        log(tag, f"Failed to join source channel {chan_str}: {e}")
        return None

async def check_last_messages_for_all_channels(client: TelegramClient, owner_id: int):
    profile = get_profile(owner_id)
    me = await client.get_me()
    tag = f"{owner_id}/{me.username or me.id}"

    for cid in list(profile.source_channel_ids):
        try:
            entity = await client.get_entity(cid)
            async for msg in client.iter_messages(entity, limit=1):
                if not msg.message:
                    continue
                links = TELEGRAM_LINK_REGEX.findall(msg.message)
                if not links:
                    continue
                log(tag, f"Last message in source {cid} has links: {links}")
                for link in links:
                    await join_by_link(client, link)
        except Exception as e:
            log(tag, f"Error reading last message of {cid}: {e}")

def setup_user_handlers(client: TelegramClient, owner_id: int):
    profile = get_profile(owner_id)

    @client.on(events.NewMessage)
    async def handler(event: events.NewMessage.Event):
        if event.chat_id not in profile.source_channel_ids:
            return
        me = await client.get_me()
        tag = f"{owner_id}/{me.username or me.id}"
        text = event.message.message or ""
        links = TELEGRAM_LINK_REGEX.findall(text)
        if not links:
            return
        log(tag, f"New message in source {event.chat_id} has links: {links}")
        for link in links:
            await join_by_link(client, link)

async def finish_login_for_account(uid: int, password_used: Optional[str]):
    data = pending_account.get(uid)
    if not data:
        return
    profile = get_profile(uid)
    api_id = data["api_id"]
    api_hash = data["api_hash"]
    phone = data["phone"]
    client: TelegramClient = data["client"]

    session_string = extract_session_string(client)
    if not session_string:
        # Fall back to an empty string instead of NULL so the DB constraint
        # cannot be violated; this keeps the bot alive and lets the account
        # stay usable in the current runtime.
        print(f"WARNING: could not extract session string for {phone}; storing empty string.")

    cfg = AccountConfig(
        api_id=api_id,
        api_hash=api_hash,
        phone=phone,
        password=password_used,
        session_string=session_string,
    )
    profile.accounts.append(cfg)
    profile.user_clients[phone] = client
    profile.client_to_phone[client] = phone
    client_owner[client] = uid

    setup_user_handlers(client, uid)

    for chan_str in profile.source_channels:
        await join_source_channel(client, chan_str, uid)

    await check_last_messages_for_all_channels(client, uid)

    pending_account.pop(uid, None)
    set_state(uid, STATE_NONE)
    await save_profile(uid, profile)

async def add_source_channel_from_text(owner_id: int, text: str):
    profile = get_profile(owner_id)
    chan_str = text.strip()
    profile.source_channels.append(chan_str)

    for client in profile.user_clients.values():
        await join_source_channel(client, chan_str, owner_id)

    for client in profile.user_clients.values():
        await check_last_messages_for_all_channels(client, owner_id)
    await save_profile(owner_id, profile)

async def remove_source_channel_by_index(owner_id: int, idx: int):
    profile = get_profile(owner_id)
    if idx < 1 or idx > len(profile.source_channels):
        raise IndexError("ایندکس نامعتبر است.")
    removed = profile.source_channels.pop(idx - 1)
    log(f"SYSTEM/{owner_id}", f"Source channel removed: {removed}")
    profile.source_channel_ids.clear()
    for client in profile.user_clients.values():
        for chan_str in profile.source_channels:
            await join_source_channel(client, chan_str, owner_id)
    await save_profile(owner_id, profile)

async def remove_account_by_index(owner_id: int, idx: int):
    profile = get_profile(owner_id)
    if idx < 1 or idx > len(profile.accounts):
        raise IndexError("ایندکس نامعتبر است.")
    cfg = profile.accounts.pop(idx - 1)
    client = profile.user_clients.pop(cfg.phone, None)
    if client is not None:
        profile.client_to_phone.pop(client, None)
        client_owner.pop(client, None)
        await client.disconnect()
        if cfg.phone in profile.target_chats:
            del profile.target_chats[cfg.phone]
        log(f"SYSTEM/{owner_id}", f"Account {cfg.phone} disconnected & removed.")
    await save_profile(owner_id, profile)

async def add_manual_group(owner_id: int, link: str):
    profile = get_profile(owner_id)
    if not profile.user_clients:
        raise Exception("هیچ اکانتی لاگین نشده است.")
    success_count = 0
    for client in profile.user_clients.values():
        try:
            await join_by_link(client, link)
            success_count += 1
        except Exception as e:
            log(f"{owner_id}", f"Error joining group for one client: {e}")
    await save_profile(owner_id, profile)
    return success_count

async def send_loop_for_client(client: TelegramClient, phone: str, owner_id: int):
    profile = get_profile(owner_id)
    me = await client.get_me()
    tag = f"{owner_id}/{me.username or me.id}"

    chats = list(profile.target_chats.get(phone, []))
    if not profile.messages or not chats:
        log(tag, "No messages or target chats for this client.")
        return

    for chat_id in chats:
        try:
            text = random.choice(profile.messages)
            await client.send_message(chat_id, text)
            log(tag, f"Sent initial message to {chat_id}")
        except Exception as e:
            log(tag, f"Failed to send initial message to {chat_id}: {e}")

    while profile.sending_active:
        if profile.timer_type == "fixed":
            delay_min = profile.timer_value
        else:
            delay_min = random.randint(15, 500)

        delay_sec = delay_min * 60
        log(tag, f"Sleeping for {delay_min} minutes before next send...")
        try:
            await asyncio.sleep(delay_sec)
        except asyncio.CancelledError:
            log(tag, "Send loop cancelled.")
            break

        if not profile.sending_active:
            break

        chats = list(profile.target_chats.get(phone, []))
        if not profile.messages or not chats:
            log(tag, "No messages or target chats (loop).")
            continue

        for chat_id in chats:
            try:
                text = random.choice(profile.messages)
                await client.send_message(chat_id, text)
                log(tag, f"Sent scheduled message to {chat_id}")
            except Exception as e:
                log(tag, f"Failed to send scheduled message to {chat_id}: {e}")

async def start_sending_process(event):
    owner_id = event.sender_id
    profile = get_profile(owner_id)

    if profile.sending_active:
        await event.edit("فرآیند ارسال از قبل فعال است.", buttons=sending_menu_buttons(is_owner(owner_id)))
        return

    if not profile.user_clients:
        await event.edit("هیچ اکانتی اضافه نشده.", buttons=sending_menu_buttons(is_owner(owner_id)))
        return
    if not profile.messages:
        await event.edit("هیچ پیامی در لیست نیست.", buttons=sending_menu_buttons(is_owner(owner_id)))
        return

    has_any_target = any(profile.target_chats.get(phone) for phone in profile.user_clients.keys())
    if not has_any_target:
        await event.edit("هیچ چت هدفی (از طریق لینک‌ها) ثبت نشده.", buttons=sending_menu_buttons(is_owner(owner_id)))
        return

    profile.sending_active = True

    for t in profile.send_tasks:
        t.cancel()
    profile.send_tasks = []

    loop = asyncio.get_running_loop()
    for phone, client in profile.user_clients.items():
        if not profile.target_chats.get(phone):
            continue
        task = loop.create_task(send_loop_for_client(client, phone, owner_id))
        profile.send_tasks.append(task)

    await event.edit(
        "✅ فرآیند ارسال پیام‌ها شروع شد.\n"
        "الان هر یوزر یک پیام رندوم فرستاد و ادامه طبق تایمر خواهد بود.",
        buttons=sending_menu_buttons(is_owner(owner_id))
    )

async def stop_sending_process(event):
    owner_id = event.sender_id
    profile = get_profile(owner_id)

    if not profile.sending_active:
        await event.edit("فرآیند ارسال از قبل متوقف است.", buttons=sending_menu_buttons(is_owner(owner_id)))
        return

    profile.sending_active = False

    for t in profile.send_tasks:
        t.cancel()
    profile.send_tasks = []

    await event.edit("⏹ همه‌ی فرآیندهای ارسال پیام متوقف شدند.",
                     buttons=sending_menu_buttons(is_owner(owner_id)))

# ---------- NEW: Set Bio for all accounts ----------
async def set_bio_for_all_accounts(owner_id: int, bio_text: str) -> str:
    """Set the given bio text for all active accounts of this owner.
       Returns a summary message."""
    profile = get_profile(owner_id)
    if not profile.user_clients:
        return "❌ هیچ اکانت فعالی برای تغییر بیوگرافی وجود ندارد."

    success = []
    failed = []

    for phone, client in profile.user_clients.items():
        try:
            # Ensure client is authorized
            if not await client.is_user_authorized():
                failed.append(f"{phone} (not authorized)")
                continue

            # Update profile bio using account.UpdateProfileRequest
            await client(UpdateProfileRequest(about=bio_text))
            success.append(phone)
            log(f"{owner_id}/{phone}", f"Bio updated to: {bio_text[:30]}...")
        except Exception as e:
            failed.append(f"{phone} ({str(e)})")

    summary = f"✅ بیوگرافی برای {len(success)} اکانت تنظیم شد.\n"
    if failed:
        summary += f"❌ {len(failed)} اکانت با خطا مواجه شد:\n" + "\n".join(failed)
    else:
        summary += "همه اکانت‌ها با موفقیت به‌روزرسانی شدند."
    return summary

# ---------- Bot Menus ----------
bot_client = TelegramClient("bot_session", API_ID, API_HASH)

def main_menu_buttons(owner: bool):
    rows = [
        [Button.inline("👤 مدیریت اکانت‌ها", b"menu_accounts")],
        [Button.inline("📡 مدیریت کانال‌ها", b"menu_channels")],
        [Button.inline("💬 مدیریت پیام‌ها", b"menu_messages")],
        [Button.inline("⏱ تنظیم تایمر", b"menu_timer")],
        [Button.inline("🚀 کنترل ارسال پیام‌ها", b"menu_sending")],
        [Button.inline("➕ افزودن گروه دستی", b"group_add")],
    ]
    if owner:
        rows.append([Button.inline("👑 مدیریت کاربران ویژه", b"menu_special")])
    return rows

def accounts_menu_buttons():
    return [
        [Button.inline("➕ افزودن اکانت جدید", b"acc_add")],
        [Button.inline("📜 لیست اکانت‌ها", b"acc_list")],
        [Button.inline("🗑 حذف اکانت", b"acc_remove")],
        [Button.inline("📝 تنظیم بیوگرافی", b"acc_set_bio")],  # NEW
        [Button.inline("⬅️ بازگشت", b"back_main")],
    ]

def channels_menu_buttons():
    return [
        [Button.inline("➕ افزودن کانال منبع", b"chan_add")],
        [Button.inline("📜 لیست کانال‌ها", b"chan_list")],
        [Button.inline("🗑 حذف کانال", b"chan_remove")],
        [Button.inline("⬅️ بازگشت", b"back_main")],
    ]

def messages_menu_buttons():
    return [
        [Button.inline("➕ افزودن پیام", b"msg_add")],
        [Button.inline("📜 لیست پیام‌ها", b"msg_list")],
        [Button.inline("🗑 حذف پیام", b"msg_remove")],
        [Button.inline("⬅️ بازگشت", b"back_main")],
    ]

def timer_menu_buttons():
    return [
        [Button.inline("⏱ تنظیم فاصله (دقیقه)", b"timer_set_value")],
        [
            Button.inline("⚙️ fixed", b"timer_fixed"),
            Button.inline("🎲 random", b"timer_random"),
        ],
        [Button.inline("⬅️ بازگشت", b"back_main")],
    ]

def sending_menu_buttons(owner: bool):
    return [
        [Button.inline("▶️ شروع ارسال", b"send_start")],
        [Button.inline("⏹ توقف ارسال", b"send_stop")],
        [Button.inline("⬅️ بازگشت", b"back_main")],
    ]

def special_menu_buttons():
    return [
        [Button.inline("➕ افزودن کاربر ویژه", b"special_add")],
        [Button.inline("🗑 حذف کاربر ویژه", b"special_remove")],
        [Button.inline("⬅️ بازگشت", b"back_main")],
    ]

# ---------- Bot Handlers ----------
@bot_client.on(events.NewMessage(pattern="/start"))
async def bot_start(event: events.NewMessage.Event):
    uid = event.sender_id
    if not check_admin(event):
        return
    # Ensure profile is loaded
    if uid not in profiles:
        pd = await load_profile(uid)
        if pd is None:
            # Create new profile entry
            async with AsyncSessionLocal() as session:
                session.add(ProfileModel(owner_id=uid))
                await session.commit()
            pd = await load_profile(uid)
        if pd:
            profiles[uid] = pd
        else:
            # Fallback
            profiles[uid] = ProfileData()
    get_profile(uid)
    set_state(uid, STATE_NONE)
    text = "سلام 👋\nبه پنل مدیریت خوش اومدی.\n\nیکی از گزینه‌های زیر رو انتخاب کن:"
    await event.respond(text, buttons=main_menu_buttons(is_owner(uid)))

@bot_client.on(events.CallbackQuery)
async def bot_callback(event: events.CallbackQuery.Event):
    uid = event.sender_id
    if not check_admin(event):
        await event.answer("اجازه دسترسی نداری.", alert=True)
        return

    owner_flag = is_owner(uid)
    profile = get_profile(uid)

    data = event.data.decode("utf-8")
    set_state(uid, STATE_NONE)

    if data == "back_main":
        await event.edit("منوی اصلی 👇", buttons=main_menu_buttons(owner_flag))
        return

    if data == "menu_accounts":
        await event.edit("👤 مدیریت اکانت‌ها:", buttons=accounts_menu_buttons())
        return

    if data == "acc_list":
        if not profile.accounts:
            txt = "هنوز هیچ اکانتی اضافه نشده."
        else:
            lines = ["📜 لیست اکانت‌ها:"]
            for i, cfg in enumerate(profile.accounts, start=1):
                lines.append(f"{i}) {cfg.phone} (api_id={cfg.api_id})")
            txt = "\n".join(lines)
        await event.edit(txt, buttons=accounts_menu_buttons())
        return

    if data == "acc_add":
        pending_account[uid] = {}
        set_state(uid, STATE_ACC_API_ID)
        txt = "➕ افزودن اکانت جدید:\n\nاول `api_id` رو بفرست."
        await event.edit(txt, buttons=accounts_menu_buttons(), parse_mode="Markdown")
        return

    if data == "acc_remove":
        if not profile.accounts:
            await event.edit("هیچ اکانتی برای حذف وجود ندارد.", buttons=accounts_menu_buttons())
            return
        set_state(uid, STATE_WAIT_ACCOUNT_REMOVE)
        lines = ["🗑 حذف اکانت:\nشماره اکانتی که می‌خوای حذف بشه رو بفرست.\n"]
        for i, cfg in enumerate(profile.accounts, start=1):
            lines.append(f"{i}) {cfg.phone}")
        await event.edit("\n".join(lines), buttons=accounts_menu_buttons())
        return

    # NEW: Set bio
    if data == "acc_set_bio":
        if not profile.user_clients:
            await event.edit("❌ هیچ اکانت فعالی برای تنظیم بیوگرافی وجود ندارد.", buttons=accounts_menu_buttons())
            return
        set_state(uid, STATE_WAIT_BIO)
        await event.edit(
            "📝 لطفاً بیوگرافی جدیدی که می‌خواهید برای **همه** اکانت‌ها تنظیم شود را وارد کنید.\n"
            "حداکثر ۷۰ کاراکتر (Telegram limit).",
            buttons=accounts_menu_buttons(),
            parse_mode="Markdown"
        )
        return

    if data == "menu_channels":
        await event.edit("📡 مدیریت کانال‌های منبع:", buttons=channels_menu_buttons())
        return

    if data == "chan_list":
        if not profile.source_channels:
            txt = "هنوز هیچ کانالی ثبت نشده."
        else:
            lines = ["📜 لیست کانال‌ها:"]
            for i, ch in enumerate(profile.source_channels, start=1):
                lines.append(f"{i}) {ch}")
            txt = "\n".join(lines)
        await event.edit(txt, buttons=channels_menu_buttons())
        return

    if data == "chan_add":
        set_state(uid, STATE_WAIT_CHANNEL_ADD)
        txt = (
            "➕ افزودن کانال منبع:\n\n"
            "یکی از موارد زیر را بفرست:\n"
            "- username (مثلاً: `my_channel`)\n"
            "- یا @username (مثلاً: `@my_channel`)\n"
            "- یا لینک کامل `https://t.me/...` (پابلیک یا پرایوت)"
        )
        await event.edit(txt, buttons=channels_menu_buttons(), parse_mode="Markdown")
        return

    if data == "chan_remove":
        if not profile.source_channels:
            await event.edit("هیچ کانالی برای حذف وجود ندارد.", buttons=channels_menu_buttons())
            return
        set_state(uid, STATE_WAIT_CHANNEL_REMOVE)
        lines = ["🗑 حذف کانال:\nشماره کانالی که می‌خوای حذف بشه رو بفرست.\n"]
        for i, ch in enumerate(profile.source_channels, start=1):
            lines.append(f"{i}) {ch}")
        await event.edit("\n".join(lines), buttons=channels_menu_buttons())
        return

    if data == "menu_messages":
        await event.edit("💬 مدیریت پیام‌ها:", buttons=messages_menu_buttons())
        return

    if data == "msg_list":
        if not profile.messages:
            txt = "هنوز هیچ پیامی ذخیره نشده."
        else:
            lines = ["📜 لیست پیام‌ها:"]
            for i, msg in enumerate(profile.messages, start=1):
                lines.append(f"{i}) {msg}")
            txt = "\n".join(lines)
        await event.edit(txt, buttons=messages_menu_buttons())
        return

    if data == "msg_add":
        set_state(uid, STATE_WAIT_MESSAGE_ADD)
        await event.edit("➕ متن پیام جدید رو بفرست.", buttons=messages_menu_buttons())
        return

    if data == "msg_remove":
        if not profile.messages:
            await event.edit("هیچ پیامی برای حذف وجود ندارد.", buttons=messages_menu_buttons())
            return
        set_state(uid, STATE_WAIT_MESSAGE_REMOVE)
        lines = ["🗑 حذف پیام:\nشماره پیامی که می‌خوای حذف بشه رو بفرست.\n"]
        for i, msg in enumerate(profile.messages, start=1):
            lines.append(f"{i}) {msg}")
        await event.edit("\n".join(lines), buttons=messages_menu_buttons())
        return

    if data == "menu_timer":
        txt = (
            f"⏱ تنظیم تایمر:\n"
            f"- نوع فعلی: {profile.timer_type}\n"
            f"- فاصله‌ی fixed فعلی: {profile.timer_value} دقیقه\n"
            f"- random: بین 15 تا 500 دقیقه برای هر یوزر."
        )
        await event.edit(txt, buttons=timer_menu_buttons())
        return

    if data == "timer_set_value":
        set_state(uid, STATE_WAIT_TIMER_VALUE)
        await event.edit(
            "⏱ مقدار فاصله‌ی ثابت (دقیقه) رو بفرست. مثال: `5`",
            buttons=timer_menu_buttons(),
            parse_mode="Markdown"
        )
        return

    if data == "timer_fixed":
        profile.timer_type = "fixed"
        await save_profile(uid, profile)
        await event.edit(
            f"نوع تایمر روی fixed تنظیم شد.\nفاصله فعلی: {profile.timer_value} دقیقه",
            buttons=timer_menu_buttons()
        )
        return

    if data == "timer_random":
        profile.timer_type = "random"
        await save_profile(uid, profile)
        await event.edit(
            "نوع تایمر روی random تنظیم شد.\n"
            "فاصله‌ی هر یوزر به صورت رندوم بین 15 و 500 دقیقه انتخاب می‌شود.",
            buttons=timer_menu_buttons()
        )
        return

    if data == "menu_sending":
        await event.edit("🚀 کنترل ارسال پیام‌ها:", buttons=sending_menu_buttons(owner_flag))
        return

    if data == "send_start":
        await start_sending_process(event)
        return

    if data == "send_stop":
        await stop_sending_process(event)
        return

    if data == "group_add":
        set_state(uid, STATE_WAIT_GROUP_ADD)
        await event.edit(
            "➕ لینک گروهی که می‌خواهید به لیست هدف اضافه شود را بفرستید:\n"
            "مثال: https://t.me/joinchat/ABCDEF یا https://t.me/mygroup",
            buttons=main_menu_buttons(owner_flag),
            parse_mode="Markdown"
        )
        return

    if data == "menu_special":
        if not owner_flag:
            await event.answer("فقط مالک ربات می‌تواند کاربران ویژه را مدیریت کند.", alert=True)
            return
        if not SPECIAL_USERS:
            txt = "هنوز هیچ کاربر ویژه‌ای ثبت نشده."
        else:
            lines = ["👑 لیست کاربران ویژه (user_id):"]
            for uid2 in SPECIAL_USERS:
                lines.append(f"- {uid2}")
            txt = "\n".join(lines)
        await event.edit(txt, buttons=special_menu_buttons())
        return

    if data == "special_add":
        if not owner_flag:
            await event.answer("فقط مالک ربات می‌تواند کاربر ویژه اضافه کند.", alert=True)
            return
        set_state(uid, STATE_WAIT_SPECIAL_ADD)
        await event.edit(
            "👑 user_id کاربری که می‌خواهی ویژه شود را بفرست.\n"
            "مثال: `123456789`",
            buttons=special_menu_buttons(),
            parse_mode="Markdown"
        )
        return

    if data == "special_remove":
        if not owner_flag:
            await event.answer("فقط مالک ربات می‌تواند کاربر ویژه حذف کند.", alert=True)
            return
        if not SPECIAL_USERS:
            await event.edit("هیچ کاربر ویژه‌ای برای حذف وجود ندارد.", buttons=special_menu_buttons())
            return
        set_state(uid, STATE_WAIT_SPECIAL_REMOVE)
        lines = ["🗑 حذف کاربر ویژه:\nuser_id یکی از کاربران زیر را بفرست:"]
        for uid2 in SPECIAL_USERS:
            lines.append(f"- {uid2}")
        await event.edit("\n".join(lines), buttons=special_menu_buttons())
        return

@bot_client.on(events.NewMessage)
async def bot_text_handler(event: events.NewMessage.Event):
    if not event.is_private:
        return
    if not check_admin(event):
        return

    uid = event.sender_id
    text = (event.raw_text or "").strip()
    state = get_state(uid)

    if text.startswith("/"):
        return

    profile = get_profile(uid)

    if state == STATE_ACC_API_ID:
        try:
            api_id = int(text)
            pending_account.setdefault(uid, {})["api_id"] = api_id
            set_state(uid, STATE_ACC_API_HASH)
            await event.respond("حالا `api_hash` رو بفرست.", parse_mode="Markdown")
        except ValueError:
            await event.respond("api_id باید عددی باشه. دوباره بفرست.")
        return

    if state == STATE_ACC_API_HASH:
        pending_account.setdefault(uid, {})["api_hash"] = text
        set_state(uid, STATE_ACC_PHONE)
        await event.respond("شماره تلفن اکانت (مثلاً +98912...) رو بفرست.")
        return

    if state == STATE_ACC_PHONE:
        data = pending_account.setdefault(uid, {})
        data["phone"] = text

        api_id = data["api_id"]
        api_hash = data["api_hash"]
        phone = data["phone"]

        session_name = f"session_{uid}_{phone.replace('+', '')}"
        client = TelegramClient(session_name, api_id, api_hash)
        await client.connect()

        data["client"] = client

        try:
            await client.send_code_request(phone)
            set_state(uid, STATE_ACC_CODE)
            await event.respond("کدی که برات اومده رو بفرست.")
        except Exception as e:
            await event.respond(f"خطا در ارسال کد:\n{e}")
            pending_account.pop(uid, None)
            set_state(uid, STATE_NONE)
        return

    if state == STATE_ACC_CODE:
        data = pending_account.get(uid)
        if not data:
            await event.respond("مشکلی پیش اومد، از منوی افزودن اکانت دوباره شروع کن.")
            set_state(uid, STATE_NONE)
            return

        client: TelegramClient = data["client"]
        phone = data["phone"]
        code = text

        try:
            await client.sign_in(phone=phone, code=code)
            await finish_login_for_account(uid, password_used=None)
            await event.respond("✅ اکانت بدون 2FA با موفقیت لاگین شد و اضافه شد.")
            await event.respond("👤 برگردیم به منوی اکانت‌ها:", buttons=accounts_menu_buttons())
        except SessionPasswordNeededError:
            set_state(uid, STATE_ACC_2FA)
            await event.respond("این اکانت 2FA دارد. رمز 2FA را بفرست.")
        except Exception as e:
            await event.respond(f"کد اشتباه یا خطا:\n{e}\nدوباره کد را بفرست.")
        return

    if state == STATE_ACC_2FA:
        data = pending_account.get(uid)
        if not data:
            await event.respond("مشکلی پیش اومد، از اول افزودن اکانت رو شروع کن.")
            set_state(uid, STATE_NONE)
            return

        client: TelegramClient = data["client"]
        password = text

        try:
            await client.sign_in(password=password)
            await finish_login_for_account(uid, password_used=password)
            await event.respond("✅ اکانت با 2FA با موفقیت لاگین شد و اضافه شد.")
            await event.respond("👤 برگردیم به منوی اکانت‌ها:", buttons=accounts_menu_buttons())
        except Exception as e:
            await event.respond(f"رمز 2FA اشتباه یا خطا:\n{e}\nدوباره بفرست.")
        return

    if state == STATE_WAIT_ACCOUNT_REMOVE:
        try:
            idx = int(text)
            await remove_account_by_index(uid, idx)
            set_state(uid, STATE_NONE)
            await event.respond("✅ اکانت حذف شد.")
            await event.respond("👤 برگردیم به منوی اکانت‌ها:", buttons=accounts_menu_buttons())
        except Exception as e:
            await event.respond(f"❌ خطا در حذف اکانت:\n{e}")
        return

    if state == STATE_WAIT_CHANNEL_ADD:
        try:
            await add_source_channel_from_text(uid, text)
            set_state(uid, STATE_NONE)
            await event.respond("✅ کانال منبع اضافه شد. همه‌ی یوزرها join شدند و آخرین پیام برای لینک چک شد.")
            await event.respond("📡 برگردیم به منوی کانال‌ها:", buttons=channels_menu_buttons())
        except Exception as e:
            await event.respond(f"❌ خطا در افزودن کانال:\n{e}")
        return

    if state == STATE_WAIT_CHANNEL_REMOVE:
        try:
            idx = int(text)
            await remove_source_channel_by_index(uid, idx)
            set_state(uid, STATE_NONE)
            await event.respond("✅ کانال منبع حذف شد.")
            await event.respond("📡 برگردیم به منوی کانال‌ها:", buttons=channels_menu_buttons())
        except Exception as e:
            await event.respond(f"❌ خطا در حذف کانال:\n{e}")
        return

    if state == STATE_WAIT_MESSAGE_ADD:
        profile.messages.append(text)
        set_state(uid, STATE_NONE)
        await save_profile(uid, profile)
        await event.respond("✅ پیام به لیست اضافه شد.")
        await event.respond("💬 برگردیم به منوی پیام‌ها:", buttons=messages_menu_buttons())
        return

    if state == STATE_WAIT_MESSAGE_REMOVE:
        try:
            idx = int(text)
            if idx < 1 or idx > len(profile.messages):
                raise IndexError("ایندکس نامعتبر است.")
            removed = profile.messages.pop(idx - 1)
            set_state(uid, STATE_NONE)
            await save_profile(uid, profile)
            await event.respond(f"✅ پیام حذف شد:\n{removed}")
            await event.respond("💬 برگردیم به منوی پیام‌ها:", buttons=messages_menu_buttons())
        except Exception as e:
            await event.respond(f"❌ خطا در حذف پیام:\n{e}")
        return

    if state == STATE_WAIT_TIMER_VALUE:
        try:
            val = int(text)
            if val <= 0:
                raise ValueError
            profile.timer_value = val
            set_state(uid, STATE_NONE)
            await save_profile(uid, profile)
            await event.respond(f"⏱ فاصله‌ی fixed روی {profile.timer_value} دقیقه تنظیم شد.")
            await event.respond("منوی تایمر:", buttons=timer_menu_buttons())
        except Exception:
            await event.respond("عدد معتبر (دقیقه مثبت) وارد کن.")
        return

    if state == STATE_WAIT_GROUP_ADD:
        try:
            count = await add_manual_group(uid, text)
            set_state(uid, STATE_NONE)
            await event.respond(
                f"✅ گروه با موفقیت به لیست هدف برای {count} اکانت اضافه شد.\n"
                "همه اکانت‌ها به گروه ملحق شده و در لیست هدف قرار گرفتند.",
                buttons=main_menu_buttons(is_owner(uid))
            )
        except Exception as e:
            await event.respond(f"❌ خطا در افزودن گروه:\n{e}", buttons=main_menu_buttons(is_owner(uid)))
        return

    # NEW: Set bio handler
    if state == STATE_WAIT_BIO:
        if len(text) > 70:
            await event.respond("⚠️ بیوگرافی نباید بیشتر از ۷۰ کاراکتر باشد. لطفاً کوتاه‌تر وارد کنید.")
            return
        set_state(uid, STATE_NONE)
        summary = await set_bio_for_all_accounts(uid, text)
        await event.respond(summary, buttons=accounts_menu_buttons())
        return

    if state == STATE_WAIT_SPECIAL_ADD and is_owner(uid):
        try:
            special_id = int(text)
            await add_special_user_db(special_id)
            # Load profile for the new special user (if not exists)
            if special_id not in profiles:
                pd = await load_profile(special_id)
                if pd is None:
                    async with AsyncSessionLocal() as session:
                        session.add(ProfileModel(owner_id=special_id))
                        await session.commit()
                    pd = await load_profile(special_id)
                if pd:
                    profiles[special_id] = pd
            set_state(uid, STATE_NONE)
            await event.respond(
                f"✅ کاربر ویژه اضافه شد: {special_id}\n"
                "وقتی این کاربر /start را بزند، پنل جداگانه خودش را خواهد داشت.",
                buttons=special_menu_buttons()
            )
        except Exception as e:
            await event.respond(f"❌ خطا در افزودن کاربر ویژه:\n{e}")
        return

    if state == STATE_WAIT_SPECIAL_REMOVE and is_owner(uid):
        try:
            special_id = int(text)
            if special_id in SPECIAL_USERS:
                await remove_special_user_db(special_id)
                # Remove from profiles and disconnect clients
                prof = profiles.pop(special_id, None)
                if prof:
                    for c in prof.user_clients.values():
                        try:
                            await c.disconnect()
                        except Exception:
                            pass
                set_state(uid, STATE_NONE)
                await event.respond(
                    f"✅ کاربر ویژه حذف شد: {special_id}",
                    buttons=special_menu_buttons()
                )
            else:
                await event.respond("این user_id در لیست کاربران ویژه نیست.", buttons=special_menu_buttons())
        except Exception as e:
            await event.respond(f"❌ خطا در حذف کاربر ویژه:\n{e}")
        return

# ---------- Main ----------
async def run_bot():
    await init_db()
    await load_all_profiles()
    await bot_client.start(bot_token=BOT_TOKEN)
    print("Management bot started. Use /start in Telegram with admin/special accounts.")
    await bot_client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(run_bot())
