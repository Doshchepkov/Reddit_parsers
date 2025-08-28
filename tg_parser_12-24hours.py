from telethon import TelegramClient
from datetime import datetime, timedelta, timezone
import asyncio

from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

# ==============================
# 🔹 Настройки PostgreSQL
# ==============================
user = "postgres"
password = "di563066"
host = "localhost"
port = 5432
database = "tg_parser_12-24hours"

DB_URL = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"

Base = declarative_base()

class Post(Base):
    __tablename__ = "posts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    tg_id = Column(Integer, unique=True)
    channel = Column(String, nullable=False)
    date = Column(DateTime, nullable=False)
    text = Column(Text)
    views = Column(Integer)
    forwards = Column(Integer)
    replies_count = Column(Integer)

class Comment(Base):
    __tablename__ = "comments"
    id = Column(Integer, primary_key=True, autoincrement=True)
    tg_id = Column(Integer)
    post_tg_id = Column(Integer)
    date = Column(DateTime, nullable=False)
    text = Column(Text)

engine = create_engine(DB_URL)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# ==============================
# 🔹 Настройки Telegram API
# ==============================
api_id = 26510934
api_hash = '18b179e2b32734b6b5d7b38284c345e2'
session_name = 'my_session123'

CHANNELS = [
    't.me/DeCenter',
    't.me/RBCCrypto',
    't.me/binancekillers',
    't.me/binance_announcements',
    't.me/binance_ru',
    't.me/Coin_Post',
    't.me/markettwits',
    't.me/CryptoWorldNews',
    't.me/holder_pump_alert',
    't.me/ru_holder',
    't.me/forklog',
    't.me/toncoin_rus',
    't.me/the_club_100',
    't.me/incrypted',
    't.me/TheCryptoBoar',
    't.me/CRYPTUS_MEDIA',
    't.me/CrypTime_Channel',
    't.me/prometheus',
    't.me/GemResearch1',
    't.me/denischubchik',
    't.me/garantexnews'
]

client = TelegramClient(session_name, api_id, api_hash)
MOSCOW_TZ = timezone(timedelta(hours=3))  # Москва UTC+3


async def parse_channel(channel_link, start_time, end_time, session):
    entity = await client.get_entity(channel_link)

    async for msg in client.iter_messages(entity):
        if not msg.date:
            continue

        if msg.date < start_time:
            break

        if start_time <= msg.date <= end_time:
            existing_post = session.query(Post).filter_by(tg_id=msg.id).first()
            if not existing_post:
                post = Post(
                    tg_id=msg.id,
                    channel=channel_link,
                    date=msg.date,
                    text=msg.message or "",
                    views=msg.views,
                    forwards=msg.forwards,
                    replies_count=msg.replies.replies if msg.replies else 0,
                )
                session.add(post)
                session.commit()
                print(f"✅ Пост {msg.id} сохранен в БД (len={len(post.text)})")

            if msg.replies and msg.replies.replies > 0:
                try:
                    async for comment in client.iter_messages(entity, reply_to=msg.id):
                        if not comment.date:
                            continue
                        existing_comment = session.query(Comment).filter_by(tg_id=comment.id).first()
                        if not existing_comment:
                            comment_data = Comment(
                                tg_id=comment.id,
                                post_tg_id=msg.id,
                                date=comment.date,
                                text=comment.message or "",
                            )
                            session.add(comment_data)
                            session.commit()
                            print(f"   💬 Комментарий {comment.id} сохранен (len={len(comment_data.text)})")
                except Exception as e:
                    print(f"[!] Ошибка загрузки комментариев: {e}")


async def job():
    session = Session()
    now = datetime.now(MOSCOW_TZ)
    start_time = now - timedelta(hours=4)   # от сейчас до 4ч назад
    end_time = now

    print(f"⏳ Запуск парсинга: {start_time} → {end_time}")

    for channel in CHANNELS:
        await parse_channel(channel, start_time, end_time, session)

    session.close()
    print("✅ Парсинг завершен\n")


async def scheduler():
    """Запускаем каждые 4 часа начиная с 20:00"""
    while True:
        now = datetime.now(MOSCOW_TZ)
        # ближайшее время кратное 4 часам (20:00, 00:00, 04:00, 08:00 ...)
        next_run_hour = (now.hour // 4 + 1) * 4
        if next_run_hour >= 24:
            next_run_hour -= 24
            next_run_day = now + timedelta(days=1)
        else:
            next_run_day = now

        next_run = next_run_day.replace(hour=next_run_hour, minute=0, second=0, microsecond=0)

        wait_seconds = (next_run - now).total_seconds()
        print(f"⏳ Следующий запуск в {next_run} (через {wait_seconds/60:.1f} мин)")

        await asyncio.sleep(wait_seconds)
        await job()


async def main():
    await client.start()
    await job()         # сразу запустить при старте
    await scheduler()   # потом по расписанию


with client:
    client.loop.run_until_complete(main())
