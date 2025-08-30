import praw
from prawcore.exceptions import RequestException, Forbidden, NotFound, Redirect
from datetime import datetime, timedelta, timezone
import pytz
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Text, ForeignKey, Float, Boolean
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
import time
import random
import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import queue
import ast

# --- Reddit API ---
with open("reddit_keys.json", "r", encoding="utf-8") as f:
    keys = json.load(f)

reddit_clients = [
    praw.Reddit(
        client_id=key["client_id"],
        client_secret=key["client_secret"],
        user_agent=key["user_agent"]
    )
    for key in keys
]

# --- DB ---
user = "postgres"
password = ""
host = "localhost"
port = 5432
database = "reddit_parser_12-24hours"

DB_URL = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"

Base = declarative_base()
engine = create_engine(DB_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)

# --- Модели ---
class Post(Base):
    __tablename__ = "posts"
    id = Column(String, primary_key=True)
    title = Column(Text)
    selftext = Column(Text)
    subreddit = Column(String)
    author = Column(String)
    created_at = Column(DateTime)
    score = Column(Integer)
    num_comments = Column(Integer)
    # Новые колонки
    upvote_ratio = Column(Float)
    is_video = Column(Boolean)
    locked = Column(Boolean)
    total_awards_received = Column(Integer)
    link_flair_text = Column(Text)
    num_crossposts = Column(Integer)
    is_self = Column(Boolean)
    distinguished = Column(String)
    mentioned_tickers = Column(Text)  # Будет пустой строкой

    comments = relationship("Comment", back_populates="post", cascade="all, delete-orphan")
class Comment(Base):
    __tablename__ = "comments"
    id = Column(String, primary_key=True)
    body = Column(Text)
    author = Column(String)
    created_at = Column(DateTime)
    score = Column(Integer)
    parent_id = Column(String)
    link_id = Column(String, ForeignKey("posts.id"))
    post = relationship("Post", back_populates="comments")

Base.metadata.create_all(engine)

# --- Время (12 → 4 часа назад по МСК) ---
moscow_tz = pytz.timezone("Europe/Moscow")
now_msk = datetime.now(moscow_tz)
end_time = now_msk - timedelta(hours=12)      # граница 4 часа назад
start_time = now_msk - timedelta(hours=24)   # граница 12 часов назад

print(f"Парсим с {start_time} до {end_time} (МСК)")

start_time_utc = start_time.astimezone(timezone.utc).timestamp()
end_time_utc = end_time.astimezone(timezone.utc).timestamp()

# --- Сабреддиты ---
with open("subs_final.txt", "r", encoding="utf-8") as f:
    content = f.read().strip()
subreddits = list(ast.literal_eval(content))

# --- Очередь ---
task_queue = queue.Queue()
for sub in subreddits:
    task_queue.put(sub)

POST_LIMIT = 120   # теперь берём до 120 постов

def check_with_all_clients(sub_name):
    for client in reddit_clients:
        try:
            for _ in client.subreddit(sub_name).new(limit=1):
                return True
        except Forbidden:
            continue
        except Exception:
            continue
    return False

# --- Синхронная функция для одного клиента (воркер) ---
def fetch_worker(reddit):
    with SessionLocal() as session:
        while not task_queue.empty():
            try:
                sub_name = task_queue.get_nowait()
            except queue.Empty:
                break

            subreddit = reddit.subreddit(sub_name)
            print(f"\n📥 Сабреддит: {sub_name}")

            try:
                posts_fetched = 0
                for post in subreddit.new(limit=POST_LIMIT):
                    # фильтруем по интервалу [12h, 4h]
                    if start_time_utc <= post.created_utc <= end_time_utc:
                        post_obj = Post(
    id=post.id,
    title=post.title,
    selftext=post.selftext,
    subreddit=post.subreddit.display_name,
    author=str(post.author) if post.author else None,
    created_at=datetime.fromtimestamp(post.created_utc, tz=timezone.utc),
    score=post.score,
    num_comments=post.num_comments,

    # Новые поля
    upvote_ratio=getattr(post, "upvote_ratio", None),
    is_video=getattr(post, "is_video", None),
    locked=getattr(post, "locked", None),
    total_awards_received=getattr(post, "total_awards_received", None),
    link_flair_text=getattr(post, "link_flair_text", None),
    num_crossposts=getattr(post, "num_crossposts", None),
    is_self=getattr(post, "is_self", None),
    distinguished=getattr(post, "distinguished", None),
    mentioned_tickers=""  # пока пустая строка
)
                        session.merge(post_obj)
                        posts_fetched += 1

                        # теперь берём ВСЕ комментарии (без фильтра по времени)
                        try:
                            post.comments.replace_more(limit=None)
                            for c in post.comments.list():
                                comment_obj = Comment(
                                    id=c.id,
                                    body=c.body,
                                    author=str(c.author) if c.author else None,
                                    created_at=datetime.fromtimestamp(c.created_utc, tz=timezone.utc),
                                    score=c.score,
                                    parent_id=c.parent_id,
                                    link_id=post.id
                                )
                                session.merge(comment_obj)
                        except Exception as e:
                            print(f"⚠️ Ошибка комментариев поста {post.id}: {e}")
                            continue

                session.commit()
                print(f"{sub_name}: получено {posts_fetched} постов")

            except Forbidden:
                print(f"🚫 {sub_name} дал 403. Проверяем другими ключами...")
                if not check_with_all_clients(sub_name):
                    print(f"🚫 {sub_name} приватный у всех, пропускаем.")
                else:
                    print(f"✅ {sub_name} оказался доступен другим ключом, не удаляем.")
                continue

            except NotFound:
                print(f"❌ {sub_name} не найден (404), удаляю из списка.")
                remove_subreddit_from_file(sub_name)
                continue

            except Redirect:
                print(f"❌ {sub_name} редирект → поиск, удаляю из списка.")
                remove_subreddit_from_file(sub_name)
                continue

            except RequestException as e:
                print(f"⚠️ Ошибка сети при сабреддите {sub_name}: {e}")
                time.sleep(random.uniform(1, 3))
                continue

            except Exception as e:
                print(f"⚠️ Неизвестная ошибка при сабреддите {sub_name}: {e}")
                time.sleep(random.uniform(1, 3))
                continue

            finally:
                task_queue.task_done()

            time.sleep(2.4)

# --- Удаление сабреддита ---
def remove_subreddit_from_file(sub_name, filename="subs_final.txt"):
    with open(filename, "r", encoding="utf-8") as f:
        subs = list(ast.literal_eval(f.read().strip()))
    if sub_name in subs:
        subs.remove(sub_name)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(str(subs))
        print(f"🗑 {sub_name} удалён из {filename}")

# --- Асинхронный запуск ---
async def main():
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=len(reddit_clients)) as executor:
        tasks = [loop.run_in_executor(executor, fetch_worker, client) for client in reddit_clients]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
    print("\n✅ Данные за последние 12→4 часов сохранены")
