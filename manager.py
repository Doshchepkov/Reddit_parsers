import schedule
import time
import subprocess

def run_script(script_name):
    print(f"[SCHEDULER] Запускаю {script_name} ...")
    try:
        subprocess.run(["python", script_name], check=True)
        print(f"[SCHEDULER] ✅ {script_name} успешно выполнен")
    except subprocess.CalledProcessError as e:
        print(f"[SCHEDULER] ❌ Ошибка выполнения {script_name}: {e}")
    except Exception as e:
        print(f"[SCHEDULER] ⚠️ Общая ошибка при запуске {script_name}: {e}")

# -------------------------
# 0–4 часа (6 раз в день)
for t in ["12:00", "16:00", "20:50", "00:00", "04:00", "08:00"]:
    schedule.every().day.at(t).do(run_script, "reddit_parser_0-4hours.py")

# 4–12 часов (3 раза в день)
for t in ["14:00", "22:00", "06:00"]:
    schedule.every().day.at(t).do(run_script, "reddit_parser_4-12hours.py")

# 12–24 часа (2 раза в день)
for t in ["15:00", "03:00"]:
    schedule.every().day.at(t).do(run_script, "reddit_parser_12-24hours.py")

print("[SCHEDULER] 🚀 Планировщик запущен. Жду заданий...")

while True:
    schedule.run_pending()
    time.sleep(30)  # проверка каждые 30 секунд
