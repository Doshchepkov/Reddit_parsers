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
for t in ["15:00", "19:00", "23:00", "03:00", "07:00", "11:00"]:
    schedule.every().day.at(t).do(run_script, "reddit_parser_0-4hours.py")


for t in ["15:00", "19:00", "23:00", "03:00", "07:00", "11:00"]:
    schedule.every().day.at(t).do(run_script, "reddit_parser_4-8hours.py")

# 12–24 часа (2 раза в день)
for t in ["11:00", "01:00"]:
    schedule.every().day.at(t).do(run_script, "reddit_parser_12-24hours.py")

for t in ["11:00", "01:00"]:
    schedule.every().day.at(t).do(run_script, "reddit_parser_0-12hours.py")


print("[SCHEDULER] 🚀 Планировщик запущен. Жду заданий...")

while True:
    schedule.run_pending()
    time.sleep(30)  # проверка каждые 30 секунд
