import subprocess

print("=== Шаг 1: Собираем ссылки реплеев ===")
subprocess.run(["python", "collect_replay_links.py"], check=True)

print("=== Шаг 2: Собираем summary, скриним ===")
subprocess.run(["python", "replay_ai.py"], check=True)

print("=== Шаг 3: Делаем OCR и сохраняем в Excel ===")
subprocess.run(["python", "collect_replay_screens.py"], check=True)

print("Все готово! 🚀")