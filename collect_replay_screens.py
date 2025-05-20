import os
import re
import pytesseract
from PIL import Image
import pandas as pd

# Путь к папке со скринами
BASE_DIR = "/Users/avsimkin/PycharmProjects/session_replay_ai/venv/screens/"

# Если нужно явно указать путь к бинарнику tesseract (если вдруг будет ошибка)
# pytesseract.pytesseract.tesseract_cmd = '/usr/local/bin/tesseract'
# или, если у тебя M1/M2 мак:
# pytesseract.pytesseract.tesseract_cmd = '/opt/homebrew/bin/tesseract'

data = []

for fname in os.listdir(BASE_DIR):
    if not fname.lower().endswith(('.png', '.jpg', '.jpeg')):
        continue

    # Пример: 87030a63-bef8-46d5-b939-eabba5ffa5a3_1747371475211_944438dc_actions.png
    match = re.match(r'(.+?)_(summary|actions|sentiment)\.png$', fname)
    if not match:
        continue  # не тот формат файла

    session_id, block_type = match.groups()
    img_path = os.path.join(BASE_DIR, fname)
    print(f"Обрабатываем: {img_path}")

    # OCR
    try:
        img = Image.open(img_path)
        text = pytesseract.image_to_string(img)
    except Exception as e:
        text = f"[ERROR] {e}"

    # Ищем уже созданную строку по session_id, иначе создаём новую
    found = False
    for row in data:
        if row['session_id'] == session_id:
            row[block_type] = text
            found = True
            break
    if not found:
        row = {'session_id': session_id, 'summary': '', 'actions': '', 'sentiment': ''}
        row[block_type] = text
        data.append(row)

# Сохраняем в Excel
df = pd.DataFrame(data)
excel_path = os.path.join(BASE_DIR, "replay_text_summary.xlsx")
df.to_excel(excel_path, index=False)
print(f"✅ Экспортировано в {excel_path}")