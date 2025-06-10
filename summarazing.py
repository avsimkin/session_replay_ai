import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import openai
import os

# === НАСТРОЙКИ ===
BQ_PROJECT = 'codellon-dwh'
BQ_DATASET = 'amplitude_session_replay'
BQ_TABLE = 'replay_text_complete'
CREDENTIALS_PATH = '/Users/avsimkin/PycharmProjects/session_replay_ai/venv/bigquery-credentials.json'
OPENAI_API_KEY = "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"
DATE_FOR_SUMMARY = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')  # вчерашний день

# Авторизация
credentials = service_account.Credentials.from_service_account_file(
    CREDENTIALS_PATH,
    scopes=["https://www.googleapis.com/auth/bigquery"]
)
bq_client = bigquery.Client(credentials=credentials, project=BQ_PROJECT)
openai.api_key = OPENAI_API_KEY

def load_data_for_summary(date_str):
    """Загрузить данные из BigQuery за нужную дату"""
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    query = f"""
    SELECT user_id, country, session_length, device_type, session_replay_url, summary, sentiment, actions, cluster_description
    FROM `{table_id}`
    WHERE record_date = '{date_str}'
      AND summary IS NOT NULL AND summary != ''
    """
    df = bq_client.query(query).to_dataframe()
    print(f"📊 Загружено строк для саммари: {len(df)}")
    return df

def make_llm_input(df):
    """Сформировать пул для LLM"""
    rows = []
    for _, row in df.iterrows():
        rows.append(
            f"User: {row['user_id']}, Country: {row['country']}, Length: {row['session_length']}, "
            f"Device: {row['device_type']}, URL: {row['session_replay_url']}, "
            f"Summary: {row['summary']}, Sentiment: {row['sentiment']}, "
            f"Action: {row['actions']}, Cluster: {row['cluster_description']}"
        )
    return "\n".join(rows)

def make_prompt(all_summaries):
    """Сформировать промт для LLM"""
    prompt = f"""
Ты — опытный продуктовый аналитик. 
У тебя есть выгрузка сессий пользователей из Amplitude Session Replay, где для каждой сессии есть:
- User ID
- Гео (страна)
- Длина сессии
- Тип девайса
- Ссылка на запись
- Summary (краткое описание поведения пользователя)
- Sentiment (тональность: positive/negative/neutral)
- Action (что нужно предпринять для исправления)
- Кластер (тематика или сущность: регистрация, депозит, ставка и т.д.)

Твоя задача — сделать профессиональное саммари для команды продукта.
В саммари обязательно укажи:
1. Общая статистика (уникальные пользователи, страны, средняя длина сессии, распределение по девайсам, распределение по sentiment)
2. Топ-3 кластера (паттерна) с кратким описанием и количеством сессий
3. Action — ТОП-5 рекомендаций для продукта/разработки
4. Частые проблемы / баги (с примерами user_id или ссылок)

Форматируй ответ структурированно, с подзаголовками и списками. Пиши кратко, по делу, как для внутреннего отчёта продукта. Не повторяй текст из summary, а делай выводы и обобщения.

Вот данные для анализа:
{all_summaries}
"""
    return prompt

def get_llm_summary(prompt):
    """Отправить промт в OpenAI и получить саммари"""
    print("⏳ Отправляем запрос в OpenAI...")
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1200,
        temperature=0.3
    )
    summary_text = response['choices'][0]['message']['content']
    print("✅ Получено саммари от LLM")
    return summary_text

# (Опционально) отправка в Slack
def send_to_slack(summary_text, webhook_url):
    import requests
    resp = requests.post(webhook_url, json={"text": summary_text})
    print(f"📤 Отправлено в Slack: {resp.status_code}")

def main():
    df = load_data_for_summary(DATE_FOR_SUMMARY)
    if df.empty:
        print("Нет данных для саммари за выбранную дату.")
        return
    all_summaries = make_llm_input(df)
    prompt = make_prompt(all_summaries)
    summary_text = get_llm_summary(prompt)
    print("\n=== САММАРИ ===\n")
    print(summary_text)
    # Если нужно отправить в Slack:
    # send_to_slack(summary_text, "https://hooks.slack.com/services/XXX/YYY/ZZZ")

if __name__ == "__main__":
    main()