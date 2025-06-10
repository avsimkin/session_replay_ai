import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from collections import Counter
import nltk
import re

from google.cloud import bigquery
from google.oauth2 import service_account

nltk.download('stopwords')
from nltk.corpus import stopwords

# === НАСТРОЙКИ ===
BQ_PROJECT = 'codellon-dwh'
BQ_DATASET = 'amplitude_session_replay'
BQ_TABLE = 'replay_text_complete'
CREDENTIALS_PATH = '/Users/avsimkin/PycharmProjects/session_replay_ai/venv/bigquery-credentials.json'

credentials = service_account.Credentials.from_service_account_file(
    CREDENTIALS_PATH,
    scopes=["https://www.googleapis.com/auth/bigquery"]
)
bq_client = bigquery.Client(credentials=credentials, project=BQ_PROJECT)

def get_rows_without_clusters():
    """Загрузить строки из BigQuery, где advanced_cluster или cluster_description пустые"""
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    query = f"""
    SELECT * FROM `{table_id}`
    WHERE advanced_cluster IS NULL OR cluster_description IS NULL
    """
    df = bq_client.query(query).to_dataframe()
    print(f"📊 Загружено строк без кластеров: {len(df)}")
    return df

def extract_features_advanced(row):
    summary = str(row.get('summary', '')).lower()
    actions = str(row.get('actions', '')).lower()
    sentiment = str(row.get('sentiment', '')).lower()
    combined_text = f"{summary} {actions} {sentiment}"
    features = {}
    features['navigation'] = int(any(word in combined_text for word in [
        'main page', 'navigate', 'opened', 'clicked', 'menu', 'link'
    ]))
    features['payment'] = int(any(word in combined_text for word in [
        'deposit', 'payment', 'cash', 'money', 'balance', 'refill', 'pay'
    ]))
    features['betting'] = int(any(word in combined_text for word in [
        'bet', 'betting', 'stake', 'wager', 'place', 'odds'
    ]))
    features['gaming'] = int(any(word in combined_text for word in [
        'game', 'gaming', 'stream', 'live', 'match', 'sport'
    ]))
    features['auth'] = int(any(word in combined_text for word in [
        'login', 'register', 'authorization', 'auth', 'sign'
    ]))
    features['mobile'] = int(any(word in combined_text for word in [
        'mobile', 'app', 'download', 'apk', 'application'
    ]))
    features['tech_error'] = int(any(word in combined_text for word in [
        'error', 'fail', 'invalid', 'refused', 'not working', 'timeout'
    ]))
    features['ux_issue'] = int(any(word in combined_text for word in [
        'confused', 'unclear', 'difficult', 'complicated', 'lost'
    ]))
    features['performance'] = int(any(word in combined_text for word in [
        'slow', 'loading', 'lag', 'freeze', 'stuck'
    ]))
    features['successful'] = int(any(word in combined_text for word in [
        'successful', 'completed', 'finished', 'achieved'
    ]))
    event_total = row.get('event_total', 0)
    try:
        features['event_count'] = min(int(event_total) / 20.0, 1.0)
    except:
        features['event_count'] = 0
    session_length = str(row.get('session_length', '')).lower()
    if 'h' in session_length:
        features['long_session'] = 1
    elif 'm' in session_length:
        features['medium_session'] = 1
    else:
        features['short_session'] = 1
    for key in ['long_session', 'medium_session', 'short_session']:
        if key not in features:
            features[key] = 0
    return features

def smart_categorize(row, features):
    if features['payment'] and features['tech_error']:
        return 'Проблемы с депозитами/платежами'
    if features['mobile'] and (features['tech_error'] or 'download' in str(row['summary']).lower()):
        return 'Проблемы с мобильным приложением'
    if features['auth'] and features['tech_error']:
        return 'Проблемы с авторизацией'
    if features['betting'] and features['tech_error']:
        return 'Проблемы со ставками'
    if features['payment'] and features['successful']:
        return 'Успешные депозиты'
    if features['betting'] and features['successful']:
        return 'Успешные ставки'
    if features['gaming'] or features['betting']:
        return 'Игровая активность'
    if features['navigation'] and features['event_count'] > 0.5:
        return 'Активная навигация'
    if features['long_session'] and features['event_count'] > 0.3:
        return 'Длительные активные сессии'
    if features['short_session'] and features['event_count'] < 0.2:
        return 'Короткие/неактивные сессии'
    if features['mobile']:
        return 'Мобильная активность'
    if features['performance']:
        return 'Проблемы производительности'
    if features['ux_issue']:
        return 'UX проблемы'
    sentiment_label = row.get('sentiment_label', '')
    if sentiment_label == 'negative':
        return 'Негативный опыт'
    elif sentiment_label == 'positive' and features['successful']:
        return 'Позитивный опыт'
    return 'Обычная активность'

def extract_sentiment(text):
    text = str(text).lower()
    if 'positive' in text:
        return 'positive'
    elif 'negative' in text:
        return 'negative'
    elif 'neutral' in text:
        return 'neutral'
    return 'unknown'

def has_problem_advanced(row):
    if row['sentiment_label'] == 'negative':
        return 1
    combined_text = str(row['summary']) + ' ' + str(row['actions'])
    problem_indicators = [
        'error', 'fail', 'invalid', 'refused', 'not working', 'timeout',
        'unable', 'cannot', 'problem', 'issue', 'difficulty'
    ]
    if any(indicator in combined_text.lower() for indicator in problem_indicators):
        return 1
    if row['sentiment_label'] == 'neutral':
        negative_actions = ['did not', 'failed to', 'unsuccessful', 'incomplete']
        if any(action in combined_text.lower() for action in negative_actions):
            return 1
    return 0

def detect_problem_source_advanced(row):
    combined_text = (str(row['summary']) + ' ' + str(row['actions'])).lower()
    if any(w in combined_text for w in ['deposit', 'payment', 'cash', 'money', 'balance', 'refill']):
        return 'депозит'
    elif any(w in combined_text for w in ['mobile', 'app', 'download', 'apk']):
        return 'мобильное приложение'
    elif any(w in combined_text for w in ['bet', 'betting', 'stake', 'wager']):
        return 'ставки'
    elif any(w in combined_text for w in ['login', 'register', 'auth', 'sign']):
        return 'регистрация/логин'
    elif any(w in combined_text for w in ['game', 'gaming', 'stream']):
        return 'игра'
    elif any(w in combined_text for w in ['navigation', 'menu', 'page']):
        return 'навигация'
    else:
        return 'прочее'

def main():
    print("🚀 ЗАПУСК ДОПОЛНЕНИЯ КЛАСТЕРОВ В BigQuery")
    df = get_rows_without_clusters()
    if df.empty:
        print("✅ Нет строк для обновления!")
        return

    # Извлечение признаков
    features_list = []
    for _, row in df.iterrows():
        features = extract_features_advanced(row)
        features_list.append(features)
    features_df = pd.DataFrame(features_list)

    # Smart категоризация
    smart_categories = []
    for i, row in df.iterrows():
        category = smart_categorize(row, features_list[i])
        smart_categories.append(category)
    df['smart_category'] = smart_categories

    # Sentiment
    df['sentiment_label'] = df['sentiment'].apply(extract_sentiment)

    # Флаг проблемы
    df['has_problem'] = df.apply(has_problem_advanced, axis=1)

    # Источник проблемы
    df['problem_source'] = df.apply(detect_problem_source_advanced, axis=1)

    # Кластеризация
    texts = (
            df['summary'].fillna('') + ' ' +
            df['sentiment'].fillna('') + ' ' +
            df['actions'].fillna('')
    ).values

    russian_stopwords = set(stopwords.words("russian"))
    extra_stopwords = set([
        'user', 'session', 'began', 'application', 'the', 'and', 'to', 'a', 'in', 'with',
        'click', 'entered', 'selected', 'form', 'page'
    ])

    def clean_text(text):
        text = text.lower()
        words = [w for w in text.split() if w.isalpha() and len(w) > 2 and
                 w not in russian_stopwords and w not in extra_stopwords]
        return " ".join(words)

    texts_clean = [clean_text(t) for t in texts]
    vectorizer = TfidfVectorizer(max_features=1000, min_df=1, max_df=0.8)
    text_features = vectorizer.fit_transform(texts_clean)
    scaler = StandardScaler()
    numeric_features = scaler.fit_transform(features_df.values)
    from scipy.sparse import hstack
    combined_features = hstack([text_features, numeric_features])
    n_clusters = min(8, len(df) // 2) if len(df) > 1 else 1
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    cluster_labels = kmeans.fit_predict(combined_features.toarray())
    df['advanced_cluster'] = cluster_labels

    # Описания кластеров
    cluster_descriptions = []
    for cluster_id in sorted(set(cluster_labels)):
        cluster_mask = df['advanced_cluster'] == cluster_id
        cluster_data = df[cluster_mask]
        cluster_categories = cluster_data['smart_category'].value_counts()
        top_category = cluster_categories.index[0] if len(cluster_categories) > 0 else "Неизвестно"
        description = f"{top_category}"
        if len(cluster_categories) > 1:
            description += f" + {cluster_categories.index[1]}"
        cluster_descriptions.append(description)
    cluster_desc_map = {i: desc for i, desc in enumerate(cluster_descriptions)}
    df['cluster_description'] = df['advanced_cluster'].map(cluster_desc_map)

    # === Загрузка обратно в BigQuery (только новые кластеры) ===
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    client = bq_client

    # Обновляем только нужные строки по session_id
    for _, row in df.iterrows():
        session_id = row['session_id']
        update_query = f"""
        UPDATE `{table_id}`
        SET
            advanced_cluster = @advanced_cluster,
            cluster_description = @cluster_description,
            smart_category = @smart_category,
            has_problem = @has_problem,
            problem_source = @problem_source,
            sentiment_label = @sentiment_label
        WHERE session_id = @session_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("advanced_cluster", "INT64", int(row['advanced_cluster'])),
                bigquery.ScalarQueryParameter("cluster_description", "STRING", str(row['cluster_description'])),
                bigquery.ScalarQueryParameter("smart_category", "STRING", str(row['smart_category'])),
                bigquery.ScalarQueryParameter("has_problem", "INT64", int(row['has_problem'])),
                bigquery.ScalarQueryParameter("problem_source", "STRING", str(row['problem_source'])),
                bigquery.ScalarQueryParameter("sentiment_label", "STRING", str(row['sentiment_label'])),
                bigquery.ScalarQueryParameter("session_id", "STRING", str(session_id)),
            ]
        )
        client.query(update_query, job_config=job_config).result()
        print(f"✅ Обновлено для session_id: {session_id}")

    print("🎉 Кластеры успешно добавлены в BigQuery!")

if __name__ == "__main__":
    main()