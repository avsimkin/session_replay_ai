import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from collections import Counter
import nltk
import re
import os
import sys
from datetime import datetime
from typing import Callable, Optional

# Импорты для Google Cloud
from google.cloud import bigquery
from google.oauth2 import service_account

# Добавляем путь к корню проекта для импорта config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from config.settings import settings
except ImportError:
    class MockSettings:
        GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '/etc/secrets/bigquery-credentials.json')
        BQ_PROJECT_ID = os.environ.get('BQ_PROJECT_ID', 'codellon-dwh')
        BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID', 'amplitude_session_replay')
        BQ_CLUSTERING_TABLE = os.environ.get('BQ_CLUSTERING_TABLE', 'replay_text_complete')
    settings = MockSettings()

class ClusteringAnalysisProcessor:
    def __init__(self, status_callback: Optional[Callable[[str, int], None]] = None):
        self.status_callback = status_callback
        self.credentials_path = settings.GOOGLE_APPLICATION_CREDENTIALS
        self.bq_project_id = settings.BQ_PROJECT_ID
        self.bq_dataset_id = settings.BQ_DATASET_ID
        self.bq_table = settings.BQ_CLUSTERING_TABLE
        
        # NLTK data path для Render
        nltk_data_path = os.environ.get('NLTK_DATA', '/opt/render/project/src/nltk_data')
        if os.path.exists(nltk_data_path):
            nltk.data.path.append(nltk_data_path)
        
        # Статистика работы
        self.start_time = None
        self.total_processed = 0
        self.total_successful = 0
        self.total_failed = 0
        
        self._update_status("🔐 Настраиваем подключения...", 1)
        self._init_clients()
        self._setup_nltk()

    def _update_status(self, details: str, progress: int):
        if self.status_callback: 
            self.status_callback(details, progress)
        if progress != -1: 
            print(f"[{progress}%] {details}")
        else:
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"[{timestamp}] {details}")

    def _init_clients(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self.bq_client = bigquery.Client(credentials=credentials, project=self.bq_project_id)
            self._update_status("✅ BigQuery подключен", 5)
        except Exception as e:
            raise Exception(f"❌ Ошибка подключения к BigQuery: {e}")

    def _setup_nltk(self):
        """Настройка NLTK с обработкой ошибок"""
        try:
            # Пытаемся скачать stopwords если их нет
            try:
                from nltk.corpus import stopwords
                # Проверяем доступность
                stopwords.words("russian")
                self._update_status("✅ NLTK stopwords доступны", -1)
            except:
                self._update_status("⬇️ Скачиваем NLTK stopwords...", -1)
                nltk.download('stopwords', quiet=True)
                from nltk.corpus import stopwords
                
            self.russian_stopwords = set(stopwords.words("russian"))
            self.extra_stopwords = set([
                'user', 'session', 'began', 'application', 'the', 'and', 'to', 'a', 'in', 'with',
                'click', 'entered', 'selected', 'form', 'page'
            ])
            
        except Exception as e:
            self._update_status(f"⚠️ NLTK недоступен, используем базовые stopwords: {e}", -1)
            # Fallback список stopwords
            self.russian_stopwords = set(['и', 'в', 'на', 'с', 'по', 'для', 'не', 'от', 'до', 'из'])
            self.extra_stopwords = set([
                'user', 'session', 'began', 'application', 'the', 'and', 'to', 'a', 'in', 'with',
                'click', 'entered', 'selected', 'form', 'page'
            ])

    def get_rows_without_clusters(self):
        """Загрузить строки из BigQuery, где кластеры пустые"""
        table_id = f"{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_table}"
        query = f"""
        SELECT * FROM `{table_id}`
        WHERE advanced_cluster IS NULL OR cluster_description IS NULL
        """
        
        self._update_status("🔍 Получаем строки без кластеров из BigQuery...", 10)
        
        try:
            df = self.bq_client.query(query).to_dataframe()
            self._update_status(f"📊 Загружено строк без кластеров: {len(df)}", 15)
            return df
        except Exception as e:
            self._update_status(f"❌ Ошибка загрузки данных: {e}", -1)
            raise

    def extract_features_advanced(self, row):
        """Извлечение продвинутых признаков из данных сессии"""
        summary = str(row.get('summary', '')).lower()
        actions = str(row.get('actions', '')).lower()
        sentiment = str(row.get('sentiment', '')).lower()
        combined_text = f"{summary} {actions} {sentiment}"
        
        features = {}
        
        # Функциональные признаки
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
        
        # Проблемные признаки
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
        
        # Количественные признаки
        event_total = row.get('event_total', 0)
        try:
            features['event_count'] = min(int(event_total) / 20.0, 1.0)
        except:
            features['event_count'] = 0
        
        # Длительность сессии
        session_length = str(row.get('session_length', '')).lower()
        features['long_session'] = 1 if 'h' in session_length else 0
        features['medium_session'] = 1 if 'm' in session_length and 'h' not in session_length else 0
        features['short_session'] = 1 if features['long_session'] == 0 and features['medium_session'] == 0 else 0
        
        return features

    def smart_categorize(self, row, features):
        """Умная категоризация на основе признаков"""
        # Проблемные категории
        if features['payment'] and features['tech_error']:
            return 'Проблемы с депозитами/платежами'
        if features['mobile'] and (features['tech_error'] or 'download' in str(row['summary']).lower()):
            return 'Проблемы с мобильным приложением'
        if features['auth'] and features['tech_error']:
            return 'Проблемы с авторизацией'
        if features['betting'] and features['tech_error']:
            return 'Проблемы со ставками'
        
        # Успешные категории
        if features['payment'] and features['successful']:
            return 'Успешные депозиты'
        if features['betting'] and features['successful']:
            return 'Успешные ставки'
        
        # Активность
        if features['gaming'] or features['betting']:
            return 'Игровая активность'
        if features['navigation'] and features['event_count'] > 0.5:
            return 'Активная навигация'
        if features['long_session'] and features['event_count'] > 0.3:
            return 'Длительные активные сессии'
        if features['short_session'] and features['event_count'] < 0.2:
            return 'Короткие/неактивные сессии'
        
        # Специфичные проблемы
        if features['mobile']:
            return 'Мобильная активность'
        if features['performance']:
            return 'Проблемы производительности'
        if features['ux_issue']:
            return 'UX проблемы'
        
        # По sentiment
        sentiment_label = row.get('sentiment_label', '')
        if sentiment_label == 'negative':
            return 'Негативный опыт'
        elif sentiment_label == 'positive' and features['successful']:
            return 'Позитивный опыт'
        
        return 'Обычная активность'

    def extract_sentiment(self, text):
        """Извлечение sentiment из текста"""
        text = str(text).lower()
        if 'positive' in text:
            return 'positive'
        elif 'negative' in text:
            return 'negative'
        elif 'neutral' in text:
            return 'neutral'
        return 'unknown'

    def has_problem_advanced(self, row):
        """Определение наличия проблемы"""
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

    def detect_problem_source_advanced(self, row):
        """Определение источника проблемы"""
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

    def clean_text(self, text):
        """Очистка текста для TF-IDF"""
        text = text.lower()
        words = [w for w in text.split() if w.isalpha() and len(w) > 2 and
                 w not in self.russian_stopwords and w not in self.extra_stopwords]
        return " ".join(words)

    def update_session_in_bq(self, session_data):
        """Обновление одной сессии в BigQuery"""
        table_id = f"{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_table}"
        
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
        
        try:
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("advanced_cluster", "INT64", int(session_data['advanced_cluster'])),
                    bigquery.ScalarQueryParameter("cluster_description", "STRING", str(session_data['cluster_description'])),
                    bigquery.ScalarQueryParameter("smart_category", "STRING", str(session_data['smart_category'])),
                    bigquery.ScalarQueryParameter("has_problem", "INT64", int(session_data['has_problem'])),
                    bigquery.ScalarQueryParameter("problem_source", "STRING", str(session_data['problem_source'])),
                    bigquery.ScalarQueryParameter("sentiment_label", "STRING", str(session_data['sentiment_label'])),
                    bigquery.ScalarQueryParameter("session_id", "STRING", str(session_data['session_id'])),
                ]
            )
            self.bq_client.query(update_query, job_config=job_config).result()
            return True
        except Exception as e:
            self._update_status(f"❌ Ошибка обновления session_id {session_data['session_id']}: {e}", -1)
            return False

    def run(self):
        """Основной метод кластеризации и анализа"""
        self.start_time = datetime.now()
        
        self._update_status("🔄 ЗАПУСК КЛАСТЕРИЗАЦИИ И АНАЛИЗА", 20)
        
        # Получаем данные для обработки
        df = self.get_rows_without_clusters()
        
        if df.empty:
            self._update_status("✅ Нет данных для кластеризации!", 100)
            return {"status": "no_data", "message": "Все данные уже кластеризованы"}

        self._update_status(f"📊 Начинаем анализ {len(df)} записей", 25)
        
        try:
            # Извлечение признаков
            self._update_status("🔧 Извлекаем признаки...", 30)
            features_list = []
            for _, row in df.iterrows():
                features = self.extract_features_advanced(row)
                features_list.append(features)
            features_df = pd.DataFrame(features_list)

            # Smart категоризация
            self._update_status("🏷️ Применяем умную категоризацию...", 40)
            smart_categories = []
            for i, row in df.iterrows():
                category = self.smart_categorize(row, features_list[i])
                smart_categories.append(category)
            df['smart_category'] = smart_categories

            # Обработка sentiment
            self._update_status("😊 Анализируем sentiment...", 45)
            df['sentiment_label'] = df['sentiment'].apply(self.extract_sentiment)

            # Определение проблем
            self._update_status("🔍 Определяем проблемы...", 50)
            df['has_problem'] = df.apply(self.has_problem_advanced, axis=1)
            df['problem_source'] = df.apply(self.detect_problem_source_advanced, axis=1)

            # Кластеризация
            self._update_status("🎯 Выполняем кластеризацию...", 60)
            
            # Подготовка текстов
            texts = (
                df['summary'].fillna('') + ' ' +
                df['sentiment'].fillna('') + ' ' +
                df['actions'].fillna('')
            ).values

            texts_clean = [self.clean_text(t) for t in texts]
            
            # TF-IDF векторизация
            vectorizer = TfidfVectorizer(max_features=1000, min_df=1, max_df=0.8)
            text_features = vectorizer.fit_transform(texts_clean)
            
            # Стандартизация числовых признаков
            scaler = StandardScaler()
            numeric_features = scaler.fit_transform(features_df.values)
            
            # Объединение признаков
            from scipy.sparse import hstack
            combined_features = hstack([text_features, numeric_features])
            
            # K-means кластеризация
            n_clusters = min(8, len(df) // 2) if len(df) > 1 else 1
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            cluster_labels = kmeans.fit_predict(combined_features.toarray())
            df['advanced_cluster'] = cluster_labels

            # Создание описаний кластеров
            self._update_status("📝 Создаем описания кластеров...", 70)
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

            # Обновление в BigQuery
            self._update_status("💾 Сохраняем результаты в BigQuery...", 80)
            
            for i, (_, row) in enumerate(df.iterrows()):
                progress = 80 + int((i / len(df)) * 15)
                self._update_status(f"💾 Обновляем запись {i+1}/{len(df)}", progress)
                
                success = self.update_session_in_bq(row.to_dict())
                if success:
                    self.total_successful += 1
                else:
                    self.total_failed += 1
                
                self.total_processed += 1

        except Exception as e:
            self._update_status(f"❌ Критическая ошибка в кластеризации: {e}", -1)
            raise

        # Финальная статистика
        total_time = datetime.now() - self.start_time
        result = {
            "status": "completed",
            "total_processed": self.total_processed,
            "total_successful": self.total_successful,
            "total_failed": self.total_failed,
            "success_rate": f"{(self.total_successful/self.total_processed*100):.1f}%" if self.total_processed > 0 else "0%",
            "clusters_created": len(set(cluster_labels)) if 'cluster_labels' in locals() else 0,
            "total_time_minutes": round(total_time.total_seconds() / 60, 1)
        }

        self._update_status(f"🏁 КЛАСТЕРИЗАЦИЯ ЗАВЕРШЕНА!", 100)
        self._update_status(f"📊 Обработано: {self.total_processed}, Успешно: {self.total_successful}, Кластеров: {result['clusters_created']}", 100)
        
        return result


def main():
    """Основная функция для запуска кластеризации"""
    try:
        def console_status_callback(details: str, progress: int):
            if progress != -1:
                print(f"[{progress}%] {details}")
            else:
                print(f"[INFO] {details}")

        processor = ClusteringAnalysisProcessor(status_callback=console_status_callback)
        result = processor.run()
        
        print(f"\n🏁 Финальный результат: {result}")
        return result

    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "error": str(e)}


if __name__ == "__main__":
    main()