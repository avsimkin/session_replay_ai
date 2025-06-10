import os
import json
import re
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import tempfile


class BigQueryReplayCollector:
    def __init__(self, credentials_path, project_id, dataset_id, table_id,
                 output_dataset_id='amplitude_session_replay'):
        """Инициализация коллектора Session Replay ID из BigQuery"""
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.output_dataset_id = output_dataset_id
        self.output_table_id = 'session_replay_urls'
        self.full_table_name = f"`{project_id}.{dataset_id}.{table_id}`"
        self.output_table_name = f"{project_id}.{output_dataset_id}.{self.output_table_id}"

        print("🔐 Настраиваем аутентификацию...")
        print(f"📁 Пытаемся загрузить credentials из: {credentials_path}")

        # Проверяем существование файла
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(f"❌ Файл credentials не найден: {credentials_path}")

        # Создаем credentials и client
        try:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )

            self.client = bigquery.Client(credentials=credentials, project=project_id)
            print(f"✅ Подключение к BigQuery установлено")
            print(f"📊 Исходная таблица: {self.full_table_name}")
            print(f"💾 Целевая таблица: {self.output_table_name}")

        except Exception as e:
            raise Exception(f"❌ Ошибка создания BigQuery client: {e}")

    def create_output_table(self):
        """Создание таблицы для хранения ссылок Session Replay"""
        try:
            # Проверяем существование датасета
            try:
                dataset = self.client.get_dataset(f"{self.project_id}.{self.output_dataset_id}")
                print(f"✅ Датасет {self.output_dataset_id} существует")
            except:
                print(f"📁 Создаем датасет {self.output_dataset_id}...")
                dataset = bigquery.Dataset(f"{self.project_id}.{self.output_dataset_id}")
                dataset.location = "US"
                dataset = self.client.create_dataset(dataset)
                print(f"✅ Датасет {self.output_dataset_id} создан")

            # Схема таблицы
            schema = [
                bigquery.SchemaField("record_date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("session_replay_url", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("collection_datetime", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("is_processed", "BOOLEAN", mode="REQUIRED"),
                bigquery.SchemaField("amplitude_id", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("session_replay_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("duration_seconds", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("events_count", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("processed_datetime", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("screenshots_count", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("drive_folder_id", "STRING", mode="NULLABLE"),
            ]

            # Проверяем существование таблицы
            try:
                table = self.client.get_table(self.output_table_name)
                print(f"✅ Таблица {self.output_table_id} уже существует")
                return table
            except:
                print(f"📊 Создаем таблицу {self.output_table_id}...")
                table_ref = bigquery.Table(self.output_table_name, schema=schema)
                table = self.client.create_table(table_ref)
                print(f"✅ Таблица {self.output_table_id} создана")
                return table

        except Exception as e:
            print(f"❌ Ошибка создания таблицы: {e}")
            raise

    def test_connection(self):
        """Тестирование подключения"""
        try:
            test_query = f"""
            SELECT COUNT(*) as total_events
            FROM {self.full_table_name}
            WHERE TIMESTAMP_TRUNC(event_time, DAY) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
            LIMIT 1
            """

            print("🔍 Тестируем подключение к BigQuery...")
            result = self.client.query(test_query).result()

            for row in result:
                print(f"✅ Подключение успешно! События за последние 7 дней: {row.total_events:,}")

            return True

        except Exception as e:
            print(f"❌ Ошибка тестирования подключения: {e}")
            return False

    def get_session_replay_ids_with_duration(self, start_date, end_date, min_duration_seconds=20, amplitude_id=None):
        """Получение Session Replay ID с фильтрацией по длительности сессии"""

        query = f"""
        WITH session_data AS (
            SELECT 
                amplitude_id,
                REGEXP_EXTRACT(
                    TO_JSON_STRING(event_properties),
                    r'"\[Amplitude\] Session Replay ID":"([^"]+)"'
                ) AS session_replay_id,
                REGEXP_EXTRACT(
                    TO_JSON_STRING(event_properties),
                    r'"\[Amplitude\] Session Replay ID":"[^/]+/(\d+)"'
                ) AS session_start_time_millis,
                SAFE_CAST(REGEXP_EXTRACT(
                    TO_JSON_STRING(event_properties),
                    r'"\[Amplitude\] Session Replay ID":"[^/]+/\d+/(\d+)"'
                ) AS INT64) AS session_duration_ms,
                event_time
            FROM {self.full_table_name}
            WHERE 
                TIMESTAMP_TRUNC(event_time, DAY) >= TIMESTAMP("{start_date}")
                AND TIMESTAMP_TRUNC(event_time, DAY) <= TIMESTAMP("{end_date}")
                AND TO_JSON_STRING(event_properties) LIKE '%Session Replay ID%'
        ),

        session_stats AS (
            SELECT 
                amplitude_id,
                session_replay_id,
                session_start_time_millis,
                session_duration_ms,
                COALESCE(
                    session_duration_ms,
                    CAST((MAX(UNIX_MILLIS(event_time)) - MIN(UNIX_MILLIS(event_time))) AS INT64)
                ) AS calculated_duration_ms,
                COUNT(*) as events_count,
                MIN(event_time) as first_event,
                MAX(event_time) as last_event
            FROM session_data
            WHERE session_replay_id IS NOT NULL 
                AND session_replay_id != ''
                AND session_start_time_millis IS NOT NULL
            GROUP BY amplitude_id, session_replay_id, session_start_time_millis, session_duration_ms
        )

        SELECT 
            amplitude_id,
            session_replay_id,
            session_start_time_millis,
            calculated_duration_ms,
            ROUND(calculated_duration_ms / 1000.0, 1) as duration_seconds,
            events_count,
            first_event,
            last_event,
            DATE(first_event) as record_date
        FROM session_stats
        WHERE calculated_duration_ms >= {min_duration_seconds * 1000}
        """

        if amplitude_id:
            query += f"\n    AND amplitude_id = {amplitude_id}"

        query += """
        ORDER BY amplitude_id, calculated_duration_ms DESC
        """

        print(f"🔍 Выполняем запрос за период {start_date} - {end_date}")
        print(f"⏱️ Минимальная длительность сессии: {min_duration_seconds} секунд")

        try:
            print("⏳ Выполняем запрос для получения Session Replay ID...")
            query_job = self.client.query(query)
            results_list = list(query_job.result())

            print(f"📊 Получено из BigQuery: {len(results_list)} записей")

            # Собираем данные
            data = []
            for row in results_list:
                data.append({
                    'amplitude_id': row.amplitude_id,
                    'session_replay_id': row.session_replay_id,
                    'session_start_time_millis': row.session_start_time_millis,
                    'duration_seconds': row.duration_seconds,
                    'events_count': row.events_count,
                    'record_date': row.record_date
                })

            return pd.DataFrame(data)

        except Exception as e:
            print(f"❌ Ошибка выполнения запроса: {e}")
            raise

    def format_replay_urls(self, df, project_id="258068",
                           base_url="https://app.amplitude.com/analytics/rn/session-replay"):
        """Форматирование DataFrame в ссылки на Session Replay"""

        urls_data = []
        current_time = datetime.now()

        print(f"🔗 Форматируем {len(df)} записей в URL...")

        for index, row in df.iterrows():
            try:
                amplitude_id = row['amplitude_id']
                session_replay_id = row['session_replay_id']
                session_start_time = row['session_start_time_millis']

                # Формируем URL
                url = (
                    f"{base_url}/project/{project_id}/"
                    f"search/amplitude_id%3D{amplitude_id}?"
                    f"sessionReplayId={session_replay_id}&"
                    f"sessionStartTime={session_start_time}"
                )

                # Подготавливаем запись для BigQuery
                url_record = {
                    'record_date': row['record_date'].strftime('%Y-%m-%d'),
                    'session_replay_url': url,
                    'collection_datetime': current_time.isoformat(),
                    'is_processed': False,
                    'amplitude_id': int(amplitude_id),
                    'session_replay_id': session_replay_id,
                    'duration_seconds': float(row['duration_seconds']),
                    'events_count': int(row['events_count']),
                    'processed_datetime': None,
                    'screenshots_count': None,
                    'drive_folder_id': None
                }

                urls_data.append(url_record)

            except Exception as e:
                print(f"⚠️ Ошибка форматирования URL для строки {index}: {e}")
                continue

        print(f"✅ Успешно сформировано {len(urls_data)} записей")
        return urls_data

    def check_existing_dates(self, start_date, end_date):
        """Проверка какие даты уже есть в таблице"""
        try:
            check_query = f"""
            SELECT DISTINCT record_date
            FROM `{self.output_table_name}`
            WHERE record_date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY record_date
            """

            result = self.client.query(check_query).result()
            existing_dates = {row.record_date for row in result}

            if existing_dates:
                print(f"📅 Найдены существующие данные за даты: {sorted(existing_dates)}")
                return existing_dates
            else:
                print(f"✅ Данных за период {start_date} - {end_date} в таблице нет")
                return set()

        except Exception as e:
            print(f"⚠️ Не удалось проверить существующие даты (возможно таблица не существует): {e}")
            return set()

    def filter_new_data(self, urls_data, existing_dates):
        """Фильтрация данных - оставляем только новые даты"""
        if not existing_dates:
            return urls_data

        original_count = len(urls_data)

        filtered_data = []
        for record in urls_data:
            record_date_str = record['record_date']
            record_date = datetime.strptime(record_date_str, '%Y-%m-%d').date()

            if record_date not in existing_dates:
                filtered_data.append(record)

        filtered_count = len(filtered_data)
        skipped_count = original_count - filtered_count

        if skipped_count > 0:
            print(f"🔄 Отфильтровано {skipped_count} записей (даты уже существуют)")
            print(f"✅ Осталось {filtered_count} новых записей для загрузки")

        return filtered_data

    def save_urls_to_bigquery(self, urls_data, start_date, end_date):
        """Сохранение URL в BigQuery таблицу через batch load (CSV)"""
        if not urls_data:
            print("⚠️ Нет данных для сохранения")
            return

        try:
            # Создаем таблицу если не существует
            self.create_output_table()

            # Проверяем существующие даты
            existing_dates = self.check_existing_dates(start_date, end_date)

            # Фильтруем данные - оставляем только новые даты
            urls_data = self.filter_new_data(urls_data, existing_dates)

            if not urls_data:
                print("ℹ️ Все данные за этот период уже существуют в таблице")
                return

            print(f"💾 Сохраняем {len(urls_data)} записей в BigQuery...")

            # Создаем DataFrame
            df = pd.DataFrame(urls_data)

            # Сохраняем во временный CSV файл
            with tempfile.NamedTemporaryFile(mode='w', suffix=".csv", delete=False) as temp_file:
                df.to_csv(temp_file.name, index=False, header=False)
                temp_file_path = temp_file.name

            try:
                # Настройка загрузки
                table_ref = self.client.dataset(self.output_dataset_id).table(self.output_table_id)
                job_config = bigquery.LoadJobConfig()
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                job_config.source_format = bigquery.SourceFormat.CSV
                job_config.autodetect = False

                # Загружаем файл в BigQuery
                with open(temp_file_path, "rb") as source_file:
                    job = self.client.load_table_from_file(source_file, table_ref, job_config=job_config)

                # Ждем завершения загрузки
                print("⏳ Загружаем данные в BigQuery...")
                job.result()

                print(f"✅ Успешно загружено {len(urls_data)} записей в BigQuery")

            finally:
                # Удаляем временный файл
                if os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)

        except Exception as e:
            print(f"❌ Ошибка сохранения в BigQuery: {e}")
            raise


def main():
    """Основная функция"""
    print("🚀 ЗАПУСК СБОРЩИКА SESSION REPLAY ID")
    print("=" * 50)

    CONFIG = {
        'credentials_path': '/Users/avsimkin/PycharmProjects/session_replay_ai/venv/bigquery-credentials.json',
        'project_id': 'codellon-dwh',
        'dataset_id': 'amplitude',
        'table_id': 'EVENTS_258068',
        'output_dataset_id': 'amplitude_session_replay',
        'amplitude_project_id': '258068',
        'min_duration_seconds': 20
    }

    days_back = 2
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)

    print(f"📅 Период сбора: {start_date} - {end_date}")
    print(f"⏱️ Минимальная длительность: {CONFIG['min_duration_seconds']} секунд")

    try:
        collector = BigQueryReplayCollector(
            credentials_path=CONFIG['credentials_path'],
            project_id=CONFIG['project_id'],
            dataset_id=CONFIG['dataset_id'],
            table_id=CONFIG['table_id'],
            output_dataset_id=CONFIG['output_dataset_id']
        )

        if not collector.test_connection():
            print("❌ Тест подключения не прошел. Завершаем работу.")
            return

        print(f"🔍 Собираем Session Replay ID...")
        df = collector.get_session_replay_ids_with_duration(
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            min_duration_seconds=CONFIG['min_duration_seconds']
        )

        if df.empty:
            print(f"⚠️ Не найдено ни одной сессии")
            return

        print(f"🔗 Форматируем URL...")
        urls_data = collector.format_replay_urls(
            df,
            project_id=CONFIG['amplitude_project_id']
        )

        if not urls_data:
            print("❌ Не удалось сформировать ни одного URL")
            return

        collector.save_urls_to_bigquery(urls_data, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

        print(f"\n🎉 ГОТОВО!")
        print(f"✅ Собрано и сохранено {len(urls_data)} Session Replay URL")
        print(f"💾 Таблица: {CONFIG['project_id']}.{CONFIG['output_dataset_id']}.session_replay_urls")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()