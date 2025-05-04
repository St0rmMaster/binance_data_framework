#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Модуль для работы с облачной базой данных Google Cloud SQL для хранения данных Binance.
"""

import os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple, Union
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CloudDataManager:
    """
    Класс для управления облачной базой данных Google Cloud SQL для хранения исторических данных.
    """
    
    def __init__(self, 
                 db_user: str, 
                 db_password: str, 
                 db_name: str,
                 project_id: str,
                 instance_name: str,
                 region: str = 'us-central1',
                 connection_name: Optional[str] = None,
                 use_proxy: bool = False):
        """
        Инициализация менеджера облачной базы данных.
        
        Args:
            db_user: Имя пользователя базы данных
            db_password: Пароль пользователя базы данных
            db_name: Имя базы данных
            project_id: ID проекта Google Cloud
            instance_name: Имя экземпляра Cloud SQL
            region: Регион Google Cloud (по умолчанию 'us-central1')
            connection_name: Имя подключения (по умолчанию None)
            use_proxy: Использовать ли Cloud SQL Proxy (по умолчанию False)
        """
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name
        self.project_id = project_id
        self.instance_name = instance_name
        self.region = region
        
        # Создание имени подключения, если оно не указано
        if connection_name is None:
            self.connection_name = f"{project_id}:{region}:{instance_name}"
        else:
            self.connection_name = connection_name
            
        self.use_proxy = use_proxy
        self.engine = None
        self.conn = None
        
        # Создаем подключение к базе данных
        self._connect()
        
        # Создаем таблицы при инициализации
        self.initialize_db()
    
    def _connect(self) -> bool:
        """
        Устанавливает соединение с Google Cloud SQL.
        
        Returns:
            bool: True, если соединение успешно, иначе False
        """
        try:
            if self.use_proxy:
                # Подключение через Cloud SQL Proxy
                connection_url = f"mysql+pymysql://{self.db_user}:{self.db_password}@127.0.0.1:3306/{self.db_name}"
                logger.info(f"Подключение к Cloud SQL через прокси")
            else:
                # Прямое подключение к Cloud SQL
                connection_url = f"mysql+pymysql://{self.db_user}:{self.db_password}@/{self.db_name}?unix_socket=/cloudsql/{self.connection_name}"
                logger.info(f"Прямое подключение к Cloud SQL: {self.connection_name}")
            
            self.engine = create_engine(connection_url, pool_recycle=3600)
            self.conn = self.engine.connect()
            logger.info("Успешное подключение к Google Cloud SQL")
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения к Google Cloud SQL: {e}")
            return False
    
    def initialize_db(self) -> None:
        """
        Создает таблицы в базе данных, если их еще нет.
        """
        try:
            if not self.engine:
                self._connect()
            
            # Создаем таблицу для хранения OHLCV данных
            ohlcv_table_create = """
                CREATE TABLE IF NOT EXISTS ohlcv_data (
                    timestamp BIGINT, 
                    symbol VARCHAR(20), 
                    timeframe VARCHAR(5), 
                    open DECIMAL(20, 8), 
                    high DECIMAL(20, 8), 
                    low DECIMAL(20, 8), 
                    close DECIMAL(20, 8), 
                    volume DECIMAL(30, 8),
                    PRIMARY KEY (timestamp, symbol, timeframe)
                )
            """
            
            # Создаем таблицу метаданных для отслеживания загруженных данных
            metadata_table_create = """
                CREATE TABLE IF NOT EXISTS metadata (
                    symbol VARCHAR(20),
                    timeframe VARCHAR(5),
                    start_date BIGINT,
                    end_date BIGINT,
                    last_update BIGINT,
                    PRIMARY KEY (symbol, timeframe)
                )
            """
            
            # Создаем таблицы
            with self.engine.connect() as connection:
                connection.execute(text(ohlcv_table_create))
                connection.execute(text(metadata_table_create))
                
                # Создаем индексы для ускорения запросов
                connection.execute(text('CREATE INDEX IF NOT EXISTS idx_symbol ON ohlcv_data (symbol)'))
                connection.execute(text('CREATE INDEX IF NOT EXISTS idx_timeframe ON ohlcv_data (timeframe)'))
                connection.execute(text('CREATE INDEX IF NOT EXISTS idx_timestamp ON ohlcv_data (timestamp)'))
                
                connection.commit()
            
            logger.info("База данных Google Cloud SQL инициализирована")
        except Exception as e:
            logger.error(f"Ошибка при инициализации базы данных Cloud SQL: {e}")
    
    def _timestamp_to_ms(self, dt: datetime) -> int:
        """
        Преобразует объект datetime в миллисекунды.
        
        Args:
            dt: Объект datetime
            
        Returns:
            int: Timestamp в миллисекундах
        """
        return int(dt.timestamp() * 1000)
    
    def _ms_to_datetime(self, ms: int) -> datetime:
        """
        Преобразует миллисекунды в объект datetime.
        
        Args:
            ms: Timestamp в миллисекундах
            
        Returns:
            datetime: Объект datetime
        """
        return datetime.fromtimestamp(ms / 1000)
    
    def save_data(self, df: pd.DataFrame, symbol: str, timeframe: str) -> bool:
        """
        Сохраняет данные из DataFrame в базу данных.
        
        Args:
            df: DataFrame с OHLCV данными
            symbol: Торговая пара
            timeframe: Таймфрейм
            
        Returns:
            bool: True, если данные успешно сохранены, иначе False
        """
        try:
            if df.empty:
                logger.warning("Пустой DataFrame, нечего сохранять")
                return False
            
            if not self.engine:
                self._connect()
            
            # Подготавливаем данные для вставки
            # Мы ожидаем, что индексом является timestamp в datetime
            df_copy = df.copy()
            
            # Если индекс не является datetime, преобразуем его
            if not isinstance(df_copy.index, pd.DatetimeIndex):
                logger.info("Индекс не является DatetimeIndex, пытаемся преобразовать")
                df_copy.index = pd.to_datetime(df_copy.index)
            
            # Добавляем колонки symbol и timeframe
            df_copy['symbol'] = symbol
            df_copy['timeframe'] = timeframe
            
            # Преобразуем индекс в timestamp в миллисекундах
            df_copy['timestamp'] = df_copy.index.astype(int) // 10**6  # наносекунды в миллисекунды
            
            # Сбрасываем индекс для подготовки данных к вставке
            df_copy.reset_index(drop=True, inplace=True)
            
            # Выбираем нужные колонки в нужном порядке
            columns = ['timestamp', 'symbol', 'timeframe', 'open', 'high', 'low', 'close', 'volume']
            df_prepared = df_copy[columns]
            
            # Вставляем данные
            with self.engine.begin() as connection:
                # Используем временную таблицу и INSERT IGNORE для игнорирования дубликатов
                df_prepared.to_sql('temp_ohlcv', connection, if_exists='replace', index=False)
                
                # Выполняем вставку с игнорированием дубликатов
                insert_query = """
                INSERT IGNORE INTO ohlcv_data (timestamp, symbol, timeframe, open, high, low, close, volume)
                SELECT timestamp, symbol, timeframe, open, high, low, close, volume FROM temp_ohlcv
                """
                connection.execute(text(insert_query))
            
            # Обновляем метаданные
            min_timestamp = df_copy['timestamp'].min()
            max_timestamp = df_copy['timestamp'].max()
            current_time = int(datetime.now().timestamp() * 1000)
            
            # Проверяем, существуют ли уже метаданные для этого символа и таймфрейма
            with self.engine.connect() as connection:
                result = connection.execute(
                    text('SELECT start_date, end_date FROM metadata WHERE symbol = :symbol AND timeframe = :timeframe'),
                    {"symbol": symbol, "timeframe": timeframe}
                ).fetchone()
                
                if result:
                    # Обновляем существующие метаданные
                    existing_start, existing_end = result
                    start_date = min(existing_start, min_timestamp)
                    end_date = max(existing_end, max_timestamp)
                    
                    connection.execute(
                        text('UPDATE metadata SET start_date = :start_date, end_date = :end_date, last_update = :last_update WHERE symbol = :symbol AND timeframe = :timeframe'),
                        {"start_date": start_date, "end_date": end_date, "last_update": current_time, "symbol": symbol, "timeframe": timeframe}
                    )
                else:
                    # Вставляем новые метаданные
                    connection.execute(
                        text('INSERT INTO metadata (symbol, timeframe, start_date, end_date, last_update) VALUES (:symbol, :timeframe, :start_date, :end_date, :last_update)'),
                        {"symbol": symbol, "timeframe": timeframe, "start_date": min_timestamp, "end_date": max_timestamp, "last_update": current_time}
                    )
                
                connection.commit()
            
            logger.info(f"Данные успешно сохранены для {symbol} на таймфрейме {timeframe}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных в Cloud SQL: {e}")
            return False
    
    def check_data_exists(
        self, 
        symbol: str, 
        timeframe: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> Tuple[bool, Optional[Tuple[datetime, datetime]]]:
        """
        Проверяет наличие данных для указанного символа, таймфрейма и периода.
        
        Args:
            symbol: Торговая пара
            timeframe: Таймфрейм
            start_date: Дата начала периода
            end_date: Дата окончания периода
            
        Returns:
            Tuple[bool, Optional[Tuple[datetime, datetime]]]: 
                - bool: True, если данные существуют, иначе False
                - Optional[Tuple[datetime, datetime]]: Доступный диапазон дат, если данные существуют
        """
        try:
            if not self.engine:
                self._connect()
            
            # Преобразуем даты в миллисекунды
            start_ms = self._timestamp_to_ms(start_date)
            end_ms = self._timestamp_to_ms(end_date)
            
            # Сначала проверяем метаданные для быстрого ответа
            with self.engine.connect() as connection:
                result = connection.execute(
                    text('SELECT start_date, end_date FROM metadata WHERE symbol = :symbol AND timeframe = :timeframe'),
                    {"symbol": symbol, "timeframe": timeframe}
                ).fetchone()
            
            if not result:
                # Нет метаданных, значит нет данных
                return False, None
            
            meta_start, meta_end = result
            
            # Проверяем, покрывают ли имеющиеся данные запрошенный период
            if meta_start <= start_ms and meta_end >= end_ms:
                # Данные полностью покрывают запрошенный период
                return True, (self._ms_to_datetime(meta_start), self._ms_to_datetime(meta_end))
            
            # Проверяем фактическое наличие данных в базе
            with self.engine.connect() as connection:
                result = connection.execute(
                    text('''
                    SELECT MIN(timestamp), MAX(timestamp) 
                    FROM ohlcv_data 
                    WHERE symbol = :symbol AND timeframe = :timeframe AND timestamp >= :start_ms AND timestamp <= :end_ms
                    '''),
                    {"symbol": symbol, "timeframe": timeframe, "start_ms": start_ms, "end_ms": end_ms}
                ).fetchone()
            
            if not result or result[0] is None or result[1] is None:
                # Нет данных для указанного периода
                return False, None
            
            actual_start, actual_end = result
            
            # Проверяем, покрывают ли фактические данные весь запрошенный период
            if actual_start <= start_ms and actual_end >= end_ms:
                # Данные полностью покрывают запрошенный период
                return True, (self._ms_to_datetime(actual_start), self._ms_to_datetime(actual_end))
            
            # Данные частично покрывают период
            return False, (self._ms_to_datetime(meta_start), self._ms_to_datetime(meta_end))
            
        except Exception as e:
            logger.error(f"Ошибка при проверке наличия данных в Cloud SQL: {e}")
            return False, None
    
    def get_data(
        self, 
        symbol: str, 
        timeframe: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Получает данные из базы данных для указанного символа, таймфрейма и периода.
        
        Args:
            symbol: Торговая пара
            timeframe: Таймфрейм
            start_date: Дата начала периода
            end_date: Дата окончания периода
            
        Returns:
            pd.DataFrame: DataFrame с OHLCV данными
        """
        try:
            if not self.engine:
                self._connect()
            
            # Преобразуем даты в миллисекунды
            start_ms = self._timestamp_to_ms(start_date)
            end_ms = self._timestamp_to_ms(end_date)
            
            # Запрашиваем данные
            query = """
                SELECT timestamp, open, high, low, close, volume
                FROM ohlcv_data
                WHERE symbol = :symbol AND timeframe = :timeframe AND timestamp >= :start_ms AND timestamp <= :end_ms
                ORDER BY timestamp
            """
            
            df = pd.read_sql_query(
                sql=text(query),
                con=self.engine,
                params={"symbol": symbol, "timeframe": timeframe, "start_ms": start_ms, "end_ms": end_ms}
            )
            
            if df.empty:
                logger.warning(f"Данные не найдены для {symbol} на таймфрейме {timeframe} в указанном периоде")
                return pd.DataFrame()
            
            # Преобразуем timestamp в datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # Устанавливаем timestamp в качестве индекса
            df.set_index('timestamp', inplace=True)
            
            logger.info(f"Получено {len(df)} свечей для {symbol} на таймфрейме {timeframe}")
            
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при получении данных из Cloud SQL: {e}")
            return pd.DataFrame()
    
    def get_stored_info(self) -> pd.DataFrame:
        """
        Получает информацию о всех сохраненных данных в базе.
        
        Returns:
            pd.DataFrame: DataFrame с информацией о сохраненных данных
        """
        try:
            if not self.engine:
                self._connect()
            
            # Запрашиваем метаданные из БД
            query = """
                SELECT symbol, timeframe, start_date, end_date, last_update
                FROM metadata
                ORDER BY symbol, timeframe
            """
            
            df = pd.read_sql_query(sql=text(query), con=self.engine)
            
            if df.empty:
                logger.warning("В базе данных нет сохраненных данных")
                return pd.DataFrame()
            
            # Преобразуем timestamp в datetime
            timestamp_columns = ['start_date', 'end_date', 'last_update']
            for col in timestamp_columns:
                df[col] = pd.to_datetime(df[col], unit='ms')
            
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при получении информации о сохраненных данных из Cloud SQL: {e}")
            return pd.DataFrame()
    
    def close(self):
        """
        Закрывает соединение с базой данных.
        """
        if self.conn:
            self.conn.close()
            self.conn = None
            self.engine.dispose()
            self.engine = None
            logger.info("Соединение с Google Cloud SQL закрыто")


class DataSynchronizer:
    """
    Класс для синхронизации данных между локальной базой SQLite и облачной базой Google Cloud SQL.
    """
    
    def __init__(self, local_manager, cloud_manager):
        """
        Инициализация синхронизатора данных.
        
        Args:
            local_manager: Экземпляр LocalDataManager для работы с локальной базой
            cloud_manager: Экземпляр CloudDataManager для работы с облачной базой
        """
        self.local_manager = local_manager
        self.cloud_manager = cloud_manager
    
    def sync_local_to_cloud(self) -> Dict[str, Any]:
        """
        Синхронизирует данные из локальной базы в облачную.
        
        Returns:
            Dict[str, Any]: Статистика синхронизации
        """
        stats = {
            "symbols_synced": 0,
            "records_synced": 0,
            "errors": 0
        }
        
        try:
            # Получаем информацию о данных в локальной базе
            local_info = self.local_manager.get_stored_info()
            
            if local_info.empty:
                logger.warning("Нет данных для синхронизации в локальной базе")
                return stats
            
            # Для каждого символа/таймфрейма синхронизируем данные
            for _, row in local_info.iterrows():
                symbol = row['symbol']
                timeframe = row['timeframe']
                start_date = row['start_date']
                end_date = row['end_date']
                
                try:
                    # Проверяем, есть ли эти данные уже в облаке
                    cloud_has_data, cloud_range = self.cloud_manager.check_data_exists(
                        symbol, timeframe, start_date, end_date
                    )
                    
                    # Если данные уже полностью есть в облаке, пропускаем
                    if cloud_has_data:
                        logger.info(f"Данные для {symbol} на таймфрейме {timeframe} уже синхронизированы")
                        continue
                    
                    # Получаем данные из локальной базы
                    df = self.local_manager.get_data(symbol, timeframe, start_date, end_date)
                    
                    if df.empty:
                        logger.warning(f"Пустой DataFrame для {symbol} на таймфрейме {timeframe}")
                        continue
                    
                    # Сохраняем данные в облачную базу
                    success = self.cloud_manager.save_data(df, symbol, timeframe)
                    
                    if success:
                        stats["symbols_synced"] += 1
                        stats["records_synced"] += len(df)
                        logger.info(f"Синхронизировано {len(df)} записей для {symbol} на таймфрейме {timeframe}")
                    else:
                        stats["errors"] += 1
                        logger.error(f"Ошибка при синхронизации данных для {symbol} на таймфрейме {timeframe}")
                
                except Exception as e:
                    stats["errors"] += 1
                    logger.error(f"Ошибка при синхронизации {symbol} {timeframe}: {e}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка при синхронизации данных: {e}")
            stats["errors"] += 1
            return stats
    
    def sync_cloud_to_local(self) -> Dict[str, Any]:
        """
        Синхронизирует данные из облачной базы в локальную.
        
        Returns:
            Dict[str, Any]: Статистика синхронизации
        """
        stats = {
            "symbols_synced": 0,
            "records_synced": 0,
            "errors": 0
        }
        
        try:
            # Получаем информацию о данных в облачной базе
            cloud_info = self.cloud_manager.get_stored_info()
            
            if cloud_info.empty:
                logger.warning("Нет данных для синхронизации в облачной базе")
                return stats
            
            # Для каждого символа/таймфрейма синхронизируем данные
            for _, row in cloud_info.iterrows():
                symbol = row['symbol']
                timeframe = row['timeframe']
                start_date = row['start_date']
                end_date = row['end_date']
                
                try:
                    # Проверяем, есть ли эти данные уже в локальной базе
                    local_has_data, local_range = self.local_manager.check_data_exists(
                        symbol, timeframe, start_date, end_date
                    )
                    
                    # Если данные уже полностью есть локально, пропускаем
                    if local_has_data:
                        logger.info(f"Данные для {symbol} на таймфрейме {timeframe} уже синхронизированы")
                        continue
                    
                    # Получаем данные из облачной базы
                    df = self.cloud_manager.get_data(symbol, timeframe, start_date, end_date)
                    
                    if df.empty:
                        logger.warning(f"Пустой DataFrame для {symbol} на таймфрейме {timeframe}")
                        continue
                    
                    # Сохраняем данные в локальную базу
                    success = self.local_manager.save_data(df, symbol, timeframe)
                    
                    if success:
                        stats["symbols_synced"] += 1
                        stats["records_synced"] += len(df)
                        logger.info(f"Синхронизировано {len(df)} записей для {symbol} на таймфрейме {timeframe}")
                    else:
                        stats["errors"] += 1
                        logger.error(f"Ошибка при синхронизации данных для {symbol} на таймфрейме {timeframe}")
                
                except Exception as e:
                    stats["errors"] += 1
                    logger.error(f"Ошибка при синхронизации {symbol} {timeframe}: {e}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка при синхронизации данных: {e}")
            stats["errors"] += 1
            return stats