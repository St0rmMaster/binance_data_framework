#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Модуль для подключения к Binance API и получения исторических данных.
"""

import os
import time
import logging
import pandas as pd
from datetime import datetime
from binance.client import Client
from binance.exceptions import BinanceAPIException
from typing import Optional, Dict, Any, List, Tuple

# Настройка логирования
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

class BinanceUSClient:
    """
    Класс для подключения к Binance US API и загрузки исторических данных.
    """
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        """
        Инициализация клиента Binance US API.
        
        Args:
            api_key: API ключ Binance US (опционально)
            api_secret: API секрет Binance US (опционально)
            
        Note:
            Если api_key или api_secret не переданы, метод попытается загрузить их
            из секретов Google Colab при условии, что код выполняется в среде Colab.
            Для этого в Colab должны быть настроены секреты с именами 'binance_api_key'
            и 'binance_api_secret'.
        """
        # Инициализация значений ключей из аргументов
        self._api_key = api_key
        self._api_secret = api_secret
        
        # Попытка загрузить недостающие ключи из Google Colab, если используется эта среда
        self._try_load_from_colab_secrets()
        
        logger.info(f"Инициализирован клиент Binance US API (ключ {'предоставлен' if self._api_key else 'не предоставлен'}, "
                   f"секрет {'предоставлен' if self._api_secret else 'не предоставлен'})")
        
        self.client = None
    
    def _try_load_from_colab_secrets(self):
        """
        Пытается загрузить API ключи из секретов Google Colab, если они не были предоставлены
        и код выполняется в среде Colab.
        """
        # Проверка, выполняется ли код в среде Google Colab
        is_colab = 'COLAB_GPU' in os.environ or 'google.colab' in str(get_ipython())
        
        if not is_colab:
            logger.debug("Код не выполняется в среде Google Colab, пропуск загрузки секретов")
            return
        
        # Если хотя бы один из ключей отсутствует, пытаемся загрузить из секретов Colab
        if self._api_key is None or self._api_secret is None:
            logger.info("Попытка загрузки недостающих ключей из секретов Google Colab")
            
            try:
                # Импортируем модуль userdata из google.colab
                from google.colab import userdata
                
                # Загрузка API ключа, если он не был предоставлен
                if self._api_key is None:
                    try:
                        colab_api_key = userdata.get('binance_api_key')
                        if colab_api_key:
                            self._api_key = colab_api_key
                            logger.info("API ключ успешно загружен из секретов Colab")
                        else:
                            logger.warning("Секрет 'binance_api_key' не найден в Colab")
                    except Exception as e:
                        logger.warning(f"Ошибка при получении API ключа из секретов Colab: {e}")
                
                # Загрузка API секрета, если он не был предоставлен
                if self._api_secret is None:
                    try:
                        colab_api_secret = userdata.get('binance_api_secret')
                        if colab_api_secret:
                            self._api_secret = colab_api_secret
                            logger.info("API секрет успешно загружен из секретов Colab")
                        else:
                            logger.warning("Секрет 'binance_api_secret' не найден в Colab")
                    except Exception as e:
                        logger.warning(f"Ошибка при получении API секрета из секретов Colab: {e}")
                        
            except ImportError:
                logger.warning("Не удалось импортировать google.colab.userdata. "
                             "Возможно, код выполняется в другой среде или требуется обновление Colab.")
            except Exception as e:
                logger.warning(f"Непредвиденная ошибка при загрузке секретов из Colab: {e}")
    
    @property
    def api_key(self):
        """Свойство для доступа к API ключу."""
        return self._api_key
    
    @property
    def api_secret(self):
        """Свойство для доступа к API секрету."""
        return self._api_secret
    
    def connect(self) -> bool:
        """
        Устанавливает соединение с Binance US API.
        
        Returns:
            bool: True, если соединение успешно, иначе False
        """
        try:
            self.client = Client(api_key=self._api_key, api_secret=self._api_secret, tld='us')
            # Проверка соединения
            self.client.ping()
            return True
        except BinanceAPIException as e:
            logger.error(f"Ошибка подключения к Binance US API: {e}")
            return False
        except Exception as e:
            logger.error(f"Непредвиденная ошибка при подключении: {e}")
            return False
    
    def get_client(self) -> Optional[Client]:
        """
        Возвращает инициализированный объект клиента.
        
        Returns:
            Client: Объект клиента Binance, если соединение установлено, иначе None
        """
        if not self.client:
            self.connect()
        return self.client
    
    def get_exchange_info(self) -> Dict[str, Any]:
        """
        Получает информацию о бирже для списка инструментов и таймфреймов.
        
        Returns:
            Dict: Информация о бирже
        """
        try:
            client = self.get_client()
            if not client:
                return {}
            
            exchange_info = client.get_exchange_info()
            return exchange_info
        except BinanceAPIException as e:
            logger.error(f"Ошибка получения информации о бирже: {e}")
            return {}
        except Exception as e:
            logger.error(f"Непредвиденная ошибка: {e}")
            return {}
    
    def get_usdt_trading_pairs(self) -> List[str]:
        """
        Получает список торговых пар с USDT.
        
        Returns:
            List[str]: Список символов с USDT в конце
        """
        try:
            exchange_info = self.get_exchange_info()
            if not exchange_info or 'symbols' not in exchange_info:
                return []
            
            usdt_pairs = [
                symbol['symbol'] for symbol in exchange_info['symbols']
                if symbol['symbol'].endswith('USDT') and symbol['status'] == 'TRADING'
            ]
            return sorted(usdt_pairs)
        except Exception as e:
            logger.error(f"Ошибка при получении USDT пар: {e}")
            return []
    
    def get_available_intervals(self) -> List[str]:
        """
        Возвращает список доступных интервалов (таймфреймов).
        
        Returns:
            List[str]: Список доступных таймфреймов
        """
        return ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
    
    def _convert_timestamp_to_datetime(self, timestamp: int) -> datetime:
        """
        Конвертирует timestamp в объект datetime.
        
        Args:
            timestamp: UNIX timestamp в миллисекундах
            
        Returns:
            datetime: Объект datetime
        """
        return datetime.fromtimestamp(timestamp / 1000)
    
    def get_historical_data(
        self, 
        symbol: str, 
        interval: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Получает исторические данные для указанного символа и интервала в заданном периоде.
        
        Args:
            symbol: Торговая пара (например, 'BTCUSDT')
            interval: Таймфрейм (например, '1h')
            start_date: Дата начала периода
            end_date: Дата окончания периода
            
        Returns:
            pd.DataFrame: DataFrame с историческими данными OHLCV
        """
        try:
            client = self.get_client()
            if not client:
                return pd.DataFrame()
            
            # Конвертация datetime в строковые метки времени
            start_str = int(start_date.timestamp() * 1000)
            end_str = int(end_date.timestamp() * 1000)
            
            logger.info(f"Загрузка данных для {symbol} на таймфрейме {interval} с {start_date} по {end_date}...")
            
            # Инициализация пустого списка для хранения всех свечей
            all_klines = []
            
            # Максимальное количество свечей, которое можно получить за один запрос
            max_limit = 1000
            
            # Текущее начало периода
            current_start = start_str
            
            # Загрузка данных с пагинацией
            while current_start < end_str:
                try:
                    # Получаем порцию данных
                    klines = client.get_historical_klines(
                        symbol=symbol,
                        interval=interval,
                        start_str=current_start,
                        end_str=end_str,
                        limit=max_limit
                    )
                    
                    if not klines:
                        # Если нет данных, выходим из цикла
                        break
                    
                    # Добавляем полученные свечи в общий список
                    all_klines.extend(klines)
                    
                    # Обновляем начало периода для следующего запроса
                    # Используем временную метку последней свечи + 1 мс
                    current_start = klines[-1][0] + 1
                    
                    # Делаем паузу, чтобы не превысить лимиты API
                    time.sleep(0.1)
                except BinanceAPIException as e:
                    if 'Too much request weight used' in str(e):
                        logger.warning(f"Превышен лимит запросов, ожидание 60 секунд...")
                        time.sleep(60)
                        continue
                    else:
                        logger.error(f"Ошибка API Binance при загрузке данных: {e}")
                        break
                except Exception as e:
                    logger.error(f"Непредвиденная ошибка при загрузке данных: {e}")
                    break
            
            if not all_klines:
                logger.info(f"Данные не найдены для {symbol} на таймфрейме {interval} в указанном периоде")
                return pd.DataFrame()
            
            # Преобразование данных в pandas DataFrame
            columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 
                       'close_time', 'quote_asset_volume', 'number_of_trades', 
                       'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
            
            df = pd.DataFrame(all_klines, columns=columns)
            
            # Преобразование типов данных
            numeric_columns = ['open', 'high', 'low', 'close', 'volume', 
                               'quote_asset_volume', 'taker_buy_base_asset_volume', 
                               'taker_buy_quote_asset_volume']
            
            df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric)
            
            # Преобразование timestamp в datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
            
            # Установка timestamp в качестве индекса
            df.set_index('timestamp', inplace=True)
            
            # Оставляем только нужные колонки: OHLCV
            df = df[['open', 'high', 'low', 'close', 'volume']]
            
            logger.info(f"Загружено {len(df)} свечей для {symbol} на таймфрейме {interval}")
            
            return df
            
        except BinanceAPIException as e:
            logger.error(f"Ошибка API Binance: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Непредвиденная ошибка: {e}")
            return pd.DataFrame()