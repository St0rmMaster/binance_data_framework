#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Модуль для подключения к Binance API и получения исторических данных.
"""

import time
import pandas as pd
from datetime import datetime
from binance.client import Client
from binance.exceptions import BinanceAPIException
from typing import Optional, Dict, Any, List, Tuple

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
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.client = None
    
    def connect(self) -> bool:
        """
        Устанавливает соединение с Binance US API.
        
        Returns:
            bool: True, если соединение успешно, иначе False
        """
        try:
            self.client = Client(api_key=self.api_key, api_secret=self.api_secret, tld='us')
            # Проверка соединения
            self.client.ping()
            return True
        except BinanceAPIException as e:
            print(f"Ошибка подключения к Binance US API: {e}")
            return False
        except Exception as e:
            print(f"Непредвиденная ошибка при подключении: {e}")
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
            print(f"Ошибка получения информации о бирже: {e}")
            return {}
        except Exception as e:
            print(f"Непредвиденная ошибка: {e}")
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
            print(f"Ошибка при получении USDT пар: {e}")
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
            
            print(f"Загрузка данных для {symbol} на таймфрейме {interval} с {start_date} по {end_date}...")
            
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
                        print(f"Превышен лимит запросов, ожидание 60 секунд...")
                        time.sleep(60)
                        continue
                    else:
                        print(f"Ошибка API Binance при загрузке данных: {e}")
                        break
                except Exception as e:
                    print(f"Непредвиденная ошибка при загрузке данных: {e}")
                    break
            
            if not all_klines:
                print(f"Данные не найдены для {symbol} на таймфрейме {interval} в указанном периоде")
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
            
            print(f"Загружено {len(df)} свечей для {symbol} на таймфрейме {interval}")
            
            return df
            
        except BinanceAPIException as e:
            print(f"Ошибка API Binance: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"Непредвиденная ошибка: {e}")
            return pd.DataFrame() 