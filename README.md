# Binance US Data Framework

Фреймворк для загрузки исторических данных с Binance US, их хранения в локальной базе данных и эффективного повторного использования в Google Colab.

## Особенности

- Загрузка исторических OHLCV данных с Binance US API
- Локальное хранение данных в SQLite базе данных
- Интерактивный интерфейс для выбора торговых пар, таймфреймов и временных периодов
- Поддержка ресемплирования данных (агрегация из мелких таймфреймов в крупные)
- Кэширование данных для быстрого доступа и экономии запросов к API
- Визуализация загруженных данных

## Установка

### В Google Colab

```python
!pip install git+https://github.com/St0rmMaster/binance_data_framework.git
```

Или клонировать репозиторий и установить в режиме разработки:

```python
!git clone https://github.com/St0rmMaster/binance_data_framework.git
%cd binance_data_framework
!pip install -e .
```

### Локальная установка

```bash
pip install git+https://github.com/St0rmMaster/binance_data_framework.git
```

## Использование

### Базовый пример

```python
import pandas as pd
from datetime import datetime, timedelta
from binance_data_framework import BinanceUSClient, LocalDataManager, DataDownloaderUI

# Создаем экземпляр клиента Binance US API (можно передать ключи API)
api_client = BinanceUSClient(api_key=None, api_secret=None)
api_client.connect()

# Создаем экземпляр менеджера базы данных
db_manager = LocalDataManager(db_path='binance_data.db')

# Создаем и отображаем пользовательский интерфейс
ui = DataDownloaderUI(api_client, db_manager)
ui.display()
```

### Безопасное хранение API ключей в Colab

```python
from google.colab import userdata

api_key = userdata.get('BINANCE_US_API_KEY')
api_secret = userdata.get('BINANCE_US_API_SECRET')

api_client = BinanceUSClient(api_key=api_key, api_secret=api_secret)
```

### Программное получение данных (без UI)

```python
import pandas as pd
from datetime import datetime, timedelta
from binance_data_framework import BinanceUSClient, LocalDataManager

# Создаем экземпляр клиента Binance US API
api_client = BinanceUSClient()
api_client.connect()

# Создаем экземпляр менеджера базы данных
db_manager = LocalDataManager(db_path='binance_data.db')

# Параметры запроса
symbol = 'BTCUSDT'
timeframe = '1h'
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

# Проверяем наличие данных в базе
data_exists, date_range = db_manager.check_data_exists(symbol, timeframe, start_date, end_date)

if data_exists:
    # Данные есть в базе, загружаем их
    df = db_manager.get_data(symbol, timeframe, start_date, end_date)
else:
    # Данных нет, загружаем из API и сохраняем в базу
    df = api_client.get_historical_data(symbol, timeframe, start_date, end_date)
    if not df.empty:
        db_manager.save_data(df, symbol, timeframe)

# Работаем с данными
if not df.empty:
    print(df.head())
    
    # Пример расчета простого скользящего среднего
    df['SMA_20'] = df['close'].rolling(window=20).mean()
    
    # Пример визуализации
    import matplotlib.pyplot as plt
    plt.figure(figsize=(12, 6))
    plt.plot(df.index, df['close'], label='Close')
    plt.plot(df.index, df['SMA_20'], label='SMA 20')
    plt.title(f'{symbol} - {timeframe}')
    plt.legend()
    plt.grid(True)
    plt.show()
```

## Архитектура

Фреймворк состоит из трех основных модулей:

1. **api_connector.py**: Модуль для подключения к Binance US API и загрузки исторических данных.
2. **database_handler.py**: Модуль для работы с локальной базой данных SQLite.
3. **colab_interface.py**: Модуль для создания интерактивного пользовательского интерфейса в Google Colab.

## Лицензия

MIT License