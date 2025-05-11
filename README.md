# Binance US Data Framework

**Binance US Data Framework** — это инструмент для удобной загрузки, хранения, анализа и экспорта исторических данных с Binance US. Фреймворк оптимизирован для работы в Google Colab, поддерживает кэширование в Google Drive, визуализацию, экспорт, ресемплирование и полностью интерактивный UI.

---

## Возможности

- **Загрузка исторических OHLCV-данных** с Binance US API по выбранным символам и таймфреймам.
- **Локальное хранение** данных в SQLite-базе на Google Drive (или локально).
- **Интерактивный UI** для Google Colab: выбор символов, таймфреймов, дат, фильтрация, массовая загрузка.
- **Ресемплирование**: агрегация данных из меньших таймфреймов в большие.
- **Экспорт** выбранных данных в CSV и Parquet.
- **Удаление** данных из базы через UI.
- **Визуализация**: быстрый просмотр графика цены и объема.
- **Кэширование**: повторное использование уже загруженных данных.
- **Программный доступ**: получение данных напрямую из Python-кода.
- **Безопасная работа с API-ключами** в Colab.
- **Автоматическое определение и отображение доступных данных в базе**.
- **Загрузка выбранного набора данных как текущего DataFrame для анализа**.

---

## Установка

### В Google Colab

```python
!pip install --no-cache-dir --upgrade git+https://github.com/St0rmMaster/binance_data_framework.git
```

### Локальная установка

```bash
pip install --upgrade git+https://github.com/St0rmMaster/binance_data_framework.git
```

### Для разработки

```bash
git clone https://github.com/St0rmMaster/binance_data_framework.git
cd binance_data_framework
pip install -e .
```

---

## Быстрый старт: Google Colab UI

```python
from binance_data_framework import launch_ui

# Запуск интерактивного интерфейса (Colab)
ui = launch_ui()
```

**Возможности UI:**
- Выбор таймфрейма, фильтрация и массовый выбор символов.
- Задание периода загрузки.
- Ресемплирование и визуализация.
- Просмотр и экспорт уже загруженных данных.
- Удаление выбранных данных.
- Загрузка выбранного набора как переменной `selected_df` с предпросмотром (head/tail).

---

## Пример работы с UI

```python
# После загрузки данных через UI:
# Получить доступ к последним загруженным DataFrame:
df_dict = ui.last_loaded_data_params['dataframes']
first_symbol = list(df_dict.keys())[0]
my_dataframe = df_dict[first_symbol]
```

---

## Пример программного использования (без UI)

```python
from binance_data_framework import BinanceUSClient, GoogleDriveDataManager
from datetime import datetime, timedelta

api_client = BinanceUSClient()
db_manager = GoogleDriveDataManager()

symbol = 'BTCUSDT'
timeframe = '1h'
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

# Проверка наличия данных в базе
data_exists, date_range = db_manager.check_data_exists(symbol, timeframe, start_date, end_date)

if data_exists:
    df = db_manager.get_data(symbol, timeframe, start_date, end_date)
else:
    df = api_client.get_historical_data(symbol, timeframe, start_date, end_date)
    if not df.empty:
        db_manager.save_data(df, symbol, timeframe)

# Анализ и визуализация
if not df.empty:
    print(df.head())
    df['SMA_20'] = df['close'].rolling(window=20).mean()
    import matplotlib.pyplot as plt
    plt.figure(figsize=(12, 6))
    plt.plot(df.index, df['close'], label='Close')
    plt.plot(df.index, df['SMA_20'], label='SMA 20')
    plt.title(f'{symbol} - {timeframe}')
    plt.legend()
    plt.grid(True)
    plt.show()
```

---

## Безопасное хранение API-ключей в Google Colab

```python
from google.colab import userdata

api_key = userdata.get('BINANCE_US_API_KEY')
api_secret = userdata.get('BINANCE_US_API_SECRET')
api_client = BinanceUSClient(api_key=api_key, api_secret=api_secret)
```

---

## Архитектура и основные классы

- **BinanceUSClient** — работа с Binance US API, загрузка исторических данных.
- **GoogleDriveDataManager** — хранение и управление данными в SQLite на Google Drive.
- **DataDownloaderUI** — интерактивный интерфейс для Colab (выбор, загрузка, экспорт, удаление, визуализация).
- **launch_ui()** — быстрый запуск UI в Colab.

---

## Вызовы и методы

### BinanceUSClient

- `get_usdt_trading_pairs()`
- `get_available_intervals()`
- `get_historical_data(symbol, timeframe, start_date, end_date)`

### GoogleDriveDataManager

- `check_data_exists(symbol, timeframe, start_date, end_date)`
- `get_data(symbol, timeframe, start_date, end_date)`
- `save_data(df, symbol, timeframe)`
- `delete_data(symbol, timeframe)`
- `get_stored_info()`

### DataDownloaderUI

- `display()` — отобразить интерфейс (Colab)
- `last_loaded_data_params` — доступ к последним загруженным данным

---

## Экспорт и удаление данных

- Экспорт выбранных данных в CSV/Parquet через UI (правый блок).
- Удаление выбранных данных из базы через UI.

---

## Лицензия

MIT License

---

**Binance US Data Framework** — ваш быстрый путь к анализу и автоматизации работы с историческими данными Binance US в Google Colab и Python!