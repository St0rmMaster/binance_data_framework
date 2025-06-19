# Гайд программиста: загрузка тиковых данных Dukascopy
*Актуально на 19 June 2025*

---

## Содержание
1. [Быстрый старт](#быстрый-старт)
2. [URL‑схема Dukascopy Datafeed](#url‑схема-dukascopy-datafeed)
3. [Формат `.bi5`](#формат-bi5)
4. [Инструменты для скачивания](#инструменты-для-скачивания)
5. [Обработка тиков](#обработка-тиков)
6. [Список доступных рынков](#список-доступных-рынков)
7. [Лайфхаки и подводные камни](#лайфхаки-и-подводные-камни)
8. [Полезные ссылки](#полезные-ссылки)

---

## Быстрый старт

```bash
# Устанавливаем Python‑клиент
pip install dukascopy-python

# Выкачиваем тики EURUSD за 2024 год и сохраняем в CSV
python - <<'PY'
from dukascopy import Downloader
dl = Downloader()
df = dl.download('eurusd', '2024-01-01', '2024-12-31', timeframe='tick')
df.to_csv('EURUSD_2024_tick.csv', index=False)
PY
```

---

## URL‑схема Dukascopy Datafeed

```
https://datafeed.dukascopy.com/datafeed/{SYMBOL}/{YYYY}/{MM}/{DD}/{HH}h_ticks.bi5
```

* **SYMBOL** — тикер (например `EURUSD`, `XAUUSD`, `USA500.IDX/USD`).
* **Глубина**: форекс с 2003 г.; акции CFD ≈ 2018 г.; крипто — май 2017 г.
* Файлы в UTC, содержат Bid, Ask, объём.

---

## Формат `.bi5`

| Байты | Тип | Поле |
|-------|-----|------|
| 0‑3   | `uint32` | миллисекунды с начала часа |
| 4‑7   | `float32` | Ask‑price |
| 8‑11  | `float32` | Bid‑price |
| 12‑15 | `float32` | Ask‑volume |
| 16‑19 | `float32` | Bid‑volume |

Пример декодирования:

```python
import lzma, struct, pandas as pd, datetime as dt, requests

url = "https://datafeed.dukascopy.com/datafeed/EURUSD/2024/01/02/00h_ticks.bi5"
raw = lzma.decompress(requests.get(url).content)
rows = [struct.unpack('>Iffff', raw[i:i+20]) for i in range(0, len(raw), 20)]
epoch = dt.datetime(2024,1,2,0,0,0, tzinfo=dt.timezone.utc)
df = pd.DataFrame(rows, columns=['ms','ask','bid','askvol','bidvol'])
df['timestamp'] = epoch + pd.to_timedelta(df.pop('ms'), unit='ms')
```

---

## Инструменты для скачивания

### 1. dukascopy-python

```bash
pip install dukascopy-python
```

```python
from dukascopy import Downloader
dl = Downloader(threads=8, buffer_size=256)
df = dl.download('btcusd', '2017-05-15', '2025-06-19', timeframe='m1')
```

**Плюсы:** авто‑декод, DataFrame; **Минусы:** зависимость pandas.

### 2. dukascopy-node (CLI)

```bash
npx dukascopy-node --symbol eurusd --from 2024-01-01 --to 2024-12-31                    --timeframe tick --format parquet --out ./data
```

### 3. wget/cURL

```bash
for h in {00..23}; do
  url="https://datafeed.dukascopy.com/datafeed/EURUSD/2024/01/02/${h}h_ticks.bi5"
  wget -q -P ./raw "$url"
done
```

### 4. JForex SDK (Java)

```java
IHistory hist = context.getHistory();
List<ITick> ticks = hist.getTicks("EUR/USD",
        Date.from(Instant.parse("2024-01-01T00:00:00Z")),
        Date.from(Instant.parse("2024-01-02T00:00:00Z")));
```

### 5. Web‑виджет «Historical Data Export»

<https://www.dukascopy.com/trading-tools/widgets/quotes/historical_data_feed>

---

## Обработка тиков

```python
# df — DataFrame тиков
agg_cfg = {'bid':['first','max','min','last'],
            'ask':['first','max','min','last'],
            'askvol':'sum','bidvol':'sum'}
bars = (df.set_index('timestamp')
          .resample('1min', label='left', closed='left')
          .agg(agg_cfg))
bars.columns = ['_'.join(c) for c in bars.columns]
bars.to_parquet('EURUSD_2024_M1.parquet')
```

---

## Список доступных рынков

| Категория | Примеры тикеров | Глубина |
|-----------|-----------------|---------|
| Forex | `EURUSD`, `USDJPY`, `USDCNH` | 2003‑н.в. |
| Металлы | `XAUUSD`, `XAGUSD` | 2008‑н.в. |
| Энергия | `BRENT.CMD/USD`, `WTI.CMD/USD` | 2013‑н.в. |
| Commodities | `COFFEE.CMD/USD`, `CORN.CMD/USD` | 2014‑н.в. |
| Индексы | `USA500.IDX/USD`, `DEU.IDX/EUR` | 2010‑н.в. |
| Облигации | `US10Y.BND/USD` | 2012‑н.в. |
| Крипто | `BTCUSD`, `ETHUSD` | 2017‑н.в. |
| Акции CFD | `AAPL.US`, `BMW.DE` | 2018‑н.в. |
| ETF CFD | `SPY.US`, `QQQ.US` | 2023‑н.в. |

---

## Лайфхаки и подводные камни

* Не превышайте 8 параллельных потоков — иначе HTTP 403.  
* Уик‑энды → пустые файлы.  
* Время всегда UTC; учитывайте DST при переводе.  
* Для акций нет сплит‑коррекции.  
* Объём — «tick count», а не реальный объём клиринга.

---

## Полезные ссылки

* Range of Markets: <https://www.dukascopy.com/swiss/english/cfd/range-of-markets/>  
* Widget Export: <https://www.dukascopy.com/trading-tools/widgets/quotes/historical_data_feed>  
* dukascopy-python: <https://pypi.org/project/dukascopy-python/>  
* dukascopy-node: <https://github.com/Artygor/dukascopy-node>  
* bi5 format spec: <https://github.com/mayeranalytics/bi5>  

---

> **Лицензия данных:** архив предназначен для некоммерческих исследований. Для коммерческого использования требуется согласование с Dukascopy Bank SA.
