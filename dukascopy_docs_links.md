# Dukascopy documentation & assets links

Ниже приведён список основных документов, архивов и wiki‑страниц Dukascopy, которые можно скачать или читать онлайн.

- **JForex SDK download (zip)** – <https://www.dukascopy.com/client/jforexlib/JForex-3-SDK.zip>
- **JForex SDK introduction wiki** – <https://www.dukascopy.com/wiki/en/development/get-started-api/use-jforex-sdk/download-jforex-sdk/>
- **JForex API Javadoc (root)** – <https://www.dukascopy.com/swiss/docs/api/>
- **JForex API Javadoc (overview)** – <https://www.dukascopy.com/client/javadoc/overview-summary.html>
- **Feed history wiki** – <https://www.dukascopy.com/wiki/en/development/strategy-api/historical-data/feed-history/>
- **Historical Data Feed documentation** – <https://www.dukascopy.com/swiss/english/marketwatch/historical/>
- **Historical Data Export widget** – <https://www.dukascopy.com/trading-tools/widgets/quotes/historical_data_feed>
- **Quotes Widgets API documentation** – <https://www.dukascopy.com/trading-tools/api/documentation/quotes>
- **Market Range (CFD/FX)** – <https://www.dukascopy.com/swiss/english/cfd/range-of-markets/>
- **Market Quotes Widgets catalog** – <https://www.dukascopy.com/trading-tools/widgets/quotes>
- **bi5 format parser GitHub** – <https://github.com/mayeranalytics/bi5>
- **dukascopy-python on PyPI** – <https://pypi.org/project/dukascopy-python/>
- **dukascopy-node on GitHub** – <https://github.com/Artygor/dukascopy-node>

---
## Скрипт автоматической загрузки

```bash
#!/usr/bin/env bash
set -e
OUT="dukascopy_docs"
mkdir -p "$OUT"

# Скачать архив SDK
wget -c -P "$OUT" "https://www.dukascopy.com/client/jforexlib/JForex-3-SDK.zip"

# Скачать Javadoc целиком (пример):
wget -r -np -nH -P "$OUT/javadoc" --cut-dirs=2 -e robots=off \
     --accept "*.html,*.css,*.js,*.png" \
     https://www.dukascopy.com/swiss/docs/api/

# Сохранить wiki страницы как HTML
for url in \
    "https://www.dukascopy.com/wiki/en/development/get-started-api/use-jforex-sdk/download-jforex-sdk/" \
    "https://www.dukascopy.com/wiki/en/development/strategy-api/historical-data/feed-history/"; do
    wget -E -H -k -p -P "$OUT/wiki" "$url"
done
```