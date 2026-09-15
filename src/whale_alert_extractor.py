import requests 
from datetime import datetime, timezone
from bs4 import BeautifulSoup

url = "https://whale-alert.io/whales.html"
response = requests.get(url)
if response.status_code != 200: 
    raise ValueError(f'{response.status_code} Bad request') 
response.encoding = 'utf-8'

soup = BeautifulSoup(response.content, 'html.parser')
table_rows = soup.select('table.table tbody tr')

data = { "datetime_utc": [datetime.now(timezone.utc)] * len(table_rows),
        "crypto": [
                        (row.find("th", {"scope": "row"}).find("img")["alt"].strip()
                        if row.find("th", {"scope": "row"}).find("img")
                        else row.find("th", {"scope": "row"}).get_text(strip=True))
                        for row in table_rows
                    ],
        "known": [row.find_all("td")[0].get_text(strip=True) for row in table_rows],
         "unknown": [row.find_all("td")[1].get_text(strip=True) for row in table_rows]
                }

print(data)