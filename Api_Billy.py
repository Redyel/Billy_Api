import requests
import pandas as pd
import json

url = "https://api.q10.com/v1/inasistencias?Fecha_inicio_inasistencia=2022-11-14&Fecha_fin_inasistencia=2022-12-14&Api-Key=d97a2c8cf0b94745a54ff30109c5b90f&limit=9999"
headers = {
  'Api-Key': 'd97a2c8cf0b94745a54ff30109c5b90f',
  'Cookie': '.ASPXANONYMOUS=xr0rL3sQ33_iv-iu9D8b_5xvBiVQVz_-GgQo2bMzJVbBV27mzvXTqsk6kMqd6AAJl27DGieeQAItB_kcQdOAjN0s0x3mONjkQQUSIuM6OUOVf41Tr68PzygrHtmSXbPAeoFRXw2'
}

records = []
total_records = 0

while url:
    response = requests.get(url, headers=headers)
    data = json.loads(response.text)
    records.extend(data)
    total_records += len(data)
    url = response.links.get('next', {}).get('url')

df = pd.DataFrame(records)
df.to_csv('validacion.csv', index=False)

print(df)
print(f'Total de registros: {total_records}')