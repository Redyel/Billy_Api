import requests
import pandas as pd
import json

url = "https://api.q10.com/v1/inasistencias?Fecha_inicio_inasistencia=2023-04-01&Fecha_fin_inasistencia=2023-04-11&Api-Key=d97a2c8cf0b94745a54ff30109c5b90f&limit=9999"
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

with open('records.json', 'w') as f:
    json.dump(records, f)

df = pd.read_json('records.json')

df = pd.json_normalize(df.to_dict('records'), record_path=['Cursos','Inasistencias'], meta=['Primer_nombre', 'Segundo_nombre', 'Primer_apellido', 'Segundo_apellido', 'Numero_identificacion_estudiante', 'Sexo', 'Correo_electronico_personal', 'Celular', ['Cursos', 'Nombre_modulo'], ['Cursos', 'Nombre_docente'], ['Cursos', 'Cantidad_inasistencia']], errors='ignore')

# df = df.drop(['Observaciones'], axis=1)


df.to_csv('Inasistencias.csv', index=False, encoding='UTF-8-sig')
print(df)
