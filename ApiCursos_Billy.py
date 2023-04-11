import requests
import pandas as pd
import json

url = "https://api.q10.com/v1/cursos?&Api-Key=d97a2c8cf0b94745a54ff30109c5b90f&limit=9999"
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

df['Fecha_inicio'] = pd.to_datetime(df['Fecha_inicio']).dt.strftime('%d-%m-%Y')

df['Fecha_fin'] = pd.to_datetime(df['Fecha_fin']).dt.strftime('%d-%m-%Y')


criterio = df['Codigo'].str.contains('ELIMINADO')
eliminar = df[criterio].index
df = df.drop(eliminar)

criterio_u = df['Cantidad_estudiantes_matriculados'] < 1
eliminar_u = df[criterio_u].index
df = df.drop(eliminar_u)


criterio = df.applymap(lambda x: 'EDUCACIÓN CONTINUA - Única' in str(x)).any(axis=1)
df = df[~criterio]

df = df.fillna('-')

criterio = df['Nombre_docente'].str.contains('-')
eliminar = df[criterio].index
df = df.drop(eliminar)

df['Curso exp'] = df['Codigo'] + '-'+ df['Nombre']

ndf = df.loc[:,['Nombre_sede_jornada','Nombre_programa','Codigo','Nombre','Nombre_docente','Fecha_inicio','Fecha_fin','Cupo_maximo','Cantidad_estudiantes_matriculados','Curso exp','Nombre_periodo']]

ndf.to_csv('ListadoCursos.csv', index=False, encoding='UTF-8-sig')

print(df)
print(f'Total de registros: {total_records}')