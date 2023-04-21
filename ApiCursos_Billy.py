import requests
import pandas as pd
import json
import re
import time

url = "https://api.q10.com/v1/cursos?&Api-Key=d97a2c8cf0b94745a54ff30109c5b90f&limit=9999999"
headers = {
  'Api-Key': 'd97a2c8cf0b94745a54ff30109c5b90f',
  'Cookie': '.ASPXANONYMOUS=xr0rL3sQ33_iv-iu9D8b_5xvBiVQVz_-GgQo2bMzJVbBV27mzvXTqsk6kMqd6AAJl27DGieeQAItB_kcQdOAjN0s0x3mONjkQQUSIuM6OUOVf41Tr68PzygrHtmSXbPAeoFRXw2'
}

def get_data_from_api(url):
    try:
        response = requests.get(url)
        response.raise_for_status() # raise an error if there is an HTTP error status
        data = response.json()
        return data
    except (requests.exceptions.RequestException, ValueError) as e:
        print(f"An error occurred: {e}")
        print("Waiting 5 seconds and retrying...")
        time.sleep(5) # wait 5 seconds before retrying
        return get_data_from_api(url) # retry the request



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

# Lista de códigos a buscar
codigos = ['EQ', 'EH', 'EI', 'EZ', 'J3', 'J4', 'J5', 'N1', 'N2', 'M1', 'M2', 'K1', 'K2', 'VE', 'VP', 'HBPM', 'DMTX', 'VR-I1', 'VR-I2', 'VR-I3', 'VR-I4', 'VR-I5', 'VR-I6', 'VR-I7', 'VR-TIC', 'VR-WCB', 'AIVG', 'BVG', 'CVG', 'DVG', 'DMVG', 'EVG', 'FVG', 'HVG', 'JVG', 'KVG', 'LVG', 'MVG', 'MEVG', 'NVG', 'OVG', 'PVG', 'RVG', 'RHVG', 'SVG', 'SLVG', 'UVG', 'VMVG', 'XVG', 'NPSVG', 'ZY', 'WN08']

# Patrón de búsqueda
patron = r'^(' + '|'.join(codigos) + r')\w*'

# Filtrar y eliminar filas
df = df[~df['Codigo'].str.contains(patron, flags=re.IGNORECASE, regex=True)]


criterio = df.applymap(lambda x: 'EDUCACIÓN CONTINUA - Única' in str(x)).any(axis=1)
df = df[~criterio]

criterioelim = df['Codigo'].str.contains('Convenio')
df = df.loc[~criterioelim]



df = df.fillna('-')

criterio = df['Nombre_docente'].str.contains('-')
eliminar = df[criterio].index
df = df.drop(eliminar)

df['Concatenar'] = df['Codigo'] + df['Nombre'] + df['Nombre_periodo']

ndf = df.loc[:,['Nombre_sede_jornada','Nombre_programa','Codigo','Nombre','Nombre_docente','Fecha_inicio','Fecha_fin','Cupo_maximo','Cantidad_estudiantes_matriculados','Nombre_periodo','Concatenar']]

ndf.to_csv('ListadoCursos.csv', index=False, encoding='UTF-8-sig')
