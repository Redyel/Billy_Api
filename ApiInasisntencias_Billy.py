import requests
import pandas as pd
import json
import time
from datetime import datetime
import numpy as np

url = "https://api.q10.com/v1/inasistencias?Fecha_inicio_inasistencia=2023-02-06&Fecha_fin_inasistencia=2024-03-17&Api-Key=d97a2c8cf0b94745a54ff30109c5b90f&limit=9999"
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

with open('records.json', 'w') as f:
    json.dump(records, f)

df = pd.read_json('records.json')

df['Segundo_nombre'] = df['Segundo_nombre'].fillna(' ')
df['Segundo_apellido'] = df['Segundo_apellido'].fillna(' ')


df['Estudiante'] = df['Primer_nombre']+' '+ df['Segundo_nombre'] + ' ' + df['Primer_apellido'] +' '+ df['Segundo_apellido']

df['Estudiante'] = df['Estudiante'].str.replace(r'\s{2,}', ' ', regex=True).str.title()

df = pd.json_normalize(df.to_dict('records'), record_path=['Cursos','Inasistencias'], meta=['Estudiante','Numero_identificacion_estudiante', 'Sexo', 'Correo_electronico_personal', 'Celular', ['Cursos', 'Nombre_docente'], ['Cursos', 'Codigo_modulo'], ['Cursos', 'Nombre_modulo'], ['Cursos', 'Codigo_curso'], ['Cursos', 'Nombre_curso'], ['Cursos', 'Cantidad_inasistencia'],['Cursos', 'Periodo_curso']], errors='ignore')

df[['PrimerA_Cursos.Nombre_docente','PrimerN_Cursos.Nombre_docente']] = df['Cursos.Nombre_docente'].str.split(' ', 2,expand=True)[[0,2]] 

df['PrimerN_Cursos.Nombre_docente'] = df['PrimerN_Cursos.Nombre_docente'].str.split(' ', 2,expand=True)[[0]] 

df['Docente'] = df['PrimerN_Cursos.Nombre_docente'] + ' ' + df['PrimerA_Cursos.Nombre_docente']

df['# Inasistencias'] = 1

periodos = pd.read_excel('Def_Periodos.xlsx', usecols=['Periodo', 'FechaInicio', 'FechaFin', 'PeriodoNombre', 'SemanaClase'], sheet_name=None)

fecha = df['Fecha']
periodo = df['Cursos.Periodo_curso']


def validar_fechas(df1, df2):
    # Crea una nueva columna "comentario" en el df1
    df1["Periodo"] = ""
    # Itera sobre las filas del df1
    for i, row in df1.iterrows():
        fecha = row["Fecha"]
        periodo = row["Cursos.Periodo_curso"]
        if periodo in df2:
            rango_fechas = df2[periodo][(df2[periodo]["FechaInicio"] <= fecha) & (df2[periodo]["FechaFin"] >= fecha)]
            rango_periodos = df2[periodo][(df2[periodo]["Periodo"]==periodo)]
            if not rango_fechas.empty:
                nombre_rango = rango_fechas['PeriodoNombre'].iloc[0]
                df1.at[i, "Periodo"] = nombre_rango
    return df1


PeriodoNombre = validar_fechas(df, periodos)
df = df.assign(PeriodoNombre=PeriodoNombre["Periodo"])

df['Concat_curso'] = df['Cursos.Codigo_curso'] + df ['Cursos.Nombre_curso'] + + df ['Cursos.Periodo_curso']

df_excel = pd.read_excel('Def_Periodos.xlsx', usecols=['Periodo', 'FechaInicio', 'FechaFin', 'PeriodoNombre', 'SemanaClase'], sheet_name='Periodos')

# une los DataFrames por la columna "NomPeriodo"
df = pd.merge(df, df_excel, on='PeriodoNombre', how='left')

df["Tipo de Deserción"] = df.apply(lambda row: "Administrativa" if row["Cursos.Cantidad_inasistencia"] >= (row["SemanaClase"]) else "Académica", axis=1)

criterioelim = df['Cursos.Nombre_curso'].str.contains('Convenio')
df = df.loc[~criterioelim]

# Paso 1: Leer el archivo ListadoCursos.csv y seleccionar las columnas Nombre y Nombre_programa
cursos = pd.read_csv('ListadoCursos.csv', usecols=['Concatenar', 'Nombre_programa'])

# Paso 2: Unir los dataframes df y cursos usando como clave de unión las columnas Cursos.Nombre_curso y Nombre, respectivamente
merged_df = pd.merge(df, cursos, left_on='Concat_curso', right_on='Concatenar')
merged_df = merged_df.drop_duplicates()

# Paso 3: Seleccionar las columnas deseadas del dataframe merged_df y guardarlas en un nuevo dataframe ndf
ndf = merged_df[['Estudiante', 'Numero_identificacion_estudiante', 'Nombre_programa', 'Cursos.Codigo_modulo','Cursos.Nombre_modulo', 'Cursos.Codigo_curso','Cursos.Nombre_curso', 'Cursos.Nombre_docente','Correo_electronico_personal', 'Celular', 'Cursos.Cantidad_inasistencia', 'Fecha','Docente','# Inasistencias', 'PeriodoNombre','Tipo de Deserción','SemanaClase','Cursos.Periodo_curso', 'Concat_curso']]
ndf['Ordenar'] =  ndf['Numero_identificacion_estudiante'].astype(str) + ndf['Fecha'].astype(str) + ndf['Cursos.Nombre_curso']
ndf.sort_values('Ordenar', inplace=True)

# ____________________________________________________________________________________________________

# Ordenar el dataframe por estudiante y fecha
ndf = ndf.sort_values(['Cursos.Nombre_curso', 'Numero_identificacion_estudiante', 'Fecha'])
ndf = ndf.reset_index(drop=True)

ndf['Deserción'] = pd.Series([0]*len(ndf.index))
ndf['Consecutivo'] = pd.Series([0]*len(ndf.index))
ndf['Aplica'] = pd.Series([0]*len(ndf.index))
ndf['AplicaDeserción'] = pd.Series([0]*len(ndf.index))

variable = (range(ndf['Estudiante'].count()-1))

ndf['Fecha'] = pd.to_datetime(ndf['Fecha'])

for x in variable:
    if((ndf.loc[x,'Cursos.Cantidad_inasistencia'] >= 3) and ndf.loc[x+1,'Numero_identificacion_estudiante'] == ndf.loc[x,'Numero_identificacion_estudiante']) and (ndf.loc[x+1,'Cursos.Nombre_modulo'] == ndf.loc[x,'Cursos.Nombre_modulo']) and (ndf.loc[x+1,'Cursos.Nombre_curso'] == ndf.loc[x,'Cursos.Nombre_curso']):
        ndf.loc[x+1,'Deserción'] = (ndf.loc[x+1,'Fecha'])-(ndf.loc[x,'Fecha'])
    else:
        ndf.loc[x+1,'Deserción'] = 0        


for x in variable:
    if((ndf.loc[x,'Deserción'] == 0) and ndf.loc[x+1,'Numero_identificacion_estudiante'] == ndf.loc[x,'Numero_identificacion_estudiante']) and (ndf.loc[x+1,'Cursos.Nombre_modulo'] == ndf.loc[x,'Cursos.Nombre_modulo']) and (ndf.loc[x+1,'Cursos.Nombre_curso'] == ndf.loc[x,'Cursos.Nombre_curso']):
        ndf.loc[x,'Deserción'] = ndf.loc[x+1,'Deserción']

contador = 0
for x in range(len(ndf)-1):
    if (ndf.loc[x,'Numero_identificacion_estudiante'] == ndf.loc[x+1,'Numero_identificacion_estudiante']) and (ndf.loc[x,'Cursos.Nombre_modulo'] == ndf.loc[x+1,'Cursos.Nombre_modulo']) and (ndf.loc[x,'Cursos.Nombre_curso'] == ndf.loc[x+1,'Cursos.Nombre_curso']):
        contador += 1
    else:
        contador = 0
    ndf.loc[x,'Consecutivo'] = contador

for x in range(1, len(ndf)):
    if ndf.loc[x, 'Consecutivo'] == 0 and \
        ndf.loc[x, 'Deserción'] != 0 and \
        ndf.loc[x, 'Numero_identificacion_estudiante'] == ndf.loc[x-1, 'Numero_identificacion_estudiante'] and \
        ndf.loc[x, 'Cursos.Nombre_modulo'] == ndf.loc[x-1, 'Cursos.Nombre_modulo'] and \
        ndf.loc[x, 'Cursos.Nombre_curso'] == ndf.loc[x-1, 'Cursos.Nombre_curso']:
        
        if  ndf.loc[x, 'Deserción'] != 0 and ndf.loc[x-1, 'Consecutivo'] == 0:
            ndf.loc[x, 'Consecutivo'] = 1
        else:
            ndf.loc[x, 'Consecutivo'] = ndf.loc[x-1, 'Consecutivo'] + 1

ndf['Deserción'] = pd.to_timedelta(ndf['Deserción']).dt.days.astype(str)
ndf['Deserción'] = pd.to_numeric(ndf['Deserción'])


condicion = (ndf['Deserción'] != 0) & (ndf['Consecutivo'] == 1)
aplica_grupo = ndf.groupby(['Numero_identificacion_estudiante', 'Cursos.Nombre_modulo', 'Cursos.Nombre_curso'])['Deserción'].transform('mean').where(condicion, ndf['Aplica'])
ndf['Aplica'] = pd.to_numeric(aplica_grupo)

for x in variable:
    if((ndf.loc[x,'Aplica'] == 7.0)):
        ndf.loc[x,'AplicaDeserción'] = 1
    else:
        ndf.loc[x,'AplicaDeserción'] = 0
# ____________________________________________________________________________________________________


# Guardar el resultado en un archivo CSV
ndf.to_csv('Inasistencias.csv', index=False, encoding='UTF-8-sig')