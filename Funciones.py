import requests
import json
import time
import pandas as pd
from datetime import timedelta
import re
from datetime import datetime

# ________________________ ________________________ ________________ Funciones para Listado de Cursos ________________________ ________________________ ________________

# Función para obtener la información de los cursos de Q10, conexión mediante la API junto con su API-Key
def obtener_Api(url, headers):
    records = []
    total_records = 0
    
    while url:
        response = requests.get(url, headers=headers)
        data = json.loads(response.text)
        records.extend(data)
        total_records += len(data)
        url = response.links.get('next', {}).get('url')
        
    return records, total_records

# Función para asegurar la conexión a la API
def Conexion_OK(url):
    try:
        response = requests.get(url)
        response.raise_for_status() # raise an error if there is an HTTP error status
        data = response.json()
        return data
    except (requests.exceptions.RequestException, ValueError) as e:
        print(f"An error occurred: {e}")
        print("Waiting 5 seconds and retrying...")
        time.sleep(5) # wait 5 seconds before retrying
        return Conexion_OK(url) # retry the request

# Función para limpiar el DF que se genere de la API de Cursos
def limpiar_DFCursos(df):
    
    df = df[~df['Codigo'].str.contains('ELIMINADO')] # Eliminar filas que contengan 'ELIMINADO' en la columna 'Codigo'
    df = df[df['Cantidad_estudiantes_matriculados'] >= 1] # Eliminar filas con 'Cantidad_estudiantes_matriculados' menor a 1
    codigos = ['EQ', 'EH', 'EI', 'EZ', 'J3', 'J4', 'J5', 'N1', 'N2', 'M1', 'M2', 'K1', 'K2', 'VE', 'VP', 'HBPM', 'DMTX', 'VR-I1', 'VR-I2', 'VR-I3', 'VR-I4', 'VR-I5', 'VR-I6', 'VR-I7', 'VR-TIC', 'VR-WCB', 'AIVG', 'BVG', 'CVG', 'DVG', 'DMVG', 'EVG', 'FVG', 'HVG', 'JVG', 'KVG', 'LVG', 'MVG', 'MEVG', 'NVG', 'OVG', 'PVG', 'RVG', 'RHVG', 'SVG', 'SLVG', 'UVG', 'VMVG', 'XVG', 'NPSVG', 'ZY', 'WN08', 'VK', ] # Lista de códigos a buscar
    patron = r'^(' + '|'.join(codigos) + r')\w*' # Patrón de búsqueda
    df = df[~df['Codigo'].str.contains(patron, flags=re.IGNORECASE, regex=True)] # Filtrar y eliminar filas
    df = df[~df.applymap(lambda x: 'EDUCACIÓN CONTINUA - Única' in str(x)).any(axis=1)] # Eliminar filas con 'Educación continua' en alguna columna
    df = df[~df['Codigo'].str.contains('Convenio')] # Eliminar filas que contengan 'Convenio' en la columna 'Codigo'
    df = df.fillna('-') # Rellenar valores nulos con guion
    df = df[~df['Nombre_docente'].str.contains('-')] # Eliminar filas con guiones en la columna 'Nombre_docente'
    df['Concatenar'] = df['Codigo'] + df['Nombre'] + df['Nombre_periodo'] # Se concatena la información que será comparada con el DF de inasistencias
    return df

# Función que elimina el trimestre que no aplica, usando la fecha de hoy como referencia para ubicarse en el rango de fechas.
def eliminar_TrimestreCursos(ndf, hoy):
    extra = hoy  - timedelta(days=2)
    ndf['fecha_vencimiento'] = ndf['Fecha_fin'].apply(lambda x: x.date() < extra) # Crear una nueva columna para verificar si la fecha de hoy es posterior a la fecha final
    ndf['fecha_dentro_rango'] = ndf.apply(lambda x: x['Fecha_inicio'].date() <= extra <= x['Fecha_fin'].date(), axis=1) # Crear una nueva columna para verificar si la fecha de hoy está dentro del rango
    ndf = ndf[(ndf['fecha_vencimiento'] == False) & (ndf['fecha_dentro_rango'] == True)] # Eliminar las filas cuya fecha de vencimiento sea verdadera o la fecha esté fuera del rango
    ndf = ndf.drop(['fecha_vencimiento', 'fecha_dentro_rango'], axis=1) # Eliminar las columnas de fecha de vencimiento y fecha dentro del rango
    return ndf

# Función para exportar en un archivo formato CSV el listado de cursos activos en el trimestre.
def exportar_DF(df, nombreArchivo):
    df.to_csv(nombreArchivo, index=False, encoding='UTF-8-sig')

# ________________________ ________________________ ________________ __________________________ ________________________ ________________________ __________________

# ________________________ ________________________ ________________ Funciones para Inasistencias ________________________ ________________________ ________________

# Función para llenar los vacíos de las columnas y así poder ser cargados en el CSV.
def llenar_Vacios(df,col):
    df[col] = df[col].fillna(' ')
    return df[col]

# Función para concatenar el nombre del estudiante y aplicar un formato estetico a la información.
def concat_Nombre(df,col1,col2,col3,col4):
    df['Estudiante'] = (df[col1]+' '+ df[col2] + ' ' + df[col3] +' '+ df[col4]).str.replace(r'\s{2,}', ' ', regex=True).str.title()
    return df['Estudiante']

# Función para extraer los datos del JSON anidados, es decir, extrae la fecha de inasistencia.
def normalize_json(df, record_path, meta_cols, errors='ignore'):
    return pd.json_normalize(df.to_dict('records'), 
                            record_path=record_path, 
                            meta=meta_cols,
                            errors=errors)

# Función para extraer el nombre de los docentes de forma simplificada.
def extraer_NombreD(df, col):
    # Split full name column into first and last names, and assign them to new columns
    df[['PrimerA', 'PrimerN']] = df[col].str.split(' ', 2, expand=True)[[0,2]]
    
    # Keep only first name in PrimerN column
    df['PrimerN'] = df['PrimerN'].str.split(' ', 2, expand=True)[[0]]
    
    # Concatenate first and last name columns and assign to new column
    df['Docente'] = df['PrimerN'] + ' ' + df['PrimerA']
    
    # Drop temporary columns
    df = df.drop(columns=['PrimerA', 'PrimerN'])
    
    return df

# Función para traer la semana del periodo que se está validando
def validar_Periodo(df1, df2):
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

# Función para eliminar de la columna "Cursos.Nombre_curso" las filas que contengan cadenas que no apliquen para la validación del dashboard.
def limpiar_DF(df,texto):
    criterioelim = df['Cursos.Nombre_curso'].str.contains(texto)
    df = df.loc[~criterioelim]
    return df

# Función para leer archivos
def leer_archivo(nombre_archivo, tipo_archivo, **kwargs):
    if tipo_archivo == 'csv':
        return pd.read_csv(nombre_archivo, **kwargs)
    elif tipo_archivo == 'excel':
        return pd.read_excel(nombre_archivo, **kwargs)
    elif tipo_archivo == 'json':
        return pd.read_json(nombre_archivo, **kwargs)
    elif tipo_archivo == 'sql':
        # Reemplaza 'nombre_db' y 'nombre_tabla' por los nombres correspondientes a tu base de datos y tabla
        consulta_sql = f"SELECT * FROM nombre_tabla"
        return pd.read_sql(consulta_sql, 'nombre_db', **kwargs)
    else:
        raise ValueError('Tipo de archivo no soportado')

# Función que calcula la cantidad de clases que tiene un estudiante a la semana
def ClasesXSemana(ndf):
    dias = ['lu', 'ma', 'mi', 'ju', 'vi', 'sá', 'do']
    resultados = []
    for x in range(len(ndf)):
        horario = ndf.loc[x, 'Cursos.Horario_curso']
        contador = 0
        if horario:
            horario_dias = re.findall(r'[a-záéíóú]+', horario.lower())
            for dia in dias:
                if dia in horario_dias:
                    contador += 1
            resultados.append(contador)
        else:
            resultados.append(0)
    return resultados

# Función para ajustar la columna de "Deserción", "Consecutivo", "Aplica" y "AplicaDesercion".
def procesar_Inasistencias(ndf):
    # Se definen columnas vacías para poder almacenar los datos que se recolecten de los bucles implementados.
    ndf['Fecha'] = pd.to_datetime(ndf['Fecha'])
    ndf['Deserción'] = 0
    ndf['Consecutivo'] = 0
    
    # Se compara la fecha de la fila superior con la fecha de la fila actual, este dato nos ayuda a calcular la cantidad de semanas que pasan entre una inasistencia y otra.
    for x in range(ndf['Estudiante'].count()-1):
        if((ndf.loc[x,'Cursos.Cantidad_inasistencia'] >= 3) and ndf.loc[x+1,'Numero_identificacion_estudiante'] == ndf.loc[x,'Numero_identificacion_estudiante']) and (ndf.loc[x+1,'Cursos.Nombre_modulo'] == ndf.loc[x,'Cursos.Nombre_modulo']) and (ndf.loc[x+1,'Cursos.Nombre_curso'] == ndf.loc[x,'Cursos.Nombre_curso']):
            ndf.loc[x+1,'Deserción'] = (ndf.loc[x+1,'Fecha'])-(ndf.loc[x,'Fecha'])
    
    # Si la fila de deserción es 0, entonces se toma el dato de deserción de la fila siguiente, esto es solamente para tener completos la cantidad de semanas que pasan entre una inasistencia y otra.
    for x in range(ndf['Estudiante'].count()-2, -1, -1):
        if((ndf.loc[x,'Deserción'] == 0) and ndf.loc[x+1,'Numero_identificacion_estudiante'] == ndf.loc[x,'Numero_identificacion_estudiante']) and (ndf.loc[x+1,'Cursos.Nombre_modulo'] == ndf.loc[x,'Cursos.Nombre_modulo']) and (ndf.loc[x+1,'Cursos.Nombre_curso'] == ndf.loc[x,'Cursos.Nombre_curso']):
            ndf.loc[x,'Deserción'] = ndf.loc[x+1,'Deserción']
    
    # Se crea un consecutivo para evaluar la cantidad de inasistencias que tiene el estudiante.
    contador = 0
    for x in range(len(ndf)-1):
        if (ndf.loc[x,'Numero_identificacion_estudiante'] == ndf.loc[x+1,'Numero_identificacion_estudiante']) and (ndf.loc[x,'Cursos.Nombre_modulo'] == ndf.loc[x+1,'Cursos.Nombre_modulo']) and (ndf.loc[x,'Cursos.Nombre_curso'] == ndf.loc[x+1,'Cursos.Nombre_curso']):
            contador += 1
        else:
            contador = 0
        ndf.loc[x,'Consecutivo'] = contador

    # 
    for x in range(1, len(ndf)):
        if ndf.loc[x, 'Consecutivo'] == 0 and \
            ndf.loc[x, 'Deserción'] != 0 and \
            ndf.loc[x, 'Numero_identificacion_estudiante'] == ndf.loc[x-1, 'Numero_identificacion_estudiante'] and \
            ndf.loc[x, 'Cursos.Nombre_modulo'] == ndf.loc[x-1, 'Cursos.Nombre_modulo'] and \
            ndf.loc[x, 'Cursos.Nombre_curso'] == ndf.loc[x-1, 'Cursos.Nombre_curso']:

            if ndf.loc[x, 'Deserción'] != 0 and ndf.loc[x-1, 'Consecutivo'] == 0:
                ndf.loc[x, 'Consecutivo'] = 1
            else:
                ndf.loc[x, 'Consecutivo'] = ndf.loc[x-1, 'Consecutivo'] + 1

    return ndf

# Función que permite calcular la media de inasistencias de un estudiante.
def media_Desercion(ndf):
    condicion = (ndf['Deserción'] != 0) & (ndf['Consecutivo'] == 1)
    aplica_grupo = ndf.groupby(['Numero_identificacion_estudiante', 'Cursos.Nombre_modulo', 'Cursos.Nombre_curso'])['Deserción'].transform('mean').where(condicion, ndf['Aplica'])
    ndf['Aplica'] = pd.to_numeric(aplica_grupo)
    
    return ndf

# Función que elimina las inasistencias que no están dentro del rango de fechas del trimestre que se está evaluando.
def eliminar_Trimestre(ndf):
    ndf['Fecha_inicio'] = pd.to_datetime(ndf['Fecha_inicio'])
    ndf['Fecha'] = pd.to_datetime(ndf['Fecha'])
    for x in range(ndf['Estudiante'].count()-1):
        if(ndf.loc[x,'Fecha'] < ndf.loc[x,'Fecha_inicio']):
            ndf = ndf.drop(x,axis=0)
    return ndf

# Función para calcular las inasistencias consecutivas de un estudiante que tiene dos clases a la semana.

def detectar_deserciones(ndf):
    # Crea una columna con el número de semana
    ndf['semana'] = ndf['Fecha'].dt.week
    
    # Agrupa los datos por estudiante y semana, y agrega un contador para cada fila dentro de cada grupo.
    ndf['contador'] = ndf.groupby(['Numero_identificacion_estudiante','Concat_curso', ndf['semana']]).cumcount() + 1
    
    # Crea una columna con el número de semana y el indicador de repetición
    ndf['semana_con_indicador'] = ndf['semana'].astype(str) + ndf['contador'].astype(str)
        
    return ndf


# Función para eliminar las fechas futuras de inasistencias que no aplican.
def fechas_Futuras(ndf):
    hoy = datetime.today().date()
    hoy = hoy - timedelta(days=2)
    ndf['Fecha'] = pd.to_datetime(ndf['Fecha'])
    indices_a_eliminar = []
    
    for idx in ndf.index:
        if ndf.loc[idx, 'Fecha'] > hoy:
            indices_a_eliminar.append(idx)

    ndf = ndf.drop(indices_a_eliminar)
    return ndf


# Función para extraer únicamente la información de las ultimas 3 semanas que apliquen como inasistencias de cada estudiante.
def tres_Semanas(ndf):
    grupos = ndf.groupby(['Numero_identificacion_estudiante', 'Concat_curso', 'ClasesXSemana'], group_keys= True) # Paso 1: Agrupar estudiantes con las últimas 3 inasistencias
    df_actualizado = grupos.apply(lambda x: x.sort_values('semana', ascending=True)).reset_index(drop=True) # Paso 2: Ordenar cada grupo de forma ascendente
    fecha_actual = datetime.today().date() # Paso 3: asignar a las variables "fecha_actual" & "semana_actual" la información para realizar los calculos de la deserción
    semana_actual = fecha_actual.isocalendar()[1] # Paso 3: asignar a las variables "fecha_actual" & "semana_actual" la información para realizar los calculos de la deserción
    df_actualizado['MaxSemana'] = df_actualizado.groupby(['Numero_identificacion_estudiante', 'Concat_curso', 'ClasesXSemana'], group_keys= True)['semana'].transform('max') # Paso 4: Obtener el máximo de la columna 'semana' en cada grupo y guardar en 'MaxSemana'
    df_actualizado=df_actualizado[df_actualizado['MaxSemana'] >= semana_actual-1] # Paso 5: Se filtra unicamente las semanas que se están validando
    df_actualizado.reset_index(inplace=True)# Paso 6: Se reinicia el index del DF
    ndf = df_actualizado.groupby(['Numero_identificacion_estudiante', 'Concat_curso', 'ClasesXSemana']).filter(lambda x: len(x) >= 3) # Paso 7: Filtrar grupos con exactamente 3 filas
    ndf = ndf.groupby(['Numero_identificacion_estudiante', 'Concat_curso', 'ClasesXSemana']).tail(3) # Paso 7: Filtrar grupos con exactamente 3 filas
    return ndf

# Función para calcular los estudiantes de cualquier programa que aplican como deserción.
def aplica_Desercion(ndf):
    grupos_estudiantes = ndf.groupby(['Numero_identificacion_estudiante', 'Concat_curso', 'ClasesXSemana']) # Paso 1: Agrupar el DataFrame por las columnas de los grupos de estudiantes
    ndf['sumas_Desercion'] = grupos_estudiantes['Deserción'].transform('sum') # Paso 2: Calcular la suma de la columna "Deserción" para cada grupo
    ndf.reset_index(inplace=True) # Paso 3: se reinicia el index del DF para evitar conflictos en las iteraciones de los siguientes pasos
    # Paso 4: Iteración que valida si la suma de la columna "Deserción" es igual a ciertas cantidades de números, que por lógica aplicarían como deserción
    for x in range(ndf['Estudiante'].count()):
        if ((ndf.loc[x,'ClasesXSemana']==1 and ndf.loc[x,'sumas_Desercion']==21) or (ndf.loc[x,'ClasesXSemana']==2 and ndf.loc[x,'sumas_Desercion']==8) or (ndf.loc[x,'ClasesXSemana']==2 and ndf.loc[x,'sumas_Desercion']==13) or (ndf.loc[x,'ClasesXSemana']==4 and ndf.loc[x,'sumas_Desercion']==3)or (ndf.loc[x,'ClasesXSemana']==4 and ndf.loc[x,'sumas_Desercion']==6)):
            ndf.loc[x,'Desercion']=1
        else:
            ndf.loc[x,'Desercion']=0
    ndf['sumas_Desercion'] = ndf['sumas_Desercion'].astype(int) # Paso 4: se convierte en entero la columna
    ndf = ndf.drop(columns=['level_0', 'index']) # Paso 5: se eliminan las columnas que no funcionan para el analisis del DF, las cuales python asigna por defecto
    return ndf

# Prueba de nuevo
def prueba(ndf):
    ndf = ndf.reset_index(drop=True)
    for x in range(len(ndf) - 2):
        if (ndf.loc[x, 'Deserción'] != 7 and ndf.loc[x + 1, 'Deserción'] == 7 and ndf.loc[x + 2, 'Deserción'] == 7 and ndf.loc[x, 'Numero_identificacion_estudiante'] == ndf.loc[x + 1, 'Numero_identificacion_estudiante'] and
                    ndf.loc[x, 'Cursos.Nombre_modulo'] == ndf.loc[x + 1, 'Cursos.Nombre_modulo'] and
                    ndf.loc[x, 'Cursos.Nombre_curso'] == ndf.loc[x + 1, 'Cursos.Nombre_curso'] and ndf.loc[x, 'Numero_identificacion_estudiante'] == ndf.loc[x + 2, 'Numero_identificacion_estudiante'] and
                    ndf.loc[x, 'Cursos.Nombre_modulo'] == ndf.loc[x + 2, 'Cursos.Nombre_modulo'] and
                    ndf.loc[x, 'Cursos.Nombre_curso'] == ndf.loc[x + 2, 'Cursos.Nombre_curso']):
            ndf.loc[x, 'Deserción'] = 7
    return ndf

