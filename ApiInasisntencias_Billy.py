import pandas as pd
import json
from datetime import datetime
from Funciones import *
import os

# Se definen los parametros que se enviarán a las diferentes funciones del archivo "Funciones".
# urlInasistencias = "https://api.q10.com/v1/inasistencias?Fecha_inicio_inasistencia=2023-04-01&Fecha_fin_inasistencia=2023-05-30&Api-Key=d97a2c8cf0b94745a54ff30109c5b90f&limit=99999"
urlInasistencias = "https://api.q10.com/v1/inasistencias?Fecha_inicio_inasistencia=2023-02-06&Fecha_fin_inasistencia=2024-03-17&Api-Key=d97a2c8cf0b94745a54ff30109c5b90f&limit=99999"
key = 'd97a2c8cf0b94745a54ff30109c5b90f'
cookie = '.ASPXANONYMOUS=xr0rL3sQ33_iv-iu9D8b_5xvBiVQVz_-GgQo2bMzJVbBV27mzvXTqsk6kMqd6AAJl27DGieeQAItB_kcQdOAjN0s0x3mONjkQQUSIuM6OUOVf41Tr68PzygrHtmSXbPAeoFRXw2'
hoy = datetime.today().date()

# Valida que la conexión con la API sea efectiva para proceder con el código.
Conexion_OK(urlInasistencias)

# Define una matriz vacía para posteriormente almacenar la información que s extraíga de la API Cursos, también define el total de registros que serán encontrados.
records = []
total_records = 0

# Se realiza la definición de las cabeceras que se utilizan para autenticar la solicitud
headers ={
    'Api-Key': key,
    'Cookie': cookie
}

# Se almacena la información en la matriz de "records" y "total_records", estos se obtienen con la  función "obtener_Cursos".
records, total_records = obtener_Api(urlInasistencias, headers)

# Se almacena la información en un archivo Json y esta misma es utilizada para crear el DF
with open('records.json', 'w') as f:
    json.dump(records, f)
df = pd.read_json('records.json')

# Se utiliza la función "llenar_Vacios" para poder llenar con espacios los vacíos de las columnas del DF.
df['Segundo_nombre'] = llenar_Vacios(df,'Segundo_nombre')
df['Segundo_apellido'] = llenar_Vacios(df,'Segundo_apellido')

# Con al función "concat_Nombre" se concatenan los nombres y apellidos del estudiante.
df['Estudiante'] = concat_Nombre(df,'Primer_nombre', 'Segundo_nombre', 'Primer_apellido','Segundo_apellido')

# Con la función "normalize_json" se extrae la información de las inasistencias que está almacenada dentro del JSON que se obtiene de la API.
df = normalize_json(df, record_path=['Cursos', 'Inasistencias'], 
                    meta_cols=['Estudiante', 'Numero_identificacion_estudiante', 'Sexo', 
                            'Correo_electronico_personal', 'Celular', 
                            ['Cursos', 'Nombre_docente'], ['Cursos', 'Codigo_modulo'], 
                            ['Cursos', 'Nombre_modulo'], ['Cursos', 'Codigo_curso'], 
                            ['Cursos', 'Nombre_curso'], ['Cursos', 'Cantidad_inasistencia'], 
                            ['Cursos', 'Periodo_curso'],['Cursos', 'Horario_curso']])

# Con la función "extraer_NombreD" se ajusta a una forma simplificada de primer nombre y primer apellido el nombre de los doncente.
df = extraer_NombreD(df, 'Cursos.Nombre_docente')

# Se llama al archivo "Def_Periodos.xlsx" para realizar la consulta de los periodos que se van a trabajar.
periodos = leer_archivo('Def_Periodos.xlsx', 'excel', usecols=['Periodo', 'FechaInicio', 'FechaFin', 'PeriodoNombre', 'SemanaClase'], sheet_name=None)

# Se definen dos columnas nuevas para realizar la ejecución del código, se aplica la función "validar_Periodo" para identificar y asignar el periodo a cada inasistencia del estudiante
fecha = df['Fecha']
periodo = df['Cursos.Periodo_curso']
PeriodoNombre = validar_Periodo(df, periodos)
df = df.assign(PeriodoNombre=PeriodoNombre["Periodo"])
periodos = periodos['Periodos']
df = pd.merge(df, periodos, on='PeriodoNombre', how='left')

# Se concatenan los valores que se van a comprar y servirán como llaves de búsqueda con el CSV de listado de cursos.
df['Concat_curso'] = df['Cursos.Codigo_curso'] + df ['Cursos.Nombre_curso'] + df ['Cursos.Periodo_curso']

# Se genera una lógica para asignar el tipo de deserción
df["Tipo de Deserción"] = df.apply(lambda row: "Administrativa" if row["Cursos.Cantidad_inasistencia"] >= (row["SemanaClase"]) else "Académica", axis=1)

# Emplea la función "limpiar_DF" para eliminar los conveios de la información.
df = limpiar_DF(df,'Convenio')

# Emplea la función "leer_archivo" para leer el CSV que se genera con la API de cursos.
cursos = leer_archivo('ListadoCursos.csv', 'csv')

# Se unen los dataframes df y cursos usando como clave de unión las columnas Concat_curso y Concatenar, respectivamente
merged_df = pd.merge(df, cursos, left_on='Concat_curso', right_on='Concatenar')
merged_df = merged_df.drop_duplicates()

# Se seleccionan las columnas deseadas del dataframe merged_df y guardarlas en un nuevo dataframe ndf. Se ordena el DF creando una columna de ordenamiento por estudiante, fecha y curso
ndf = merged_df[['Estudiante', 'Numero_identificacion_estudiante', 'Nombre_programa', 'Cursos.Codigo_modulo','Cursos.Nombre_modulo', 'Cursos.Codigo_curso','Cursos.Nombre_curso', 'Cursos.Nombre_docente','Correo_electronico_personal', 'Celular', 'Cursos.Cantidad_inasistencia', 'Fecha','Docente', 'PeriodoNombre','Tipo de Deserción','SemanaClase','Fecha_inicio', 'Fecha_fin','Cursos.Periodo_curso', 'Concat_curso', 'Cursos.Horario_curso']]
ndf['Ordenar'] =  ndf['Numero_identificacion_estudiante'].astype(str) + ndf['Fecha'].astype(str) + ndf['Cursos.Nombre_curso']
ndf.sort_values('Ordenar', inplace=True)

# Ordenar el dataframe por estudiante y fecha.
ndf = ndf.sort_values(['Cursos.Nombre_curso', 'Numero_identificacion_estudiante', 'Fecha'])

# Se reinicia el consecutivo del DF
ndf = ndf.reset_index(drop=True)

# Se asignan columnas vacías para la lógica de inasistencias consecutivas.
ndf['Deserción'] = pd.Series([0]*len(ndf.index))
ndf['Consecutivo'] = pd.Series([0]*len(ndf.index))
ndf['Aplica'] = pd.Series([0]*len(ndf.index))
ndf['ClasesXSemana'] = pd.Series([0]*len(ndf.index))
ndf['Desercion'] = pd.Series([0]*len(ndf.index))
ndf['MaxSemana'] = pd.Series([0]*len(ndf.index))
ndf['sumas_Desercion']= pd.Series([0]*len(ndf.index))
ndf['Día de la semana'] = pd.Series([0]*len(ndf.index))

# Se definen las variables que se van a utilizar en la lógica de inasistencias consecutivas.
variable = (range(ndf['Estudiante'].count()-1))
ndf['Fecha'] = pd.to_datetime(ndf['Fecha'])

# Emplea la función "ClasesXSemana" para calcular la cantidad de clases que el estudiante ve en la semana.
ndf['ClasesXSemana'] = ClasesXSemana(ndf)

# Con la función "procesar_Inasistencias" se utiliza para evaluar los estudiantes que cuentan con inasistencias consecutivas.
ndf = procesar_Inasistencias(ndf)

# Convierte el formato de la columna 'Deserción' de días a texto 
ndf['Deserción'] = pd.to_timedelta(ndf['Deserción']).dt.days.astype(str)
ndf['Deserción'] = pd.to_numeric(ndf['Deserción'])

# Emplea la función "media_Desercion" para calcular la media de deserción de un estudiante, con esta se define en la lógica de deserción si aplica o no como deserción. 
ndf = media_Desercion(ndf)

# Prueba de lógica de deserción.
ndf = detectar_deserciones(ndf)

# Emplea función "eliminar_Trimestre" para eliminar las inasistencias que no aplican al trimestre que está siendo evaluado.
ndf = eliminar_Trimestre(ndf)

# Emplea la función de eliminar los registros de inasistencias futuras.
ndf = fechas_Futuras(ndf)

# Emplea la función para extraer las ultimas tres semanas con inasistencias de cada estudiante.
ndf = tres_Semanas(ndf)

# Prueba
ndf = prueba(ndf)

# Emplea la función para calcular la deserción de estudiantes de cualquier programa.
ndf = aplica_Desercion(ndf)

# Asigna el día de la semana a la columna de Fecha de inasistencia del estudiante.
ndf['Día de la semana'] = ndf['Fecha'].dt.strftime('%A').str.capitalize()

# Mapeo personalizado de nombres de días de la semana
dia_semana = {
    'Monday': 'Lunes',
    'Tuesday': 'Martes',
    'Wednesday': 'Miércoles',
    'Thursday': 'Jueves',
    'Friday': 'Viernes',
    'Saturday': 'Sábado',
    'Sunday': 'Domingo'
}

# Aplicar el mapeo a la columna de días de la semana
ndf['Día de la semana'] = ndf['Día de la semana'].map(dia_semana)

# Emplea la función "exportar_DF" para guardar el archivo que alimentará el informe en Power BI.
exportar_DF(ndf,'Inasistencias.csv')

# Ruta completa de la carpeta donde deseas guardar el archivo
ruta_carpeta = 'C:/Users/Front End/Desktop/Task_Files Red/21. Billy API/Billy_Api/RepoArchivos'

# Emplea la función "exportar_DF" para exportar a la carpeta repositorio de archivos.
Repo =  datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '_Inasistencias' + '.csv'

# Ruta completa del archivo
ToRepo = os.path.join(ruta_carpeta, Repo)

exportar_DF(ndf,ToRepo)
