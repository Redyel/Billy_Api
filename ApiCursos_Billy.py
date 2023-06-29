import pandas as pd
from datetime import datetime
from Funciones import *
import os


# Se definen los parametros que se enviarán a las diferentes funciones del archivo "Funciones".
urlCursos = "https://api.q10.com/v1/cursos?&Api-Key=d97a2c8cf0b94745a54ff30109c5b90f&limit=9999999"
key = 'd97a2c8cf0b94745a54ff30109c5b90f'
cookie = '.ASPXANONYMOUS=xr0rL3sQ33_iv-iu9D8b_5xvBiVQVz_-GgQo2bMzJVbBV27mzvXTqsk6kMqd6AAJl27DGieeQAItB_kcQdOAjN0s0x3mONjkQQUSIuM6OUOVf41Tr68PzygrHtmSXbPAeoFRXw2'
hoy = datetime.today().date()

# Valida que la conexión con la API sea efectiva para proceder con el código.
Conexion_OK(urlCursos)

# Define una matriz vacía para posteriormente almacenar la información que s extraíga de la API Cursos, también define el total de registros que serán encontrados.
records = []
total_records = 0

# Se realiza la definición de las cabeceras que se utilizan para autenticar la solicitud
headers ={
    'Api-Key': key,
    'Cookie': cookie
}

# Se almacena la información en la matriz de "records" y "total_records", estos se obtienen con la  función "obtener_Cursos".
records, total_records = obtener_Api(urlCursos, headers)
with open('Cursos.json', 'w') as f:
    json.dump(records, f)

# Se crea el df con el cual se trabajarán los datos.
df = pd.DataFrame(records)

# Se convierten las fechas de inicio y fin del df en tipo String-Time
df['Fecha_inicio'] = pd.to_datetime(df['Fecha_inicio']).dt.strftime('%d-%m-%Y')
df['Fecha_fin'] = pd.to_datetime(df['Fecha_fin']).dt.strftime('%d-%m-%Y')

# Por medio de la función "limpiar_DFCursos" se limpia el DF de la información basura.
df = limpiar_DFCursos(df)

# Se define el nuevo DF con la información de interés que será trabajada
ndf = df.loc[:,['Nombre_sede_jornada','Nombre_programa','Codigo','Nombre','Nombre_docente','Fecha_inicio','Fecha_fin','Cupo_maximo','Cantidad_estudiantes_matriculados','Nombre_periodo','Concatenar']]

# Convertir las columnas de fecha en objetos de fecha
ndf['Fecha_inicio'] = pd.to_datetime(ndf['Fecha_inicio'], format='%d-%m-%Y')
ndf['Fecha_fin'] = pd.to_datetime(ndf['Fecha_fin'], format='%d-%m-%Y')

# Se eliminan las fechas que no aplican al trimestre actual y se actualiza el DF.
ndf = eliminar_TrimestreCursos(ndf, hoy)

# Se utiliza la función de "exportar_DF" para exportar el DF en formato de CSV.
exportar_DF(ndf,'ListadoCursos.csv')


# Ruta completa de la carpeta donde deseas guardar el archivo
ruta_carpeta = 'C:/Users/Front End/Desktop/Task_Files Red/21. Billy API/Billy_Api/RepoArchivos'

# Emplea la función "exportar_DF" para exportar a la carpeta repositorio de archivos.
Repo = datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '_ListadoCursos' + '.csv'

# Ruta completa del archivo
ToRepo = os.path.join(ruta_carpeta, Repo)

# Emplea la función "exportar_DF" para exportar a la carpeta repositorio de archivos.
exportar_DF(ndf,ToRepo)
