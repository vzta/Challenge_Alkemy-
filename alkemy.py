import os
from datetime import date
import pandas as pd
from glob import glob
import json
import requests
import re


today = date.today()
d1 = today.strftime("%d-%m-%Y")  # variable 'd1' para determinar el día de descarga de los archivos

URL = 'https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/'

## Mediante webscrapping y regex busco los links de descargas con los csv necesarios para el challenge ##

r = requests.get(URL)
search = ["Bibliotecas Populares", "Salas de Cine", "Museos"]  # creo lista de strings con las palabras 'claves'

s = re.sub(r'\n\s{2,}', '', re.search(r'"@graph": (\[[\s\S]+{0}[\s\S]+)}}'.format(search[0]), r.text).group(1))
data = json.loads(re.sub(r'\\"', '', re.sub(r'\\u([0-9a-fA-F]{4})', lambda m: chr(int(m.group(1), 16)), s)))
lista = []
for i in data:
    if 'schema:name' in i:
        name = i['schema:name']
        if name in search:
            lista.append(i['schema:url'])  # almaceno los links en la una lista para su próxima manipulación.
print(lista)

# procedo a descargar los archivos correspondientes:

if len(os.listdir('C:/Users/Jullier/.virtualenvs/Jullier-Gf2dfNr2/museos/2021-diciembre')) == 0:
    with open(f'C:/Users/Jullier/.virtualenvs/Jullier-Gf2dfNr2/museos/2021-diciembre/museos-{d1}.csv', 'wb') as file:
        response = requests.get(lista[0])
        file.write(response.content)

# nótese que utilizo la variable 'd1' para el nombramiento del archivo como menciona la consigna.

if len(os.listdir('C:/Users/Jullier/.virtualenvs/Jullier-Gf2dfNr2/salas de cine/2021-diciembre')) == 0:
    with open(f'C:/Users/Jullier/.virtualenvs/Jullier-Gf2dfNr2/salas de cine/2021-diciembre/cines-{d1}.csv', 'wb') as f:
        response = requests.get(lista[1])
        f.write(response.content)

if len(os.listdir('C:/Users/Jullier/.virtualenvs/Jullier-Gf2dfNr2/biliotecas/2021-diciembre')) == 0:
    with open(f'C:/Users/Jullier/.virtualenvs/Jullier-Gf2dfNr2/biliotecas/2021-diciembre/bibliotecas-{d1}.csv',
              'wb') as file:
        response = requests.get(lista[2])
        file.write(response.content)

# ASIGNO NOMBRE A LOS ARCHIVOS ASI SE EJECUTAN DESDE EL DIRECTORIO INDEPENDIENTEMENTE DE SU NOMBRE #

museos = glob("C:/Users/Jullier/.virtualenvs/Jullier-Gf2dfNr2/museos/2021-diciembre/*csv")[0]
cines = glob("C:/Users/Jullier/.virtualenvs/Jullier-Gf2dfNr2/salas de cine/2021-diciembre/*csv")[0]
bibliotecas = glob("C:/Users/Jullier/.virtualenvs/Jullier-Gf2dfNr2/biliotecas/2021-diciembre/*csv")[0]

# LECTURA DE LOS ARCHIVOS EN FORMA DE DATAFRAME#
data_m = pd.read_csv(museos, encoding='latin-1')
data_c = pd.read_csv(cines, encoding='latin-1')
data_b = pd.read_csv(bibliotecas, encoding='latin-1')

# PROCESO DE CREACIÓN Y NORMALIZACIÓN DDE DATOS EN FORMA DE DATAFRAME#
nombre_columnas = ['cod_localidad', 'id_provincia', 'id_departamento', 'categoria', 'provincia',
                   'localidad', 'nombre', 'domicilio', 'codigo postal', 'Numero de telefono', 'mail', 'web']

data_mnew = data_m[
    ['provincia_id', 'localidad_id', 'provincia', 'localidad', 'nombre', 'direccion', 'codigo_postal', 'telefono',
     'mail', 'web']]

data_mnew['Categoria'] = 'museos'
data_museos = data_mnew
data_museos['cod_Localidad'] = '-'
data_museos = data_museos[['cod_Localidad', 'provincia_id', 'localidad_id', 'Categoria', 'provincia',
                           'localidad', 'nombre', 'direccion', 'codigo_postal', 'telefono', 'mail', 'web']]
data_museos.columns = nombre_columnas

data_bnew = data_b[['Cod_Loc', 'IdProvincia', 'IdDepartamento', 'CategorÃ­a', 'Provincia', 'Localidad',
                    'Nombre', 'Domicilio', 'CP', 'TelÃ©fono', 'Mail', 'Web']]

data_cnew = data_c[['Cod_Loc', 'IdProvincia', 'IdDepartamento', 'CategorÃ­a', 'Provincia', 'Localidad',
                    'Nombre', 'DirecciÃ³n', 'CP', 'TelÃ©fono', 'Mail', 'Web']]

# SE RESCATAN LOS DATOS DE LAS COLUMNAS QUE SE NECESITAN PARA LUEGO CONCATENARLAS EN UN SÓLO DATAFRAME#

data_cnew.rename(columns={'DirecciÃ³n': 'Domicilio'}, inplace=True)

data_concat = pd.concat([data_bnew, data_cnew], axis=0, ignore_index=True)
data_concat.columns = nombre_columnas

Data_final = pd.concat([data_concat, data_museos], axis=0, ignore_index=True)  # DataFrame con él cual se irá a trabajar

# Creo la tabla según consigna (CINES)#

Tabla_cines = data_c[['Provincia', 'Pantallas', 'Butacas', 'espacio_INCAA']]

# Normalizando los datos segun consigna para creación de tabla#
tabla_museo = data_m[['fuente', 'provincia']]
tabla_museo['Categoria'] = "museos"
tabla_museo.columns = ['Fuente', 'Provincia', 'Categoria']
cines = data_c[['Fuente', 'Provincia', 'CategorÃ­a']]
biblio = data_b[['Fuente', 'Provincia', 'CategorÃ­a']]
tabla_consigna = pd.concat([biblio, cines], axis=0, ignore_index=True)
tabla_consigna.columns = ['Fuente', 'Provincia', 'Categoria']

# tabla final para la consigna (fuente provincia y categoria)#
tabla_consigna = pd.concat([tabla_consigna, tabla_museo], axis=0, ignore_index=True)
