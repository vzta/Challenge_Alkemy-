from datetime import datetime
import sqlalchemy
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from alkemy import Data_final, tabla_consigna, Tabla_cines
from decouple import config

# conecto a la base de datos
# nótese que utilizo 'secrets' haciendo referencia a datos sensibles para conectarme a la bbdd
engine = sqlalchemy.create_engine(config('secret'), echo=False)
Base = declarative_base()


# Creación de tablas mediante sqlalchemy

class Tabla1(Base):
    __tablename__ = 'Tabla_principal'
    id = Column(Integer(), primary_key=True)
    cod_localidad = Column(String(100))
    id_provincia = Column(String(1000))
    id_departamento = Column(String(1000))
    categoria = Column(String(1000))
    provincia = Column(String(1000))
    localidad = Column(String(1000))
    nombre = Column(String(1000))
    domicilio = Column(String(1000))
    codigo_postal = Column("codigo postal", String(1000))
    telefono = Column("Numero de telefono", String(1000))
    mail = Column(String(1000))
    web = Column(String(1000))
    created_at = Column(DateTime(), default=datetime.now())

    def __str__(self):
        return self.cod_localidad


class Tabla2(Base):
    __tablename__ = 'tabla_consigna'
    id = Column(Integer(), primary_key=True)
    Fuente = Column(String(1000))
    Provincia = Column(String(1000))
    Categoria = Column(String(1000))
    created_at = Column(DateTime(), default=datetime.now())

    def __str__(self):
        return self.Fuente


class Tabla3(Base):
    __tablename__ = 'tabla_cines'
    id = Column(Integer(), primary_key=True)
    Provincia = Column(String(1000))
    Pantallas = Column(Integer)
    Butacas = Column(Integer)
    espacio_INCAA = Column(String(100))
    created_at = Column(DateTime(), default=datetime.now())

    def __str__(self):
        return self.Provincia


Session = sessionmaker(bind=engine)
session = Session()

if __name__ == '__main__':
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

# llamo a los dataframe creados en el otro script .py para llenar / actualizar base de datos
Tabla_cines['created_at'] = str(datetime.now())
Data_final['created_at'] = str(datetime.now())
tabla_consigna['created_at'] = str(datetime.now())
tabla_consigna.to_sql('tabla_consigna', if_exists='append', con=engine, index=False)
Data_final.to_sql('Tabla_principal', if_exists='append', con=engine, index=False)
Tabla_cines.to_sql('tabla_cines', if_exists='append', con=engine, index=False)


def consultas():  # creé una función para visualizar algunos datos de la bbdd
    prov = input('Ingrese la provincia que para los centros culturales que desea visualizar:'
                 ' \n Ejemplo: "Buenos Aires \n"')
    tablas = session.query(Tabla1.provincia, Tabla1.nombre).filter(
        Tabla1.provincia == prov
    )
    for tabla in tablas:
        print(tabla)


consultas()
