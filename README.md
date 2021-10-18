# Sampleador de hogares censados en 2010

El codigo en este repositorio toma una muestra aleatoria de hogares que fueron censados en el Censo Nacional de Hogares y Viviendas de 2010. 

Para usarlo, Ud. necesita contar con los archivos de la base de Datos del Censo 2010.  

   - La base de Datos en formato REDATAM disponible en el repositorio no oficial de datos abierto (DatAR)

http://datar.info/dataset/censo-nacional-de-poblacion-hogares-y-viviendas-2010-cd-redatam

   - Un convertidor de REDATAM (funciona en Windows) disponible en:
    
https://www.aacademica.org/conversor.redatam


## Modo de uso:

Clonar repositorio:

`$ git clone https://github.com/matuteiglesias/samplerCensoARG.git`

O descargarlo como archivo zip. Una vez clonado el repositorio, extraiga muestras del censo corriendo el archivo `samplear.py` que quedan guardadas como archivo `csv`.

`$ cd samplerCensoARG`

**Synopsis**:

`samplear.py [-h] -dbp DATABASEPATH [-f FRAC [FRAC ...]] [-y YEARS [YEARS ...]] [-n NOMBRE] [-d DEPARTAMENTOS [DEPARTAMENTOS ...] |
                   -p PROVINCIAS [PROVINCIAS ...]]`

El unico argumento obligatorio es `DATABASEPATH`, el path donde se encuentran los archivos `PERSONA.csv`, `HOGAR.csv`, `VIVIENDA.csv`. Es decir que la forma minima de usar el script es:

`$ python samplear.py -dbp '/path/ext_CPV2010_basico_radio_pub'`

Por default, el muestreo toma el 1% de los hogares, de todos los distritos de Argentina, con poblacion proyectada al año corriente. 

**Descripcion** de las opciones para cambiar esta configuracion (en orden alfabetico):

       -dbp, --databasepath
              el path a donde se encuentra la informacion del censo en nuestro sistema.

       -d, --departamentos
              opcion para samplear hogares de ciertos departamentos (partidos, comunas). Los codigos se pueden consultar en `./data/info/dptos.csv`

       -f, --frac
              fraccion de hogares muestreados. El valor default es 0.01, es decir, el 1%.

       -p, --provincias
              opcion para samplear hogares de ciertos departamentos (partidos, comunas). Los codigos se pueden consultar en `./data/info/dptos.csv`

       -n, --nombre
              string usado para nombrar el archivo resultante. Por default este archivo viene nombrado por el año y la fraccion de muestreo. El parametro nombre entonces puede servir para distinguir el area geografica muestreada.

       -y, --years
              opcion para elegir los años que se desea muestrear. Se muestrean todos los años entre el par de años indicado.

### Ejemplos

 - Para tomar una muestra del 5% de los hogares del pais en el corriente año:

`$ python samplear.py -dbp '/path/ext_CPV2010_basico_radio_pub' -f 0.05`

 - Para tomar muestras del 5% de los hogares del pais para los años entre 2015 y 2020:

`$ python samplear.py -dbp '/media/miglesia/Elements/suite/ext_CPV2010_basico_radio_pub' -f 0.005 -y 2015 2021`


 - Para tomar muestras del 5% de los hogares de la Ciudad de Buenos Aires en el año 2020

`$ python samplear.py -dbp '/media/miglesia/Elements/suite/ext_CPV2010_basico_radio_pub' -f 0.005 -p 2 -y 2020 2021`


 - Para tomar muestras del 5% de los hogares de los partidos de Pilar y Escobar en el año 2020

`$ python samplear.py -dbp '/media/miglesia/Elements/suite/ext_CPV2010_basico_radio_pub' -f 0.005 -d 6638 6252 -y 2020 2021`


 - Para tomar una muestra del 1% de los hogares del pais en el corriente año, y incluir la palabra 'prueba' en el nombre de archivo:

`$ python samplear.py -dbp '/media/miglesia/Elements/suite/ext_CPV2010_basico_radio_pub' -n 'prueba'`

