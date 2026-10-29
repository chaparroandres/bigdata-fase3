# Procesamiento en batch con Apache Spark: casos positivos de COVID-19 en Colombia

Proyecto de la Tarea 3 del curso Big Data (código 202016911), Universidad Nacional Abierta y a Distancia, programa de Ingeniería de Sistemas. Autor: Andrés Chaparro.

## Descripción de la solución

El proyecto resuelve un problema de análisis epidemiológico: identificar cómo se distribuyeron los casos positivos de COVID-19 en Colombia en el tiempo, por departamento, por sexo y por edad, y calcular la letalidad en cada grupo para ver dónde fue mayor el riesgo. El conjunto de datos es el consolidado nacional de casos positivos que publica el Instituto Nacional de Salud en el portal de datos abiertos de Colombia (https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia/gt2j-8ykr), que supera los seis millones de registros y por eso justifica un motor distribuido como Spark.

La aplicación está escrita en Python con PySpark y se organiza en cuatro etapas. En la carga, el CSV original se lee como texto, se normalizan los nombres de las columnas, y se muestra el esquema. En la limpieza, se eliminan espacios y cadenas vacías, se descartan los registros duplicados por identificador de caso, se convierte la fecha de notificación a tipo fecha, se lleva la edad a años, se construye un indicador de fallecido, y se filtran los registros con fecha, departamento, sexo o edad inválidos. En el análisis exploratorio, se calculan estadísticas descriptivas, valores nulos, casos por mes, por departamento, por sexo y por grupo de edad, la letalidad con DataFrames y con Spark SQL, y un conteo por departamento con RDDs (map y reduceByKey). En la visualización, se generan cuatro gráficas en PNG y cinco tablas en CSV dentro de la carpeta de salida.

## Estructura del repositorio

```
.
├── README.md
├── requirements.txt
├── .gitignore
├── scripts/
│   └── download_data.sh      Descarga el conjunto de datos desde datos.gov.co
└── src/
    └── batch_analysis.py     Aplicación Spark (carga, limpieza, EDA y gráficas)
```

## Requisitos

La aplicación se probó sobre Ubuntu 22.04 con Java 11, Apache Spark 3.5 y Python 3. Se necesitan además las librerías pandas y matplotlib, que se usan solo para graficar los resultados ya agregados (unas pocas filas).

```bash
java -version
spark-submit --version
pip install -r requirements.txt
```

## Instrucciones de ejecución

Primero se clona el repositorio y se descargan los datos. El script pagina la API del portal de a 50.000 registros y acepta como parámetro el total de registros que se quiere traer; por defecto trae un millón, que ocupa alrededor de 200 MB.

```bash
git clone https://github.com/<usuario>/bigdata-batch-covid-colombia.git
cd bigdata-batch-covid-colombia
./scripts/download_data.sh 1000000
```

Luego se ejecuta la aplicación con spark-submit, indicando el archivo de entrada y la carpeta de salida.

```bash
spark-submit src/batch_analysis.py --input data/casos_covid.csv --output output
```

Si el sistema tiene varias versiones de Python y Spark usa una que no corresponde, se fija el intérprete antes de ejecutar:

```bash
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
```

## Resultados generados

Al terminar, en la carpeta `output/` quedan las tablas `casos_por_mes.csv`, `casos_por_departamento_top10.csv`, `casos_por_edad_sexo.csv`, `letalidad_por_edad.csv` y `letalidad_por_departamento.csv`, y en `output/figuras/` las gráficas `01_casos_por_mes.png`, `02_top10_departamentos.png`, `03_casos_edad_sexo.png` y `04_letalidad_por_edad.png`. La consola muestra además el esquema, el número de registros antes y después de la limpieza, y las tablas del análisis exploratorio.

## Procesamiento en tiempo real

El componente de tiempo real (Kafka y Spark Streaming) se desarrolló siguiendo el Anexo 3 del curso: un productor en Python envía lecturas simuladas de sensores al topic `sensor_data`, y un consumidor en Spark Streaming calcula la temperatura y la humedad promedio por sensor en ventanas de un minuto.

## Fuente de los datos

Instituto Nacional de Salud (2020 en adelante). Casos positivos de COVID-19 en Colombia [Conjunto de datos]. Portal de Datos Abiertos de Colombia, datos.gov.co. https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia/gt2j-8ykr
