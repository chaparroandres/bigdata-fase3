#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Procesamiento en batch con Apache Spark (PySpark).

Conjunto de datos: Casos positivos de COVID-19 en Colombia (Instituto Nacional de
Salud, portal datos.gov.co, identificador gt2j-8ykr).

Etapas:
  1. Carga del CSV original.
  2. Limpieza y transformación con DataFrames.
  3. Análisis exploratorio (DataFrames, Spark SQL y un ejemplo con RDDs).
  4. Visualización: gráficas PNG y tablas CSV en la carpeta de salida.
"""
import argparse
import os
import re
import unicodedata
from operator import add

import matplotlib

matplotlib.use("Agg")  # Sin entorno gráfico (servidor Ubuntu por SSH)
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ----------------------------------------------------------------------------
# Utilidades
# ----------------------------------------------------------------------------
def normalize(name):
    """Convierte un nombre de columna a minúsculas, sin tildes ni símbolos."""
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", ascii_name.lower()).strip("_")


def pick(df, *candidates):
    """Devuelve el primer nombre de columna que exista en el DataFrame."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def titulo(texto):
    print("\n" + "=" * 78)
    print(texto)
    print("=" * 78)


def parse_args():
    p = argparse.ArgumentParser(description="Procesamiento batch COVID-19 Colombia con Spark")
    p.add_argument("--input", default="data/casos_covid.csv", help="Ruta del CSV de entrada")
    p.add_argument("--output", default="output", help="Carpeta de salida (gráficas y tablas)")
    return p.parse_args()


# ----------------------------------------------------------------------------
# 1. Carga
# ----------------------------------------------------------------------------
def cargar(spark, ruta):
    titulo("1. CARGA DEL CONJUNTO DE DATOS")
    # Se lee todo como texto y se castea después, para controlar los datos sucios.
    df = (spark.read.option("header", True)
          .option("multiLine", True)
          .option("escape", '"')
          .csv(ruta))
    df = df.toDF(*[normalize(c) for c in df.columns])
    print("Columnas detectadas:", df.columns)
    df.printSchema()
    return df


# ----------------------------------------------------------------------------
# 2. Limpieza y transformación
# ----------------------------------------------------------------------------
def limpiar(df):
    titulo("2. LIMPIEZA Y TRANSFORMACIÓN")

    c_fecha = pick(df, "fecha_de_notificaci_n", "fecha_de_notificacion", "fecha_reporte_web")
    c_depto = pick(df, "departamento_nom", "nom_departamento", "departamento")
    c_sexo = pick(df, "sexo")
    c_edad = pick(df, "edad")
    c_unidad = pick(df, "unidad_medida")
    c_estado = pick(df, "estado")
    c_recup = pick(df, "recuperado")
    c_muerte = pick(df, "fecha_muerte")
    c_id = pick(df, "id_de_caso", "id_caso")

    faltantes = [n for n, c in [("fecha", c_fecha), ("departamento", c_depto),
                                ("sexo", c_sexo), ("edad", c_edad)] if c is None]
    if faltantes:
        raise SystemExit(f"No se encontraron las columnas: {faltantes}. Columnas: {df.columns}")

    total_inicial = df.count()
    print(f"Registros cargados: {total_inicial:,}")

    # Espacios sobrantes y cadenas vacías -> nulos
    df = df.select([
        F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))).alias(c)
        for c in df.columns
    ])

    # Duplicados por identificador del caso
    if c_id:
        df = df.dropDuplicates([c_id])
        print(f"Registros tras eliminar duplicados: {df.count():,}")

    # Fecha de notificación a tipo fecha (acepta aaaa-mm-dd y dd/mm/aaaa)
    f10 = F.substring(F.col(c_fecha), 1, 10)
    fecha = F.coalesce(F.to_date(f10, "yyyy-MM-dd"), F.to_date(f10, "dd/MM/yyyy"))

    # Edad a años: si la unidad no es años (meses o días) se asume 0 años
    edad = F.col(c_edad).cast("int")
    if c_unidad:
        edad = F.when(F.col(c_unidad) == "1", edad).otherwise(F.lit(0))

    es_fallecido = (F.upper(F.col(c_recup)) == "FALLECIDO") if c_recup else F.lit(False)
    if c_estado:
        es_fallecido = es_fallecido | (F.upper(F.col(c_estado)) == "FALLECIDO")
    if c_muerte:
        es_fallecido = es_fallecido | F.col(c_muerte).isNotNull()

    limpio = df.select(
        fecha.alias("fecha_notificacion"),
        F.upper(F.col(c_depto)).alias("departamento"),
        F.upper(F.col(c_sexo)).alias("sexo"),
        edad.alias("edad"),
        (F.upper(F.col(c_estado)) if c_estado else F.lit(None).cast("string")).alias("estado"),
        F.coalesce(es_fallecido, F.lit(False)).cast("int").alias("fallecido"),
    )

    # Registros válidos: fecha, departamento y sexo conocidos; edad entre 0 y 110
    limpio = limpio.filter(
        F.col("fecha_notificacion").isNotNull()
        & F.col("departamento").isNotNull()
        & F.col("sexo").isin("F", "M")
        & F.col("edad").between(0, 110)
    )

    # Columnas derivadas: mes y grupo de edad
    decena = (F.floor(F.col("edad") / 10) * 10).cast("int")
    limpio = (limpio
              .withColumn("mes", F.date_format("fecha_notificacion", "yyyy-MM"))
              .withColumn("grupo_edad",
                          F.when(F.col("edad") >= 80, "80+")
                          .otherwise(F.format_string("%02d-%02d", decena, decena + 9)))
              .cache())

    total_final = limpio.count()
    print(f"Registros válidos tras la limpieza: {total_final:,}")
    print(f"Registros descartados: {total_inicial - total_final:,}")
    limpio.show(5, truncate=False)
    return limpio


# ----------------------------------------------------------------------------
# 3. Análisis exploratorio
# ----------------------------------------------------------------------------
def analizar(spark, df, out):
    titulo("3. ANÁLISIS EXPLORATORIO (EDA)")

    print("Estadísticas descriptivas de la edad:")
    df.select("edad").describe().show()

    print("Valores nulos por columna:")
    df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

    por_mes = df.groupBy("mes").count().orderBy("mes")
    por_depto = df.groupBy("departamento").count().orderBy(F.desc("count"))
    por_sexo = df.groupBy("sexo").count().orderBy("sexo")
    por_edad = df.groupBy("grupo_edad", "sexo").count().orderBy("grupo_edad", "sexo")
    letal_edad = df.groupBy("grupo_edad").agg(
        F.count("*").alias("casos"),
        F.sum("fallecido").alias("fallecidos"),
        F.round(F.sum("fallecido") * 100 / F.count("*"), 2).alias("letalidad_pct"),
    ).orderBy("grupo_edad")

    print("Casos por departamento (top 10):")
    por_depto.show(10, truncate=False)
    print("Casos por sexo:")
    por_sexo.show()
    print("Letalidad por grupo de edad:")
    letal_edad.show()

    # Spark SQL: letalidad por departamento (departamentos con más de 1000 casos)
    df.createOrReplaceTempView("casos")
    letal_depto = spark.sql("""
        SELECT departamento,
               COUNT(*) AS casos,
               SUM(fallecido) AS fallecidos,
               ROUND(SUM(fallecido) * 100 / COUNT(*), 2) AS letalidad_pct
        FROM casos
        GROUP BY departamento
        HAVING COUNT(*) > 1000
        ORDER BY letalidad_pct DESC
    """)
    print("Letalidad por departamento (Spark SQL, más de 1000 casos):")
    letal_depto.show(10, truncate=False)

    # Ejemplo con RDDs: conteo por departamento con map y reduceByKey
    rdd_top = (df.select("departamento").rdd
               .map(lambda fila: (fila[0], 1))
               .reduceByKey(add)
               .takeOrdered(5, key=lambda kv: -kv[1]))
    print("Top 5 departamentos calculado con RDD (map + reduceByKey):", rdd_top)

    # Resultados pequeños a pandas para graficar y guardar
    pd_mes = por_mes.toPandas()
    pd_depto = por_depto.limit(10).toPandas()
    pd_edad = por_edad.toPandas()
    pd_letal = letal_edad.toPandas()
    pd_letal_depto = letal_depto.toPandas()

    for nombre, tabla in [("casos_por_mes", pd_mes),
                          ("casos_por_departamento_top10", pd_depto),
                          ("casos_por_edad_sexo", pd_edad),
                          ("letalidad_por_edad", pd_letal),
                          ("letalidad_por_departamento", pd_letal_depto)]:
        tabla.to_csv(os.path.join(out, f"{nombre}.csv"), index=False)

    return pd_mes, pd_depto, pd_edad, pd_letal


# ----------------------------------------------------------------------------
# 4. Visualización
# ----------------------------------------------------------------------------
def graficar(pd_mes, pd_depto, pd_edad, pd_letal, out):
    titulo("4. VISUALIZACIÓN")
    figs = os.path.join(out, "figuras")
    os.makedirs(figs, exist_ok=True)

    paso = max(1, len(pd_mes) // 12)
    fig, ax = plt.subplots(figsize=(11, 4.5))
    ax.plot(range(len(pd_mes)), pd_mes["count"], color="#1f77b4", linewidth=2)
    ax.set_title("Casos positivos de COVID-19 por mes de notificación")
    ax.set_xlabel("Mes")
    ax.set_ylabel("Número de casos")
    ax.set_xticks(range(0, len(pd_mes), paso))
    ax.set_xticklabels(pd_mes["mes"].iloc[::paso], rotation=45, ha="right")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(figs, "01_casos_por_mes.png"), dpi=130)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(9, 5))
    d = pd_depto.sort_values("count")
    ax.barh(d["departamento"], d["count"], color="#2ca02c")
    ax.set_title("Diez departamentos con más casos")
    ax.set_xlabel("Número de casos")
    fig.tight_layout()
    fig.savefig(os.path.join(figs, "02_top10_departamentos.png"), dpi=130)
    plt.close(fig)

    piv = pd_edad.pivot(index="grupo_edad", columns="sexo", values="count").fillna(0)
    fig, ax = plt.subplots(figsize=(10, 4.5))
    piv.plot(kind="bar", ax=ax, color={"F": "#d62728", "M": "#1f77b4"})
    ax.set_title("Casos por grupo de edad y sexo")
    ax.set_xlabel("Grupo de edad (años)")
    ax.set_ylabel("Número de casos")
    ax.legend(title="Sexo")
    fig.tight_layout()
    fig.savefig(os.path.join(figs, "03_casos_edad_sexo.png"), dpi=130)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.bar(pd_letal["grupo_edad"], pd_letal["letalidad_pct"], color="#9467bd")
    ax.set_title("Letalidad (%) por grupo de edad")
    ax.set_xlabel("Grupo de edad (años)")
    ax.set_ylabel("Fallecidos por cada 100 casos")
    fig.tight_layout()
    fig.savefig(os.path.join(figs, "04_letalidad_por_edad.png"), dpi=130)
    plt.close(fig)

    print(f"Gráficas guardadas en: {figs}")


def main():
    args = parse_args()
    os.makedirs(args.output, exist_ok=True)

    spark = (SparkSession.builder
             .appName("BatchCovidColombia")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = cargar(spark, args.input)
    limpio = limpiar(df)
    pd_mes, pd_depto, pd_edad, pd_letal = analizar(spark, limpio, args.output)
    graficar(pd_mes, pd_depto, pd_edad, pd_letal, args.output)

    titulo("PROCESO FINALIZADO")
    print(f"Resultados en la carpeta: {os.path.abspath(args.output)}")
    spark.stop()


if __name__ == "__main__":
    main()
