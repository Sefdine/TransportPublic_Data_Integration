# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Configuration des paramètres de stockage Azure
# MAGIC
# MAGIC Dans cette section, nous configurons les paramètres nécessaires pour accéder au stockage Azure Blob.
# MAGIC
# MAGIC - **Nom du conteneur :** publictransportdata
# MAGIC - **Nom du compte de stockage :** sefdinenassufblobcontain
# MAGIC - **Signature d'Accès Partagé (SAS) :** ?sv=2022-11-02&ss=b&srt=sco&sp=rwdlacyx&se=2023-09-26T18:30:48Z&st=2023-09-26T10:30:48Z&spr=https&sig=nu8xi1VHftfusb2%2Fp%2BLxHRnT4RszWQ3vdS7OgT%2Fo7o8%3D
# MAGIC
# MAGIC Ces paramètres sont essentiels pour établir une connexion sécurisée avec le stockage Azure Blob et accéder aux données stockées dans le conteneur.

# COMMAND ----------

# MAGIC %scala
# MAGIC var containerName = "publictransportdata"
# MAGIC var storageAccountName = "sefdinenassufblobcontain"
# MAGIC var sas = "?sv=2022-11-02&ss=b&srt=sco&sp=rwdlacyx&se=2023-09-26T18:30:48Z&st=2023-09-26T10:30:48Z&spr=https&sig=nu8xi1VHftfusb2%2Fp%2BLxHRnT4RszWQ3vdS7OgT%2Fo7o8%3D"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Configuration de l'URL et du paramètre de stockage Azure
# MAGIC
# MAGIC Dans cette section, nous configurons l'URL et le paramètre nécessaires pour accéder au stockage Azure Blob.

# COMMAND ----------

# MAGIC %scala
# MAGIC var url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
# MAGIC var config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Montage du stockage Azure Blob
# MAGIC
# MAGIC Dans cette section, nous montons le stockage Azure Blob en utilisant les paramètres configurés précédemment.

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.mount(
# MAGIC     source = url,
# MAGIC     mountPoint = "/mnt/staging",
# MAGIC     extraConfigs = Map(config -> sas)
# MAGIC )

# COMMAND ----------

# MAGIC %fs ls /mnt/staging/raw

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformation
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Lecture et Traitement des Données
# MAGIC
# MAGIC Dans cette section, nous effectuons la lecture et le traitement des données en utilisant PySpark dans Azure Databricks.

# COMMAND ----------

from pyspark.sql.functions import col, year, month, date_format, expr, day, udf, avg, sum
from pyspark.sql.types import DateType, IntegerType, StringType, TimestampType

# Read the data
df = spark.read.csv("/mnt/staging/raw/public-transport-data-650c6707b7a50741400514.csv", header=True)

# Define a dictionary to map column names to their data types
column_types = {
    'Date': DateType(),
    'TransportType': StringType(),
    'Route': StringType(),
    'Passengers': IntegerType(),
    'DepartureStation': StringType(),
    'ArrivalStation': StringType(),
    'Delay': IntegerType()
}

# Cast the columns to their respective data types
for col_name, col_type in column_types.items():
    df = df.withColumn(col_name, col(col_name).cast(col_type))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Ajout de Colonnes pour l'Année, le Mois et le Jour
# MAGIC
# MAGIC Dans cette section, nous ajoutons trois nouvelles colonnes à notre DataFrame `df` pour représenter l'année, le mois et le jour à partir de la colonne 'Date'.

# COMMAND ----------

# Create hierarchy of the date
df = df.withColumn('Year', year(df['Date']))
df = df.withColumn('Month', month(df['Date']))
df = df.withColumn('Day', day(df['Date']))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Transformation des Heures et Calcul du Délai
# MAGIC
# MAGIC Dans cette section, nous effectuons la transformation des heures d'arrivée et calculons le délai entre l'heure de départ et l'heure d'arrivée dans notre DataFrame `df`.

# COMMAND ----------

# Define function to deal with the arrival times
def transform_time(arrival_time):
    parts = arrival_time.split(':')
    hours = int(parts[0])
    minutes = int(parts[1])

    # Check if minutes exceed 59
    if minutes > 59:
        # Set minutes to 00
        hours += 1
        minutes = 0

    # Check if hours exceed 23
    if hours > 23:
        # Set hours to 00
        hours = 0
        
    return f'{hours:02d}:{minutes:02d}'

# COMMAND ----------

# Register the transform_time function as a UDF
transform_time_udf = udf(transform_time, StringType())

# Apply the functions in the dataset
df = df.withColumn("ArrivalTime", transform_time_udf(df['ArrivalTime']))

# Calculate delay between departTime and arrivalTime
df = df.withColumn("CurrentDelay", expr(
    "from_unixtime(unix_timestamp(ArrivalTime, 'HH:mm') - unix_timestamp(DepartureTime, 'HH:mm'), 'HH:mm')"
))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Catégorisation des Retards
# MAGIC
# MAGIC Dans cette section, nous effectuons la catégorisation des retards en fonction de la valeur du délai et ajoutons une nouvelle colonne 'CategoryLate' à notre DataFrame `df`.

# COMMAND ----------

# Define category_late function to deal with times
def category_late(currentDelay):
    parts = currentDelay.split(':')
    hours = int(parts[0])
    minutes = int(parts[1])

    if minutes < 1: 
        return 'Pas de Retard'
    elif minutes <= 10:
        return 'Retard Court'
    elif minutes <= 20:
        return 'Retard Moyen'
    else:
        return 'Long Retard'

# Transform the function we created to user-defined function
category_late_udf = udf(category_late, StringType())

# Apply the function the dataset
df = df.withColumn('CategoryLate', category_late_udf(df['CurrentDelay']))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyse des Passagers
# MAGIC Identifier les heures de pointe et hors pointe en fonction du nombre de passagers.

# COMMAND ----------

# Define function to guess the peak hours
def peak_hours(passengers):
    passengers = int(passengers)

    if passengers >= 50:
        return 'Heure de pointe'
    else:
        return 'Heure hors pointe'
    
peak_hours_udf = udf(peak_hours, StringType())

# Apply the function to our dataset
df = df.withColumn('PeakHours', peak_hours_udf(df['Passengers']))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Analyse des itinéraires
# MAGIC
# MAGIC Dans cette section, nous effectuons l'agrégation des données en fonction de la colonne 'Route' et rejoignons le DataFrame agrégé 'result_df' avec le DataFrame d'origine 'df'.
# MAGIC
# MAGIC On calculer le retard moyen, le nombre moyen de passagers et le nombre total de voyages pour chaque itinéraire.
# MAGIC

# COMMAND ----------

# Agrégation des données par la colonne 'Route' avec calcul de la moyenne du retard, de la moyenne des passagers, et de la somme du retard
result_df = df.groupBy('Route').agg(
    avg('Delay').alias('AverageDelay'),
    avg('Passengers').alias('AveragePassengers'),
    sum('Delay').alias('TotalTrips')
)

# Join the aggregated DataFrame 'result_df' back to the original DataFrame 'df'
df = df.join(result_df, on='Route', how='left')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Enregistrement du DataFrame dans un Fichier CSV
# MAGIC
# MAGIC Dans cette section, nous enregistrons le DataFrame `df` dans un fichier CSV.
# MAGIC

# COMMAND ----------

import pandas as pd

# Convert the Spark DataFrame to a pandas DataFrame
pandas_df = df.toPandas()

# Save the pandas DataFrame as a CSV file
pandas_df.to_csv('/dbfs/mnt/staging/processed/data_cleaned.csv', index=False)
