# Databricks notebook source
# import pandas as pd
# import random
# from datetime import datetime, timedelta

# # Generate data for January 2023
# start_date = datetime(2023, 1, 1)
# end_date = datetime(2023, 1, 31)
# date_generated = [start_date + timedelta(days=x) for x in range(0, (end_date-start_date).days)]

# transport_types = ["Bus", "Train", "Tram", "Metro"]
# routes = ["Route_" + str(i) for i in range(1, 11)]
# stations = ["Station_" + str(i) for i in range(1, 21)]

# # Randomly select 5 days as extreme weather days
# extreme_weather_days = random.sample(date_generated, 5)

# data = []

# for date in date_generated:
#     for _ in range(32):  # 32 records per day to get a total of 992 records for January
#         transport = random.choice(transport_types)
#         route = random.choice(routes)

#         # Normal operating hours
#         departure_hour = random.randint(5, 22)
#         departure_minute = random.randint(0, 59)

#         # Introducing Unusual Operating Hours for buses
#         if transport == "Bus" and random.random() < 0.05:  # 5% chance
#             departure_hour = 3

#         departure_time = f"{departure_hour:02}:{departure_minute:02}"

#         # Normal duration
#         duration = random.randint(10, 120)

#         # Introducing Short Turnarounds
#         if random.random() < 0.05:  # 5% chance
#             duration = random.randint(1, 5)

#         # General delay
#         delay = random.randint(0, 15)

#         # Weather Impact
#         if date in extreme_weather_days:
#             # Increase delay by 10 to 60 minutes
#             delay += random.randint(10, 60)

#             # 10% chance to change the route
#             if random.random() < 0.10:
#                 route = random.choice(routes)

#         total_minutes = departure_minute + duration + delay
#         arrival_hour = departure_hour + total_minutes // 60
#         arrival_minute = total_minutes % 60
#         arrival_time = f"{arrival_hour:02}:{arrival_minute:02}"

#         passengers = random.randint(1, 100)
#         departure_station = random.choice(stations)
#         arrival_station = random.choice(stations)

#         data.append([date, transport, route, departure_time, arrival_time, passengers, departure_station, arrival_station, delay])

# df = pd.DataFrame(data, columns=["Date", "TransportType", "Route", "DepartureTime", "ArrivalTime", "Passengers", "DepartureStation", "ArrivalStation", "Delay"])
# df.to_csv("public_transport_data.csv", index=False)


# COMMAND ----------

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
# MAGIC ## Date
# MAGIC Extraire le year, month et day of the month

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Ajout de Colonnes pour l'Année, le Mois et le Jour
# MAGIC
# MAGIC Dans cette section, nous ajoutons trois nouvelles colonnes à notre DataFrame `df` pour représenter l'année, le mois et le jour à partir de la colonne 'Date'.

# COMMAND ----------

df = df.withColumn('Year', year(df['Date']))
df = df.withColumn('Month', month(df['Date']))
df = df.withColumn('Day', day(df['Date']))

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.head(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Durée de voyage
# MAGIC Soustraire l'heure de départ et l'heure d'arrivée

# COMMAND ----------

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

# COMMAND ----------

# Calculate delay between departTime and arrivalTime
df = df.withColumn("CurrentDelay", expr(
    "from_unixtime(unix_timestamp(ArrivalTime, 'HH:mm') - unix_timestamp(DepartureTime, 'HH:mm'), 'HH:mm')"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## RetardCategory
# MAGIC 'Pas de Retard', 'Retard Court' (1-10 minutes), 'Retard Moyen' (11-20 minutes) et 'Long Retard' (>20 minutes).

# COMMAND ----------

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
    
category_late_udf = udf(category_late, StringType())

# COMMAND ----------

df = df.withColumn('CategoryLate', category_late_udf(df['CurrentDelay']))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyse des Passagers
# MAGIC Identifier les heures de pointe et hors pointe en fonction du nombre de passagers.

# COMMAND ----------

def peak_hours(passengers):
    passengers = int(passengers)

    if passengers >= 50:
        return 'Heure de pointe'
    else:
        return 'Heure hors pointe'
    
peak_hours_udf = udf(peak_hours, StringType())


# COMMAND ----------

df = df.withColumn('PeakHours', peak_hours_udf(df['Passengers']))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyse des Itinéraires
# MAGIC Calculer le retard moyen, le nombre moyen de passagers et le nombre total de voyages pour chaque itinéraire.

# COMMAND ----------

result_df = df.groupBy('Route').agg(
    avg('Delay').alias('AverageDelay'),
    avg('Passengers').alias('AveragePassengers'),
    sum('Delay').alias('TotalTrips')
)

# Join the aggregated DataFrame 'result_df' back to the original DataFrame 'df'
df = df.join(result_df, on='Route', how='left')

# COMMAND ----------

display(df)

# COMMAND ----------

# Save the DataFrame to a CSV file
df.write.csv('/mnt/staging2/processed/data_cleaned.csv', header=True)

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/staging2/processed
