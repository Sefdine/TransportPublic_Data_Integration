# Databricks notebook source
import pandas as pd
import random
from datetime import datetime, timedelta

# Generate data for January 2023
start_date = datetime(2023, 3, 1)
end_date = datetime(2023, 3, 31)
date_generated = [start_date + timedelta(days=x) for x in range(0, (end_date-start_date).days)]

transport_types = ["Bus", "Train", "Tram", "Metro"]
routes = ["Route_" + str(i) for i in range(1, 11)]
stations = ["Station_" + str(i) for i in range(1, 21)]

# Randomly select 5 days as extreme weather days
extreme_weather_days = random.sample(date_generated, 5)

data = []

for date in date_generated:
    for _ in range(32):  # 32 records per day to get a total of 992 records for January
        transport = random.choice(transport_types)
        route = random.choice(routes)

        # Normal operating hours
        departure_hour = random.randint(5, 22)
        departure_minute = random.randint(0, 59)

        # Introducing Unusual Operating Hours for buses
        if transport == "Bus" and random.random() < 0.05:  # 5% chance
            departure_hour = 3

        departure_time = f"{departure_hour:02}:{departure_minute:02}"

        # Normal duration
        duration = random.randint(10, 120)

        # Introducing Short Turnarounds
        if random.random() < 0.05:  # 5% chance
            duration = random.randint(1, 5)

        # General delay
        delay = random.randint(0, 15)

        # Weather Impact
        if date in extreme_weather_days:
            # Increase delay by 10 to 60 minutes
            delay += random.randint(10, 60)

            # 10% chance to change the route
            if random.random() < 0.10:
                route = random.choice(routes)

        total_minutes = departure_minute + duration + delay
        arrival_hour = departure_hour + total_minutes // 60
        arrival_minute = total_minutes % 60
        arrival_time = f"{arrival_hour:02}:{arrival_minute:02}"

        passengers = random.randint(1, 100)
        departure_station = random.choice(stations)
        arrival_station = random.choice(stations)

        data.append([date, transport, route, departure_time, arrival_time, passengers, departure_station, arrival_station, delay])

df = pd.DataFrame(data, columns=["Date", "TransportType", "Route", "DepartureTime", "ArrivalTime", "Passengers", "DepartureStation", "ArrivalStation", "Delay"])
df.to_csv("/dbfs/mnt/staging/raw/public_transport_data-march.csv", index=False)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/staging/raw
# MAGIC
