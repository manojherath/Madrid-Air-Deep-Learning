#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
import json
import random
import os


# ### Read Raw Data

# In[3]:


# Read data from Excel
air_jan = pd.read_csv('data/air_data/ene_mo24.csv', sep=';')
air_feb = pd.read_csv('data/air_data/feb_mo24.csv',sep=';')
air_mar = pd.read_csv('data/air_data/mar_mo24.csv',sep=';')
air_apr = pd.read_csv('data/air_data/abr_mo24.csv',sep=';')


# In[ ]:


air_jan["PUNTO_MUESTREO"].unique()


# ### Function to convert to the format of |Date|Station|Time|Data|

# In[4]:


def reshape_air_quality_data(df):
    # Rename columns temporarily for pd.to_datetime()
    df = df.rename(columns={"ANO": "year", "MES": "month", "DIA": "day"})
    
    # Convert year, month, and day to a single Date column
    df["Date"] = pd.to_datetime(df[['year', 'month', 'day']])
    
    # Select necessary columns
    id_vars = ['Date', 'PUNTO_MUESTREO']
    value_vars = [f"H{i:02d}" for i in range(1, 25)]  # Columns H01 to H24
    
    # Melt the DataFrame
    df_melted = df.melt(id_vars=id_vars, value_vars=value_vars, 
                         var_name="Time", value_name="Data")
    
    # Extract and format hour from column names (H01 -> 01:00)
    df_melted["Time"] = df_melted["Time"].str.extract(r'(\d+)')[0].astype(int).map(lambda x: f"{x:02d}:00")
    
    # Rename columns
    df_melted.rename(columns={'PUNTO_MUESTREO': 'Station'}, inplace=True)

    # Reorder columns
    df_melted = df_melted[['Date', 'Station', 'Time', 'Data']]
    
    # **Sort by Station, Date, and Time**
    df_melted = df_melted.sort_values(by=['Station', 'Date', 'Time']).reset_index(drop=True)
    
    return df_melted


# In[ ]:


air_jan_formatted = reshape_air_quality_data(air_jan)
air_feb_formatted = reshape_air_quality_data(air_feb)
air_mar_formatted = reshape_air_quality_data(air_mar)
air_apr_formatted = reshape_air_quality_data(air_apr)
air_jan_formatted


# In[14]:


air_apr_formatted


# In[19]:


air_apr_formatted["Station"].unique().size


# ### Function to Convert the Data Frame to the NGSI-LD Data Format
# - convert data frame to ngsild format
# - save as json

# In[60]:


def convert_to_ngsild(df, month):
    # Create a list to store the entities
    entities = []
    
    # Helper function to generate a random 10-digit datasetId
    def generate_dataset_id():
        return f"urn:ngsi-ld:{random.randint(1000000000, 9999999999)}"

    # Iterate over each unique station in the DataFrame
    for station in df['Station'].unique():
        station_df = df[df['Station'] == station]

        # Extract the first part of the station code
        station_base = station.split("_")[0]  # Extracts '28079004' from '28079004_12_8'

        # Create flow data 
        data = []
        for _, row in station_df.iterrows():
            # Handle the Date and Time formatting
            if isinstance(row['Date'], pd.Timestamp):
                date_value = row['Date']
            else:
                date_value = pd.to_datetime(row['Date'])

            time_str = row['Time'].strip()

            # Check if time is "24:00" and convert to "00:00" of the next day
            if time_str == "24:00":
                date_value += pd.Timedelta(days=1)
                time_str = "00:00"

            # Ensure date is in 'YYYY-MM-DD' format
            date_str = date_value.strftime('%Y-%m-%d')

            observed_at = f"{date_str}T{time_str}:00Z"  # Combine date and time correctly
            data.append({
                "type": "Property",
                "observedAt": observed_at,
                "datasetId": generate_dataset_id(),  # Add the random datasetId
                "value": row['Data'],
                "unitCode": "GQ"
            })

        # Create an entity using station_base
        entity_1 = {
            "id": f"urn:ngsi-ld:AirQualityObserved:{station_base}",  # Using '28079004'
            "type": "AirQualityObserved",
            "refRoad": {
                "type": "Relationship",
                "object": f"urn:ngsi-ld:Road:{station_base}"  # Using '28079004'
            },
            "temporalResolution": {
                "type": "Property",
                "value": "PT1H"
            },
            "nox": data,
            "@context": [
                "https://easy-global-market.github.io/c2jn-data-models/jsonld-contexts/c2jn-compound.jsonld"
            ]
        }

        entities.append(entity_1)

        # Save the JSON output to a file
        with open(f"data_air_json/{month}/air_quality_observed_{station}_{month}.json", 'w') as json_file:
            json.dump(entities, json_file, indent=4)

    return entities


# In[ ]:


air_jan_formatted["Station"].unique()


# In[8]:


stations_so2    = ['28079008_1_38', '28079035_1_38', '28079036_1_38']
stations_co     = ['28079004_6_48', '28079008_6_48', '28079035_6_48', '28079056_6_48']

stations_tol    = ['28079008_20_59', '28079011_20_59', '28079018_20_59', '28079024_20_59', '28079038_20_59', '28079055_20_59']
stations_ben    = ['28079008_30_59', '28079011_30_59', '28079018_30_59', '28079024_30_59', '28079038_30_59', '28079055_30_59']
stations_ebe    = ['28079008_35_59', '28079011_35_59', '28079018_35_59', '28079024_35_59', '28079038_35_59', '28079055_35_59']

stations_pm25   = ['28079008_9_47', '28079024_9_47', '28079038_9_47', '28079047_9_47', '28079048_9_47', '28079050_9_47', '28079056_9_47', '28079057_9_47']
stations_pm10 = ['28079008_10_47', '28079018_10_47', '28079024_10_47', '28079036_10_47', '28079038_10_47', '28079040_10_47', '28079047_10_47', '28079048_10_47',
                 '28079050_10_47', '28079055_10_47', '28079056_10_47', '28079057_10_47', '28079060_10_47']
stations_o3 = ['28079008_14_6', '28079016_14_6', '28079017_14_6', '28079018_14_6', '28079024_14_6', '28079027_14_6', '28079035_14_6', '28079039_14_6',
               '28079049_14_6', '28079054_14_6', '28079058_14_6', '28079059_14_6', '28079060_14_6']

stations_no     = ['28079004_7_8', '28079008_7_8', '28079011_7_8', '28079016_7_8', '28079017_7_8', '28079018_7_8', '28079024_7_8', '28079027_7_8' , 
                   '28079035_7_8', '28079036_7_8', '28079038_7_8', '28079039_7_8', '28079040_7_8', '28079047_7_8', '28079048_7_8', '28079049_7_8' ,
                   '28079050_7_8', '28079054_7_8', '28079055_7_8', '28079056_7_8', '28079057_7_8', '28079058_7_8', '28079059_7_8', '28079060_7_8' ]
stations_no2    = ['28079004_8_8', '28079008_8_8', '28079011_8_8', '28079016_8_8', '28079017_8_8', '28079018_8_8', '28079024_8_8', '28079027_8_8', 
                   '28079035_8_8', '28079036_8_8', '28079038_8_8', '28079039_8_8', '28079040_8_8', '28079047_8_8', '28079048_8_8', '28079049_8_8', 
                   '28079050_8_8', '28079054_8_8', '28079055_8_8', '28079056_8_8', '28079057_8_8', '28079058_8_8', '28079059_8_8', '28079060_8_8']
stations_nox    = ['28079004_12_8', '28079008_12_8', '28079011_12_8', '28079016_12_8', '28079017_12_8', '28079018_12_8', '28079024_12_8', '28079027_12_8',
                   '28079035_12_8', '28079036_12_8', '28079038_12_8', '28079039_12_8', '28079040_12_8', '28079047_12_8', '28079048_12_8', '28079049_12_8',
                   '28079050_12_8', '28079054_12_8', '28079055_12_8', '28079056_12_8', '28079057_12_8', '28079058_12_8', '28079059_12_8', '28079060_12_8']


# ### Function to find Stations and there Gases 

# In[ ]:


# Create a dictionary to map gases to their respective stations
gas_to_stations = {
    'SO2': stations_so2,
    'CO': stations_co,
    'TOL': stations_tol,
    'BEN': stations_ben,
    'EBE': stations_ebe,
    'PM2.5': stations_pm25,
    'PM10': stations_pm10,
    'O3': stations_o3,
    'NO': stations_no,
    'NO2': stations_no2,
    'NOX': stations_nox
}

# Create a dictionary to map each station to the gases it measures
station_to_gases = {}

# Populate the station_to_gases dictionary
for gas, stations in gas_to_stations.items():
    for station in stations:
        # Extract the station name (first part of the string)
        station_name = station.split('_')[0]
        if station_name not in station_to_gases:
            station_to_gases[station_name] = set()  # Use a set to avoid duplicate gases
        station_to_gases[station_name].add(gas)

# Convert sets to lists for better readability
station_to_gases = {station: sorted(gases) for station, gases in station_to_gases.items()}

# Print the result
for station, gases in station_to_gases.items():
    print(f"{station}: {', '.join(gases)}")


# #### Selected Stations
# - 28079008
# - 28079018
# - 28079024
# - 28079060

# ### Jan

# In[55]:


air_jan_formatted_so2 = air_jan_formatted[air_jan_formatted["Station"].isin(stations_so2)]
air_jan_formatted_co = air_jan_formatted[air_jan_formatted["Station"].isin(stations_co)]
air_jan_formatted_tol = air_jan_formatted[air_jan_formatted["Station"].isin(stations_tol)]
air_jan_formatted_ben = air_jan_formatted[air_jan_formatted["Station"].isin(stations_ben)]
air_jan_formatted_ebe = air_jan_formatted[air_jan_formatted["Station"].isin(stations_ebe)]
air_jan_formatted_pm25 = air_jan_formatted[air_jan_formatted["Station"].isin(stations_pm25)]
air_jan_formatted_pm10 = air_jan_formatted[air_jan_formatted["Station"].isin(stations_pm10)]
air_jan_formatted_o3 = air_jan_formatted[air_jan_formatted["Station"].isin(stations_o3)]
air_jan_formatted_no = air_jan_formatted[air_jan_formatted["Station"].isin(stations_no)]
air_jan_formatted_no2 = air_jan_formatted[air_jan_formatted["Station"].isin(stations_no2)]
air_jan_formatted_nox = air_jan_formatted[air_jan_formatted["Station"].isin(stations_nox)]


# ### Feb

# In[11]:


air_feb_formatted_so2 = air_feb_formatted[air_feb_formatted["Station"].isin(stations_so2)]
air_feb_formatted_co = air_feb_formatted[air_feb_formatted["Station"].isin(stations_co)]
air_feb_formatted_tol = air_feb_formatted[air_feb_formatted["Station"].isin(stations_tol)]
air_feb_formatted_ben = air_feb_formatted[air_feb_formatted["Station"].isin(stations_ben)]
air_feb_formatted_ebe = air_feb_formatted[air_feb_formatted["Station"].isin(stations_ebe)]
air_feb_formatted_pm25 = air_feb_formatted[air_feb_formatted["Station"].isin(stations_pm25)]
air_feb_formatted_pm10 = air_feb_formatted[air_feb_formatted["Station"].isin(stations_pm10)]
air_feb_formatted_o3 = air_feb_formatted[air_feb_formatted["Station"].isin(stations_o3)]
air_feb_formatted_no = air_feb_formatted[air_feb_formatted["Station"].isin(stations_no)]
air_feb_formatted_no2 = air_feb_formatted[air_feb_formatted["Station"].isin(stations_no2)]
air_feb_formatted_nox = air_feb_formatted[air_feb_formatted["Station"].isin(stations_nox)]


# ### Mar

# In[12]:


air_mar_formatted_so2 = air_mar_formatted[air_mar_formatted["Station"].isin(stations_so2)]
air_mar_formatted_co = air_mar_formatted[air_mar_formatted["Station"].isin(stations_co)]
air_mar_formatted_tol = air_mar_formatted[air_mar_formatted["Station"].isin(stations_tol)]
air_mar_formatted_ben = air_mar_formatted[air_mar_formatted["Station"].isin(stations_ben)]
air_mar_formatted_ebe = air_mar_formatted[air_mar_formatted["Station"].isin(stations_ebe)]
air_mar_formatted_pm25 = air_mar_formatted[air_mar_formatted["Station"].isin(stations_pm25)]
air_mar_formatted_pm10 = air_mar_formatted[air_mar_formatted["Station"].isin(stations_pm10)]
air_mar_formatted_o3 = air_mar_formatted[air_mar_formatted["Station"].isin(stations_o3)]
air_mar_formatted_no = air_mar_formatted[air_mar_formatted["Station"].isin(stations_no)]
air_mar_formatted_no2 = air_mar_formatted[air_mar_formatted["Station"].isin(stations_no2)]
air_mar_formatted_nox = air_mar_formatted[air_mar_formatted["Station"].isin(stations_nox)]


# ### Apr

# In[13]:


air_apr_formatted_so2 = air_apr_formatted[air_apr_formatted["Station"].isin(stations_so2)]
air_apr_formatted_co = air_apr_formatted[air_apr_formatted["Station"].isin(stations_co)]
air_apr_formatted_tol = air_apr_formatted[air_apr_formatted["Station"].isin(stations_tol)]
air_apr_formatted_ben = air_apr_formatted[air_apr_formatted["Station"].isin(stations_ben)]
air_apr_formatted_ebe = air_apr_formatted[air_apr_formatted["Station"].isin(stations_ebe)]
air_apr_formatted_pm25 = air_apr_formatted[air_apr_formatted["Station"].isin(stations_pm25)]
air_apr_formatted_pm10 = air_apr_formatted[air_apr_formatted["Station"].isin(stations_pm10)]
air_apr_formatted_o3 = air_apr_formatted[air_apr_formatted["Station"].isin(stations_o3)]
air_apr_formatted_no = air_apr_formatted[air_apr_formatted["Station"].isin(stations_no)]
air_apr_formatted_no2 = air_apr_formatted[air_apr_formatted["Station"].isin(stations_no2)]
air_apr_formatted_nox = air_apr_formatted[air_apr_formatted["Station"].isin(stations_nox)]


# In[14]:


air_apr_formatted_no


# In[56]:


air_jan_formatted_co


# In[ ]:


#convert_to_ngsild(air_jan_formatted_co, "jan")
#convert_to_ngsild(air_feb_formatted_co, "feb")
#convert_to_ngsild(air_mar_formatted_co, "mar")
#convert_to_ngsild(air_apr_formatted_co, "apr")


# In[ ]:


#convert_to_ngsild(air_jan_formatted_o3, "jan")
#convert_to_ngsild(air_feb_formatted_o3, "feb")
#convert_to_ngsild(air_mar_formatted_o3, "mar")
#convert_to_ngsild(air_apr_formatted_o3, "apr")


# In[ ]:


#convert_to_ngsild(air_jan_formatted_no, "jan")
#convert_to_ngsild(air_feb_formatted_no, "feb")
#convert_to_ngsild(air_mar_formatted_no, "mar")
#convert_to_ngsild(air_apr_formatted_no, "apr")


# In[ ]:


#convert_to_ngsild(air_jan_formatted_no2, "jan")
#convert_to_ngsild(air_feb_formatted_no2, "feb")
#convert_to_ngsild(air_mar_formatted_no2, "mar")
#convert_to_ngsild(air_apr_formatted_no2, "apr")


# In[62]:


#convert_to_ngsild(air_jan_formatted_nox, "jan")
#convert_to_ngsild(air_feb_formatted_nox, "feb")
#convert_to_ngsild(air_mar_formatted_nox, "mar")
#convert_to_ngsild(air_apr_formatted_nox, "apr")


# ### Function to Read JSON Data

# In[53]:


# Read traffic flow data
def read_air_data(json_file_path, gas):
    # Step 1: Read the JSON data from the file
    with open(json_file_path, 'r') as json_file:
        data = json.load(json_file)

    # Step 2: Find the entity data for the specified direction
    flow_data = None
    for entity in data:
        flow_data = entity.get(gas,[])

    # Step 3: Convert the flow data to a pandas DataFrame
    df = pd.DataFrame(flow_data)
    df['observedAt'] = pd.to_datetime(df['observedAt'])
    df = df.sort_values('observedAt')

    return df


# In[ ]:


#print data read code
for e in range(1, 61):  # Loop for entity numbers 1 to 60
    for d in range(1, 3):  # Loop for directions 1 and 2
        print(f'df_mar_e{e:02}_dir_{d}_train = read_air_data("data_air_json/jan/urn_ngsi-ld_TrafficFlowObserved_ES{e:02}_jan.json")')


# ### NO

# In[ ]:


for stations in stations_no:
    print(f'df_mar_{stations.split("_")[0]}_no_train = read_air_data("data_air_json/mar/air_quality_observed_{stations}_mar.json", gas="no")')


# In[45]:


df_jan_28079004_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079004_7_8_jan.json", gas="no")
df_jan_28079008_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079008_7_8_jan.json", gas="no")
df_jan_28079011_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079011_7_8_jan.json", gas="no")
df_jan_28079016_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079016_7_8_jan.json", gas="no")
df_jan_28079017_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079017_7_8_jan.json", gas="no")
df_jan_28079018_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079018_7_8_jan.json", gas="no")
df_jan_28079024_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079024_7_8_jan.json", gas="no")
df_jan_28079027_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079027_7_8_jan.json", gas="no")
df_jan_28079035_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079035_7_8_jan.json", gas="no")
df_jan_28079036_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079036_7_8_jan.json", gas="no")
df_jan_28079038_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079038_7_8_jan.json", gas="no")
df_jan_28079039_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079039_7_8_jan.json", gas="no")
df_jan_28079040_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079040_7_8_jan.json", gas="no")
df_jan_28079047_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079047_7_8_jan.json", gas="no")
df_jan_28079048_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079048_7_8_jan.json", gas="no")
df_jan_28079049_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079049_7_8_jan.json", gas="no")
df_jan_28079050_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079050_7_8_jan.json", gas="no")
df_jan_28079054_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079054_7_8_jan.json", gas="no")
df_jan_28079055_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079055_7_8_jan.json", gas="no")
df_jan_28079056_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079056_7_8_jan.json", gas="no")
df_jan_28079057_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079057_7_8_jan.json", gas="no")
df_jan_28079058_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079058_7_8_jan.json", gas="no")
df_jan_28079059_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079059_7_8_jan.json", gas="no")
df_jan_28079060_no_train = read_air_data("data_air_json/jan/air_quality_observed_28079060_7_8_jan.json", gas="no")


df_feb_28079004_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079004_7_8_feb.json", gas="no")
df_feb_28079008_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079008_7_8_feb.json", gas="no")
df_feb_28079011_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079011_7_8_feb.json", gas="no")
df_feb_28079016_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079016_7_8_feb.json", gas="no")
df_feb_28079017_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079017_7_8_feb.json", gas="no")
df_feb_28079018_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079018_7_8_feb.json", gas="no")
df_feb_28079024_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079024_7_8_feb.json", gas="no")
df_feb_28079027_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079027_7_8_feb.json", gas="no")
df_feb_28079035_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079035_7_8_feb.json", gas="no")
df_feb_28079036_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079036_7_8_feb.json", gas="no")
df_feb_28079038_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079038_7_8_feb.json", gas="no")
df_feb_28079039_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079039_7_8_feb.json", gas="no")
df_feb_28079040_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079040_7_8_feb.json", gas="no")
df_feb_28079047_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079047_7_8_feb.json", gas="no")
df_feb_28079048_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079048_7_8_feb.json", gas="no")
df_feb_28079049_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079049_7_8_feb.json", gas="no")
df_feb_28079050_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079050_7_8_feb.json", gas="no")
df_feb_28079054_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079054_7_8_feb.json", gas="no")
df_feb_28079055_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079055_7_8_feb.json", gas="no")
df_feb_28079056_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079056_7_8_feb.json", gas="no")
df_feb_28079057_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079057_7_8_feb.json", gas="no")
df_feb_28079058_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079058_7_8_feb.json", gas="no")
df_feb_28079059_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079059_7_8_feb.json", gas="no")
df_feb_28079060_no_train = read_air_data("data_air_json/feb/air_quality_observed_28079060_7_8_feb.json", gas="no")

df_mar_28079004_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079004_7_8_mar.json", gas="no")
df_mar_28079008_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079008_7_8_mar.json", gas="no")
df_mar_28079011_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079011_7_8_mar.json", gas="no")
df_mar_28079016_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079016_7_8_mar.json", gas="no")
df_mar_28079017_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079017_7_8_mar.json", gas="no")
df_mar_28079018_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079018_7_8_mar.json", gas="no")
df_mar_28079024_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079024_7_8_mar.json", gas="no")
df_mar_28079027_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079027_7_8_mar.json", gas="no")
df_mar_28079035_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079035_7_8_mar.json", gas="no")
df_mar_28079036_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079036_7_8_mar.json", gas="no")
df_mar_28079038_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079038_7_8_mar.json", gas="no")
df_mar_28079039_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079039_7_8_mar.json", gas="no")
df_mar_28079040_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079040_7_8_mar.json", gas="no")
df_mar_28079047_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079047_7_8_mar.json", gas="no")
df_mar_28079048_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079048_7_8_mar.json", gas="no")
df_mar_28079049_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079049_7_8_mar.json", gas="no")
df_mar_28079050_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079050_7_8_mar.json", gas="no")
df_mar_28079054_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079054_7_8_mar.json", gas="no")
df_mar_28079055_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079055_7_8_mar.json", gas="no")
df_mar_28079056_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079056_7_8_mar.json", gas="no")
df_mar_28079057_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079057_7_8_mar.json", gas="no")
df_mar_28079058_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079058_7_8_mar.json", gas="no")
df_mar_28079059_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079059_7_8_mar.json", gas="no")
df_mar_28079060_no_train = read_air_data("data_air_json/mar/air_quality_observed_28079060_7_8_mar.json", gas="no")


# Concat Train Data
df_28079004_no_train = pd.concat([df_jan_28079004_no_train, df_feb_28079004_no_train, df_mar_28079004_no_train], ignore_index = 'True')
df_28079016_no_train = pd.concat([df_jan_28079016_no_train, df_feb_28079016_no_train, df_mar_28079016_no_train], ignore_index = 'True')
df_28079017_no_train = pd.concat([df_jan_28079017_no_train, df_feb_28079017_no_train, df_mar_28079017_no_train], ignore_index = 'True')
df_28079027_no_train = pd.concat([df_jan_28079027_no_train, df_feb_28079027_no_train, df_mar_28079027_no_train], ignore_index = 'True')
df_28079039_no_train = pd.concat([df_jan_28079039_no_train, df_feb_28079039_no_train, df_mar_28079039_no_train], ignore_index = 'True')
df_28079049_no_train = pd.concat([df_jan_28079049_no_train, df_feb_28079049_no_train, df_mar_28079049_no_train], ignore_index = 'True')
df_28079054_no_train = pd.concat([df_jan_28079054_no_train, df_feb_28079054_no_train, df_mar_28079054_no_train], ignore_index = 'True')
df_28079058_no_train = pd.concat([df_jan_28079058_no_train, df_feb_28079058_no_train, df_mar_28079058_no_train], ignore_index = 'True')
df_28079059_no_train = pd.concat([df_jan_28079059_no_train, df_feb_28079059_no_train, df_mar_28079059_no_train], ignore_index = 'True')


# Test Data
df_28079004_no_test = read_air_data("data_air_json/apr/air_quality_observed_28079004_7_8_apr.json", gas="no").iloc[0:192]
df_28079016_no_test = read_air_data("data_air_json/apr/air_quality_observed_28079016_7_8_apr.json", gas="no").iloc[0:192]
df_28079017_no_test = read_air_data("data_air_json/apr/air_quality_observed_28079017_7_8_apr.json", gas="no").iloc[0:192]
df_28079027_no_test = read_air_data("data_air_json/apr/air_quality_observed_28079027_7_8_apr.json", gas="no").iloc[0:192]
df_28079039_no_test = read_air_data("data_air_json/apr/air_quality_observed_28079039_7_8_apr.json", gas="no").iloc[0:192]
df_28079049_no_test = read_air_data("data_air_json/apr/air_quality_observed_28079049_7_8_apr.json", gas="no").iloc[0:192]
df_28079054_no_test = read_air_data("data_air_json/apr/air_quality_observed_28079054_7_8_apr.json", gas="no").iloc[0:192]
df_28079058_no_test = read_air_data("data_air_json/apr/air_quality_observed_28079058_7_8_apr.json", gas="no").iloc[0:192]
df_28079059_no_test = read_air_data("data_air_json/apr/air_quality_observed_28079059_7_8_apr.json", gas="no").iloc[0:192]


# ### NO2

# In[ ]:


for stations in stations_no2:
    print(f'df_mar_{stations.split("_")[0]}_no2_train = read_air_data("data_air_json/mar/air_quality_observed_{stations}_mar.json", gas="no2")')


# In[46]:


df_jan_28079004_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079004_8_8_jan.json", gas="no2")
df_jan_28079008_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079008_8_8_jan.json", gas="no2")
df_jan_28079011_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079011_8_8_jan.json", gas="no2")
df_jan_28079016_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079016_8_8_jan.json", gas="no2")
df_jan_28079017_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079017_8_8_jan.json", gas="no2")
df_jan_28079018_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079018_8_8_jan.json", gas="no2")
df_jan_28079024_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079024_8_8_jan.json", gas="no2")
df_jan_28079027_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079027_8_8_jan.json", gas="no2")
df_jan_28079035_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079035_8_8_jan.json", gas="no2")
df_jan_28079036_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079036_8_8_jan.json", gas="no2")
df_jan_28079038_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079038_8_8_jan.json", gas="no2")
df_jan_28079039_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079039_8_8_jan.json", gas="no2")
df_jan_28079040_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079040_8_8_jan.json", gas="no2")
df_jan_28079047_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079047_8_8_jan.json", gas="no2")
df_jan_28079048_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079048_8_8_jan.json", gas="no2")
df_jan_28079049_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079049_8_8_jan.json", gas="no2")
df_jan_28079050_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079050_8_8_jan.json", gas="no2")
df_jan_28079054_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079054_8_8_jan.json", gas="no2")
df_jan_28079055_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079055_8_8_jan.json", gas="no2")
df_jan_28079056_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079056_8_8_jan.json", gas="no2")
df_jan_28079057_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079057_8_8_jan.json", gas="no2")
df_jan_28079058_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079058_8_8_jan.json", gas="no2")
df_jan_28079059_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079059_8_8_jan.json", gas="no2")
df_jan_28079060_no2_train = read_air_data("data_air_json/jan/air_quality_observed_28079060_8_8_jan.json", gas="no2")

df_feb_28079004_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079004_8_8_feb.json", gas="no2")
df_feb_28079008_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079008_8_8_feb.json", gas="no2")
df_feb_28079011_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079011_8_8_feb.json", gas="no2")
df_feb_28079016_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079016_8_8_feb.json", gas="no2")
df_feb_28079017_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079017_8_8_feb.json", gas="no2")
df_feb_28079018_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079018_8_8_feb.json", gas="no2")
df_feb_28079024_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079024_8_8_feb.json", gas="no2")
df_feb_28079027_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079027_8_8_feb.json", gas="no2")
df_feb_28079035_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079035_8_8_feb.json", gas="no2")
df_feb_28079036_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079036_8_8_feb.json", gas="no2")
df_feb_28079038_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079038_8_8_feb.json", gas="no2")
df_feb_28079039_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079039_8_8_feb.json", gas="no2")
df_feb_28079040_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079040_8_8_feb.json", gas="no2")
df_feb_28079047_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079047_8_8_feb.json", gas="no2")
df_feb_28079048_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079048_8_8_feb.json", gas="no2")
df_feb_28079049_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079049_8_8_feb.json", gas="no2")
df_feb_28079050_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079050_8_8_feb.json", gas="no2")
df_feb_28079054_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079054_8_8_feb.json", gas="no2")
df_feb_28079055_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079055_8_8_feb.json", gas="no2")
df_feb_28079056_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079056_8_8_feb.json", gas="no2")
df_feb_28079057_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079057_8_8_feb.json", gas="no2")
df_feb_28079058_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079058_8_8_feb.json", gas="no2")
df_feb_28079059_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079059_8_8_feb.json", gas="no2")
df_feb_28079060_no2_train = read_air_data("data_air_json/feb/air_quality_observed_28079060_8_8_feb.json", gas="no2")

df_mar_28079004_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079004_8_8_mar.json", gas="no2")
df_mar_28079008_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079008_8_8_mar.json", gas="no2")
df_mar_28079011_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079011_8_8_mar.json", gas="no2")
df_mar_28079016_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079016_8_8_mar.json", gas="no2")
df_mar_28079017_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079017_8_8_mar.json", gas="no2")
df_mar_28079018_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079018_8_8_mar.json", gas="no2")
df_mar_28079024_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079024_8_8_mar.json", gas="no2")
df_mar_28079027_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079027_8_8_mar.json", gas="no2")
df_mar_28079035_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079035_8_8_mar.json", gas="no2")
df_mar_28079036_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079036_8_8_mar.json", gas="no2")
df_mar_28079038_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079038_8_8_mar.json", gas="no2")
df_mar_28079039_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079039_8_8_mar.json", gas="no2")
df_mar_28079040_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079040_8_8_mar.json", gas="no2")
df_mar_28079047_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079047_8_8_mar.json", gas="no2")
df_mar_28079048_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079048_8_8_mar.json", gas="no2")
df_mar_28079049_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079049_8_8_mar.json", gas="no2")
df_mar_28079050_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079050_8_8_mar.json", gas="no2")
df_mar_28079054_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079054_8_8_mar.json", gas="no2")
df_mar_28079055_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079055_8_8_mar.json", gas="no2")
df_mar_28079056_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079056_8_8_mar.json", gas="no2")
df_mar_28079057_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079057_8_8_mar.json", gas="no2")
df_mar_28079058_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079058_8_8_mar.json", gas="no2")
df_mar_28079059_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079059_8_8_mar.json", gas="no2")
df_mar_28079060_no2_train = read_air_data("data_air_json/mar/air_quality_observed_28079060_8_8_mar.json", gas="no2")

# Concat Train Data
df_28079004_no2_train = pd.concat([df_jan_28079004_no2_train, df_feb_28079004_no2_train, df_mar_28079004_no2_train], ignore_index = 'True')
df_28079016_no2_train = pd.concat([df_jan_28079016_no2_train, df_feb_28079016_no2_train, df_mar_28079016_no2_train], ignore_index = 'True')
df_28079017_no2_train = pd.concat([df_jan_28079017_no2_train, df_feb_28079017_no2_train, df_mar_28079017_no2_train], ignore_index = 'True')
df_28079027_no2_train = pd.concat([df_jan_28079027_no2_train, df_feb_28079027_no2_train, df_mar_28079027_no2_train], ignore_index = 'True')
df_28079039_no2_train = pd.concat([df_jan_28079039_no2_train, df_feb_28079039_no2_train, df_mar_28079039_no2_train], ignore_index = 'True')
df_28079049_no2_train = pd.concat([df_jan_28079049_no2_train, df_feb_28079049_no2_train, df_mar_28079049_no2_train], ignore_index = 'True')
df_28079054_no2_train = pd.concat([df_jan_28079054_no2_train, df_feb_28079054_no2_train, df_mar_28079054_no2_train], ignore_index = 'True')
df_28079058_no2_train = pd.concat([df_jan_28079058_no2_train, df_feb_28079058_no2_train, df_mar_28079058_no2_train], ignore_index = 'True')
df_28079059_no2_train = pd.concat([df_jan_28079059_no2_train, df_feb_28079059_no2_train, df_mar_28079059_no2_train], ignore_index = 'True')


# Test Data
df_28079004_no2_test = read_air_data("data_air_json/apr/air_quality_observed_28079004_8_8_apr.json", gas="no2").iloc[0:192]
df_28079016_no2_test = read_air_data("data_air_json/apr/air_quality_observed_28079016_8_8_apr.json", gas="no2").iloc[0:192]
df_28079017_no2_test = read_air_data("data_air_json/apr/air_quality_observed_28079017_8_8_apr.json", gas="no2").iloc[0:192]
df_28079027_no2_test = read_air_data("data_air_json/apr/air_quality_observed_28079027_8_8_apr.json", gas="no2").iloc[0:192]
df_28079039_no2_test = read_air_data("data_air_json/apr/air_quality_observed_28079039_8_8_apr.json", gas="no2").iloc[0:192]
df_28079049_no2_test = read_air_data("data_air_json/apr/air_quality_observed_28079049_8_8_apr.json", gas="no2").iloc[0:192]
df_28079054_no2_test = read_air_data("data_air_json/apr/air_quality_observed_28079054_8_8_apr.json", gas="no2").iloc[0:192]
df_28079058_no2_test = read_air_data("data_air_json/apr/air_quality_observed_28079058_8_8_apr.json", gas="no2").iloc[0:192]
df_28079059_no2_test = read_air_data("data_air_json/apr/air_quality_observed_28079059_8_8_apr.json", gas="no2").iloc[0:192]


# ### NOx

# In[ ]:


for stations in stations_nox:
    print(f'df_mar_{stations.split("_")[0]}_nox_train = read_air_data("data_air_json/mar/air_quality_observed_{stations}_mar.json", gas="nox")')


# In[47]:


df_jan_28079004_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079004_12_8_jan.json", gas="nox")
df_jan_28079008_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079008_12_8_jan.json", gas="nox")
df_jan_28079011_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079011_12_8_jan.json", gas="nox")
df_jan_28079016_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079016_12_8_jan.json", gas="nox")
df_jan_28079017_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079017_12_8_jan.json", gas="nox")
df_jan_28079018_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079018_12_8_jan.json", gas="nox")
df_jan_28079024_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079024_12_8_jan.json", gas="nox")
df_jan_28079027_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079027_12_8_jan.json", gas="nox")
df_jan_28079035_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079035_12_8_jan.json", gas="nox")
df_jan_28079036_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079036_12_8_jan.json", gas="nox")
df_jan_28079038_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079038_12_8_jan.json", gas="nox")
df_jan_28079039_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079039_12_8_jan.json", gas="nox")
df_jan_28079040_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079040_12_8_jan.json", gas="nox")
df_jan_28079047_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079047_12_8_jan.json", gas="nox")
df_jan_28079048_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079048_12_8_jan.json", gas="nox")
df_jan_28079049_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079049_12_8_jan.json", gas="nox")
df_jan_28079050_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079050_12_8_jan.json", gas="nox")
df_jan_28079054_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079054_12_8_jan.json", gas="nox")
df_jan_28079055_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079055_12_8_jan.json", gas="nox")
df_jan_28079056_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079056_12_8_jan.json", gas="nox")
df_jan_28079057_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079057_12_8_jan.json", gas="nox")
df_jan_28079058_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079058_12_8_jan.json", gas="nox")
df_jan_28079059_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079059_12_8_jan.json", gas="nox")
df_jan_28079060_nox_train = read_air_data("data_air_json/jan/air_quality_observed_28079060_12_8_jan.json", gas="nox")

df_feb_28079004_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079004_12_8_feb.json", gas="nox")
df_feb_28079008_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079008_12_8_feb.json", gas="nox")
df_feb_28079011_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079011_12_8_feb.json", gas="nox")
df_feb_28079016_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079016_12_8_feb.json", gas="nox")
df_feb_28079017_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079017_12_8_feb.json", gas="nox")
df_feb_28079018_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079018_12_8_feb.json", gas="nox")
df_feb_28079024_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079024_12_8_feb.json", gas="nox")
df_feb_28079027_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079027_12_8_feb.json", gas="nox")
df_feb_28079035_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079035_12_8_feb.json", gas="nox")
df_feb_28079036_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079036_12_8_feb.json", gas="nox")
df_feb_28079038_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079038_12_8_feb.json", gas="nox")
df_feb_28079039_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079039_12_8_feb.json", gas="nox")
df_feb_28079040_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079040_12_8_feb.json", gas="nox")
df_feb_28079047_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079047_12_8_feb.json", gas="nox")
df_feb_28079048_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079048_12_8_feb.json", gas="nox")
df_feb_28079049_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079049_12_8_feb.json", gas="nox")
df_feb_28079050_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079050_12_8_feb.json", gas="nox")
df_feb_28079054_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079054_12_8_feb.json", gas="nox")
df_feb_28079055_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079055_12_8_feb.json", gas="nox")
df_feb_28079056_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079056_12_8_feb.json", gas="nox")
df_feb_28079057_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079057_12_8_feb.json", gas="nox")
df_feb_28079058_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079058_12_8_feb.json", gas="nox")
df_feb_28079059_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079059_12_8_feb.json", gas="nox")
df_feb_28079060_nox_train = read_air_data("data_air_json/feb/air_quality_observed_28079060_12_8_feb.json", gas="nox")

df_mar_28079004_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079004_12_8_mar.json", gas="nox")
df_mar_28079008_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079008_12_8_mar.json", gas="nox")
df_mar_28079011_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079011_12_8_mar.json", gas="nox")
df_mar_28079016_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079016_12_8_mar.json", gas="nox")
df_mar_28079017_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079017_12_8_mar.json", gas="nox")
df_mar_28079018_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079018_12_8_mar.json", gas="nox")
df_mar_28079024_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079024_12_8_mar.json", gas="nox")
df_mar_28079027_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079027_12_8_mar.json", gas="nox")
df_mar_28079035_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079035_12_8_mar.json", gas="nox")
df_mar_28079036_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079036_12_8_mar.json", gas="nox")
df_mar_28079038_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079038_12_8_mar.json", gas="nox")
df_mar_28079039_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079039_12_8_mar.json", gas="nox")
df_mar_28079040_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079040_12_8_mar.json", gas="nox")
df_mar_28079047_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079047_12_8_mar.json", gas="nox")
df_mar_28079048_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079048_12_8_mar.json", gas="nox")
df_mar_28079049_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079049_12_8_mar.json", gas="nox")
df_mar_28079050_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079050_12_8_mar.json", gas="nox")
df_mar_28079054_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079054_12_8_mar.json", gas="nox")
df_mar_28079055_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079055_12_8_mar.json", gas="nox")
df_mar_28079056_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079056_12_8_mar.json", gas="nox")
df_mar_28079057_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079057_12_8_mar.json", gas="nox")
df_mar_28079058_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079058_12_8_mar.json", gas="nox")
df_mar_28079059_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079059_12_8_mar.json", gas="nox")
df_mar_28079060_nox_train = read_air_data("data_air_json/mar/air_quality_observed_28079060_12_8_mar.json", gas="nox")

# Concat Train Data
df_28079004_nox_train = pd.concat([df_jan_28079004_nox_train, df_feb_28079004_nox_train, df_mar_28079004_nox_train], ignore_index = 'True')
df_28079016_nox_train = pd.concat([df_jan_28079016_nox_train, df_feb_28079016_nox_train, df_mar_28079016_nox_train], ignore_index = 'True')
df_28079017_nox_train = pd.concat([df_jan_28079017_nox_train, df_feb_28079017_nox_train, df_mar_28079017_nox_train], ignore_index = 'True')
df_28079027_nox_train = pd.concat([df_jan_28079027_nox_train, df_feb_28079027_nox_train, df_mar_28079027_nox_train], ignore_index = 'True')
df_28079039_nox_train = pd.concat([df_jan_28079039_nox_train, df_feb_28079039_nox_train, df_mar_28079039_nox_train], ignore_index = 'True')
df_28079049_nox_train = pd.concat([df_jan_28079049_nox_train, df_feb_28079049_nox_train, df_mar_28079049_nox_train], ignore_index = 'True')
df_28079054_nox_train = pd.concat([df_jan_28079054_nox_train, df_feb_28079054_nox_train, df_mar_28079054_nox_train], ignore_index = 'True')
df_28079058_nox_train = pd.concat([df_jan_28079058_nox_train, df_feb_28079058_nox_train, df_mar_28079058_nox_train], ignore_index = 'True')
df_28079059_nox_train = pd.concat([df_jan_28079059_nox_train, df_feb_28079059_nox_train, df_mar_28079059_nox_train], ignore_index = 'True')

df_28079004_nox_test = read_air_data("data_air_json/apr/air_quality_observed_28079004_12_8_apr.json", gas="nox").iloc[0:192]
df_28079016_nox_test = read_air_data("data_air_json/apr/air_quality_observed_28079016_12_8_apr.json", gas="nox").iloc[0:192]
df_28079017_nox_test = read_air_data("data_air_json/apr/air_quality_observed_28079017_12_8_apr.json", gas="nox").iloc[0:192]
df_28079027_nox_test = read_air_data("data_air_json/apr/air_quality_observed_28079027_12_8_apr.json", gas="nox").iloc[0:192]
df_28079039_nox_test = read_air_data("data_air_json/apr/air_quality_observed_28079039_12_8_apr.json", gas="nox").iloc[0:192]
df_28079049_nox_test = read_air_data("data_air_json/apr/air_quality_observed_28079049_12_8_apr.json", gas="nox").iloc[0:192]
df_28079054_nox_test = read_air_data("data_air_json/apr/air_quality_observed_28079054_12_8_apr.json", gas="nox").iloc[0:192]
df_28079058_nox_test = read_air_data("data_air_json/apr/air_quality_observed_28079058_12_8_apr.json", gas="nox").iloc[0:192]
df_28079059_nox_test = read_air_data("data_air_json/apr/air_quality_observed_28079059_12_8_apr.json", gas="nox").iloc[0:192]


# ### O3

# In[ ]:


for stations in stations_o3:
    print(f'df_mar_{stations.split("_")[0]}_o3_train = read_air_data("data_air_json/mar/air_quality_observed_{stations}_mar.json", gas="o3")')


# In[52]:


df_jan_28079008_o3_train = read_air_data("data_air_json/jan/air_quality_observed_28079008_14_6_jan.json", gas="o3")
df_jan_28079016_o3_train = read_air_data("data_air_json/jan/air_quality_observed_28079016_14_6_jan.json", gas="o3")
df_jan_28079017_o3_train = read_air_data("data_air_json/jan/air_quality_observed_28079017_14_6_jan.json", gas="o3")
df_jan_28079018_o3_train = read_air_data("data_air_json/jan/air_quality_observed_28079018_14_6_jan.json", gas="o3")
df_jan_28079024_o3_train = read_air_data("data_air_json/jan/air_quality_observed_28079024_14_6_jan.json", gas="o3")
df_jan_28079027_o3_train = read_air_data("data_air_json/jan/air_quality_observed_28079027_14_6_jan.json", gas="o3")
df_jan_28079035_o3_train = read_air_data("data_air_json/jan/air_quality_observed_28079035_14_6_jan.json", gas="o3")
df_jan_28079039_o3_train = read_air_data("data_air_json/jan/air_quality_observed_28079039_14_6_jan.json", gas="o3")
df_jan_28079049_o3_train = read_air_data("data_air_json/jan/air_quality_observed_28079049_14_6_jan.json", gas="o3")
df_jan_28079054_o3_train = read_air_data("data_air_json/jan/air_quality_observed_28079054_14_6_jan.json", gas="o3")
df_jan_28079058_o3_train = read_air_data("data_air_json/jan/air_quality_observed_28079058_14_6_jan.json", gas="o3")
df_jan_28079059_o3_train = read_air_data("data_air_json/jan/air_quality_observed_28079059_14_6_jan.json", gas="o3")
df_jan_28079060_o3_train = read_air_data("data_air_json/jan/air_quality_observed_28079060_14_6_jan.json", gas="o3")

df_feb_28079008_o3_train = read_air_data("data_air_json/feb/air_quality_observed_28079008_14_6_feb.json", gas="o3")
df_feb_28079016_o3_train = read_air_data("data_air_json/feb/air_quality_observed_28079016_14_6_feb.json", gas="o3")
df_feb_28079017_o3_train = read_air_data("data_air_json/feb/air_quality_observed_28079017_14_6_feb.json", gas="o3")
df_feb_28079018_o3_train = read_air_data("data_air_json/feb/air_quality_observed_28079018_14_6_feb.json", gas="o3")
df_feb_28079024_o3_train = read_air_data("data_air_json/feb/air_quality_observed_28079024_14_6_feb.json", gas="o3")
df_feb_28079027_o3_train = read_air_data("data_air_json/feb/air_quality_observed_28079027_14_6_feb.json", gas="o3")
df_feb_28079035_o3_train = read_air_data("data_air_json/feb/air_quality_observed_28079035_14_6_feb.json", gas="o3")
df_feb_28079039_o3_train = read_air_data("data_air_json/feb/air_quality_observed_28079039_14_6_feb.json", gas="o3")
df_feb_28079049_o3_train = read_air_data("data_air_json/feb/air_quality_observed_28079049_14_6_feb.json", gas="o3")
df_feb_28079054_o3_train = read_air_data("data_air_json/feb/air_quality_observed_28079054_14_6_feb.json", gas="o3")
df_feb_28079058_o3_train = read_air_data("data_air_json/feb/air_quality_observed_28079058_14_6_feb.json", gas="o3")
df_feb_28079059_o3_train = read_air_data("data_air_json/feb/air_quality_observed_28079059_14_6_feb.json", gas="o3")
df_feb_28079060_o3_train = read_air_data("data_air_json/feb/air_quality_observed_28079060_14_6_feb.json", gas="o3")

df_mar_28079008_o3_train = read_air_data("data_air_json/mar/air_quality_observed_28079008_14_6_mar.json", gas="o3")
df_mar_28079016_o3_train = read_air_data("data_air_json/mar/air_quality_observed_28079016_14_6_mar.json", gas="o3")
df_mar_28079017_o3_train = read_air_data("data_air_json/mar/air_quality_observed_28079017_14_6_mar.json", gas="o3")
df_mar_28079018_o3_train = read_air_data("data_air_json/mar/air_quality_observed_28079018_14_6_mar.json", gas="o3")
df_mar_28079024_o3_train = read_air_data("data_air_json/mar/air_quality_observed_28079024_14_6_mar.json", gas="o3")
df_mar_28079027_o3_train = read_air_data("data_air_json/mar/air_quality_observed_28079027_14_6_mar.json", gas="o3")
df_mar_28079035_o3_train = read_air_data("data_air_json/mar/air_quality_observed_28079035_14_6_mar.json", gas="o3")
df_mar_28079039_o3_train = read_air_data("data_air_json/mar/air_quality_observed_28079039_14_6_mar.json", gas="o3")
df_mar_28079049_o3_train = read_air_data("data_air_json/mar/air_quality_observed_28079049_14_6_mar.json", gas="o3")
df_mar_28079054_o3_train = read_air_data("data_air_json/mar/air_quality_observed_28079054_14_6_mar.json", gas="o3")
df_mar_28079058_o3_train = read_air_data("data_air_json/mar/air_quality_observed_28079058_14_6_mar.json", gas="o3")
df_mar_28079059_o3_train = read_air_data("data_air_json/mar/air_quality_observed_28079059_14_6_mar.json", gas="o3")
df_mar_28079060_o3_train = read_air_data("data_air_json/mar/air_quality_observed_28079060_14_6_mar.json", gas="o3")

# Concat Train Data
df_28079016_o3_train = pd.concat([df_jan_28079016_o3_train, df_feb_28079016_o3_train, df_mar_28079016_o3_train], ignore_index = 'True')
df_28079017_o3_train = pd.concat([df_jan_28079017_o3_train, df_feb_28079017_o3_train, df_mar_28079017_o3_train], ignore_index = 'True')
df_28079027_o3_train = pd.concat([df_jan_28079027_o3_train, df_feb_28079027_o3_train, df_mar_28079027_o3_train], ignore_index = 'True')
df_28079039_o3_train = pd.concat([df_jan_28079039_o3_train, df_feb_28079039_o3_train, df_mar_28079039_o3_train], ignore_index = 'True')
df_28079049_o3_train = pd.concat([df_jan_28079049_o3_train, df_feb_28079049_o3_train, df_mar_28079049_o3_train], ignore_index = 'True')
df_28079054_o3_train = pd.concat([df_jan_28079054_o3_train, df_feb_28079054_o3_train, df_mar_28079054_o3_train], ignore_index = 'True')
df_28079058_o3_train = pd.concat([df_jan_28079058_o3_train, df_feb_28079058_o3_train, df_mar_28079058_o3_train], ignore_index = 'True')
df_28079059_o3_train = pd.concat([df_jan_28079059_o3_train, df_feb_28079059_o3_train, df_mar_28079059_o3_train], ignore_index = 'True')


df_28079016_o3_test = read_air_data("data_air_json/apr/air_quality_observed_28079016_14_6_apr.json", gas="o3").iloc[0:192]
df_28079017_o3_test = read_air_data("data_air_json/apr/air_quality_observed_28079017_14_6_apr.json", gas="o3").iloc[0:192]
df_28079027_o3_test = read_air_data("data_air_json/apr/air_quality_observed_28079027_14_6_apr.json", gas="o3").iloc[0:192]
df_28079039_o3_test = read_air_data("data_air_json/apr/air_quality_observed_28079039_14_6_apr.json", gas="o3").iloc[0:192]
df_28079049_o3_test = read_air_data("data_air_json/apr/air_quality_observed_28079049_14_6_apr.json", gas="o3").iloc[0:192]
df_28079054_o3_test = read_air_data("data_air_json/apr/air_quality_observed_28079054_14_6_apr.json", gas="o3").iloc[0:192]
df_28079058_o3_test = read_air_data("data_air_json/apr/air_quality_observed_28079058_14_6_apr.json", gas="o3").iloc[0:192]
df_28079059_o3_test = read_air_data("data_air_json/apr/air_quality_observed_28079059_14_6_apr.json", gas="o3").iloc[0:192]


# ### CO

# In[65]:


df_jan_28079004_co_train = read_air_data("data_air_json/jan/air_quality_observed_28079004_6_48_jan.json", gas="co")

df_feb_28079004_co_train = read_air_data("data_air_json/feb/air_quality_observed_28079004_6_48_feb.json", gas="co")

df_mar_28079004_co_train = read_air_data("data_air_json/mar/air_quality_observed_28079004_6_48_mar.json", gas="co")

df_28079004_co_train = pd.concat([df_jan_28079004_co_train, df_feb_28079004_co_train, df_mar_28079004_co_train], ignore_index = 'True')

df_28079004_co_test = read_air_data("data_air_json/apr/air_quality_observed_28079004_6_48_apr.json", gas="co").iloc[0:192]


# ### Scaling

# In[ ]:


df_28079004_train = pd.concat([df_28079004_no_train['value'], df_28079004_no2_train['value'], df_28079004_nox_train['value'], df_28079004_co_train['value']], axis=1, ignore_index = 'True')

df_28079016_train = pd.concat([df_28079016_no_train['value'], df_28079016_no2_train['value'], df_28079016_nox_train['value'], df_28079016_o3_train['value']], axis=1, ignore_index = 'True')
df_28079017_train = pd.concat([df_28079017_no_train['value'], df_28079017_no2_train['value'], df_28079017_nox_train['value'], df_28079017_o3_train['value']], axis=1, ignore_index = 'True')
df_28079027_train = pd.concat([df_28079027_no_train['value'], df_28079027_no2_train['value'], df_28079027_nox_train['value'], df_28079027_o3_train['value']], axis=1, ignore_index = 'True')
df_28079039_train = pd.concat([df_28079039_no_train['value'], df_28079039_no2_train['value'], df_28079039_nox_train['value'], df_28079039_o3_train['value']], axis=1, ignore_index = 'True')
df_28079049_train = pd.concat([df_28079049_no_train['value'], df_28079049_no2_train['value'], df_28079049_nox_train['value'], df_28079049_o3_train['value']], axis=1, ignore_index = 'True')
df_28079054_train = pd.concat([df_28079054_no_train['value'], df_28079054_no2_train['value'], df_28079054_nox_train['value'], df_28079054_o3_train['value']], axis=1, ignore_index = 'True')
df_28079058_train = pd.concat([df_28079058_no_train['value'], df_28079058_no2_train['value'], df_28079058_nox_train['value'], df_28079058_o3_train['value']], axis=1, ignore_index = 'True')
df_28079059_train = pd.concat([df_28079059_no_train['value'], df_28079059_no2_train['value'], df_28079059_nox_train['value'], df_28079059_o3_train['value']], axis=1, ignore_index = 'True')

df_all_train = pd.concat([df_28079004_train, df_28079016_train, df_28079017_train, df_28079027_train, df_28079039_train,df_28079049_train, df_28079054_train, df_28079058_train, df_28079059_train ], axis=0, ignore_index=True)


# In[115]:


df_all_train.shape


# In[ ]:


df_28079004_test = pd.concat([df_28079004_no_test['value'], df_28079004_no2_test['value'], df_28079004_nox_test['value'], df_28079004_co_test['value']], axis=1, ignore_index = 'True')

df_28079016_test = pd.concat([df_28079016_no_test['value'], df_28079016_no2_test['value'], df_28079016_nox_test['value'], df_28079016_o3_test['value']], axis=1, ignore_index = 'True')
df_28079017_test = pd.concat([df_28079017_no_test['value'], df_28079017_no2_test['value'], df_28079017_nox_test['value'], df_28079017_o3_test['value']], axis=1, ignore_index = 'True')
df_28079027_test = pd.concat([df_28079027_no_test['value'], df_28079027_no2_test['value'], df_28079027_nox_test['value'], df_28079027_o3_test['value']], axis=1, ignore_index = 'True')
df_28079039_test = pd.concat([df_28079039_no_test['value'], df_28079039_no2_test['value'], df_28079039_nox_test['value'], df_28079039_o3_test['value']], axis=1, ignore_index = 'True')
df_28079049_test = pd.concat([df_28079049_no_test['value'], df_28079049_no2_test['value'], df_28079049_nox_test['value'], df_28079049_o3_test['value']], axis=1, ignore_index = 'True')
df_28079054_test = pd.concat([df_28079054_no_test['value'], df_28079054_no2_test['value'], df_28079054_nox_test['value'], df_28079054_o3_test['value']], axis=1, ignore_index = 'True')
df_28079058_test = pd.concat([df_28079058_no_test['value'], df_28079058_no2_test['value'], df_28079058_nox_test['value'], df_28079058_o3_test['value']], axis=1, ignore_index = 'True')
df_28079059_test = pd.concat([df_28079059_no_test['value'], df_28079059_no2_test['value'], df_28079059_nox_test['value'], df_28079059_o3_test['value']], axis=1, ignore_index = 'True')

df_all_test = pd.concat([df_28079004_test, df_28079016_test, df_28079017_test, df_28079027_test,df_28079039_test,df_28079049_test, df_28079054_test, df_28079058_test, df_28079059_test], axis=0, ignore_index=True)


# In[120]:


df_all_test.shape


# In[ ]:


df_all = pd.concat([df_all_train, df_all_test], axis = 0, ignore_index=True)
df_all


# In[ ]:


df_all_train_scaled = (df_all_train - df_all.min(axis=0)) / (df_all.max(axis=0) - df_all.min(axis=0))
df_all_train_scaled


# In[ ]:


df_all_test_scaled = (df_all_test - df_all.min(axis=0)) / (df_all.max(axis=0) - df_all.min(axis=0))
df_all_test_scaled


# In[121]:


df_28079004_train_scaled_value = df_all_train_scaled.iloc[0:2184]
df_28079016_train_scaled_value = df_all_train_scaled.iloc[2184:4368]
df_28079017_train_scaled_value = df_all_train_scaled.iloc[4368:6552]
df_28079027_train_scaled_value = df_all_train_scaled.iloc[6552:8736]
df_28079039_train_scaled_value = df_all_train_scaled.iloc[8736:10872]
df_28079049_train_scaled_value = df_all_train_scaled.iloc[10872:13056]
df_28079054_train_scaled_value = df_all_train_scaled.iloc[13056:15240]
df_28079058_train_scaled_value = df_all_train_scaled.iloc[15240:17424]
df_28079059_train_scaled_value = df_all_train_scaled.iloc[17424:19608]


# In[122]:


df_28079004_test_scaled_value = df_all_test_scaled.iloc[0:192]
df_28079016_test_scaled_value = df_all_test_scaled.iloc[192:384]
df_28079017_test_scaled_value = df_all_test_scaled.iloc[384:576]
df_28079027_test_scaled_value = df_all_test_scaled.iloc[576:768]
df_28079039_test_scaled_value = df_all_test_scaled.iloc[768:960]
df_28079049_test_scaled_value = df_all_test_scaled.iloc[960:1152]
df_28079054_test_scaled_value = df_all_test_scaled.iloc[1152:1344]
df_28079058_test_scaled_value = df_all_test_scaled.iloc[1344:1536]
df_28079059_test_scaled_value = df_all_test_scaled.iloc[1536:1728]

