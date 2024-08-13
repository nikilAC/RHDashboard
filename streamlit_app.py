import streamlit as st
import pandas as pd
import math
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
import numpy as np 
from collections.abc import Mapping
import io
import json
import time
from apiclient import discovery
from httplib2 import Http

from IPython.display import display
import boto3


# Make page wide layout 
st. set_page_config(layout="wide")

# Confidential Keys for Accessing AWS Database
amzSecrets = st.secrets["AWSKeys"]

# s3 connection to call functions on AWS Stuff
s3_client = boto3.client('s3', aws_access_key_id = amzSecrets['aws_key_access'], aws_secret_access_key = amzSecrets['aws_secret'])
def get_bucket_list(bucketKey):
  s3 = boto3.resource('s3', aws_access_key_id = amzSecrets['aws_key_access'], aws_secret_access_key = amzSecrets['aws_secret'])
  my_bucket = s3.Bucket(amzSecrets[bucketKey])
  bucketnames = np.array([])
  # List out objects in s3 bucket
  for obj in my_bucket.objects.all():
    bucketnames = np.append(bucketnames, obj.key)

  return bucketnames


def get_drive_data(weatherDataFile):
    """Grab Data from AWS .
    """

    # Get Representative SN1 Data from AWS

    representativeData = s3_client.get_object(Bucket = amzSecrets["representativedatabucket"], Key = 'SN1_Representative_Data.csv')

    # Save representative data to df

    rep_df = pd.read_csv(io.StringIO(representativeData['Body'].read().decode('utf-8')))

    # Weather Data to base Estimations

    
    weatherData = s3_client.get_object(Bucket =  amzSecrets["weatherdatabucket"], Key = weatherDataFile)

    # Condition so code works with both csv and excel files
    if '.xlsx' in weatherDataFile:
      weather_df = pd.read_excel(io.BytesIO(weatherData['Body'].read()))
    else:
      weather_df = pd.read_csv(io.StringIO(weatherData['Body'].read().decode('utf-8')))

    # Change format of object such that it outputs as Df (Csv needs decoding to get the correct result)
    return rep_df, weather_df




#sn1_data = getFilePydrive(file_name)

# RH Data Estimation Script

def perfEstFuncPolynom(weatherDataFile, scale=1):
  #Dictionaries    
  df, weatherData = get_drive_data(weatherDataFile)
  # For converting hourly estimates -> Monthly Estimates
  hours_in_month = {
    "01": 31 * 24,
    "02": 28 * 24,  # 29 * 24 for leap year
    "03": 31 * 24,
    "04": 30 * 24,
    "05": 31 * 24,
    "06": 30 * 24,
    "07": 31 * 24,
    "08": 31 * 24,
    "09": 30 * 24,
    "10": 31 * 24,
    "11": 30 * 24,
    "12": 31 * 24
      }

  # Dictionary For Translating MonthNumber to Name
  monthNumToName = {
        "1" : "January",
        "2" : "February",
        "3" : "March",
        "4" : "April",
        "5" : "May",
        "6" : "June",
        "7" : "July",
        "8" : "August",
        "9" : "September",
        "10" : "October",
        "11" : "November",
        "12" : "December"
    }
  

   # Create a consistent coloring convention for Contactors {TO CHANGE LATER WHEN WE GET MORE BRICKS}
  colors = {
      2: "blue",
      17: "red"
  }

  # Getting and collecting weather data


  # Drop all non-numeric rows from weather date (in case of null values)) 
  # Assuming temperature is Temperature_degC And humidity is RH_percent {O(n) Runtime}
  weatherData = weatherData[pd.to_numeric(weatherData['Temperature_degC'], errors='coerce').notnull()]
  weatherData = weatherData[pd.to_numeric(weatherData['RH_percent'], errors='coerce').notnull()]


    #Test Later
    #weatherData[['Temperature_degC', "RH_percent"]] = weatherData[['Temperature_degC', "RH_percent"]].apply(pd.to_numeric)

  # Make Temperature and RH Percent columns Numeric {O(n) Runtime}
  weatherData['Temperature_degC'] = pd.to_numeric(weatherData['Temperature_degC'])
  weatherData['RH_percent'] = pd.to_numeric(weatherData['RH_percent'])

  weatherData.index = np.arange(0, len(weatherData))

    # Full CO2 Calculations (Python Side)

  # Calculating CO2 Purity (Of initial SN1 Data)
  # [NOTE FLOW CALCULATION IS DONE USING FOX SENSOR: gives indiction of CO2 Flow]
  # [NOTE PURITY CALCULATION IS CURRENTLY DONE USING BGA]

  df["CO2_Purity-Corrected_g"] = df[" CO2_Fox_g"] * (df[" DAC_CO2_Percent"] / 100)

  # Calculating Kg Per Hour Again, Directly from CO2_Purity_corrected (/1000 to kg, /CycleSecs to cycle time, * 3600 to hour)
  df["CO2_Kg_Per_Hour_Projected"] = df["CO2_Purity-Corrected_g"] / 1000 / df[" CycleSecs"] * 3600

  # Calculating Kg Per Day
  df["CO2_Kg_Per_Day_Projected"] = df["CO2_Kg_Per_Hour_Projected"] * 24

  # Making various figures
  mainPlot = go.Figure()
  dayBar = go.Figure()
  monthBar = go.Figure()
  newFig = go.Figure()

  # For loop to create contactor-specific data 
  for contactor in df["Contactor Type"].unique():
    
    contactDf = df
    # Do not consider first three towers for type 17 brick
    if contactor == 17:
      contactDf = contactDf.query('`Contactor Type` == 17 and ` DAC_TowerNum` > 3')
    else:
      contactDf = contactDf.query('`Contactor Type` == @contactor')

    # Polynomial regression fit to create estimation
    poly_fit = np.polyfit(contactDf[" AirRelHumid_In"], contactDf["CO2_Kg_Per_Hour_Projected"], deg=3)

    # Find maximum and minimum RH Values in Operational data, so we can set any values not between these to one or the other
    maxRH = contactDf[' AirRelHumid_In'].max()
    minRH = contactDf[' AirRelHumid_In'].min()

    print(f"Contactor Type {contactor}")

    # Accounting for values outside our range
    weatherData.loc[weatherData['RH_percent'] < minRH, 'RH_percent'] = minRH + .01
    weatherData.loc[weatherData['RH_percent'] > maxRH, 'RH_percent'] = maxRH


    # Include timestamps for added granularity (generalize for different times of data)
    weatherData["Timestamp"] = pd.to_datetime(weatherData["Timestamp"])
    weatherData['Month_Year'] = weatherData['Timestamp'].dt.strftime('%Y-%m')
    weatherData["Month"] = weatherData["Timestamp"].dt.month
    weatherData["Day"] = weatherData["Timestamp"].dt.day
    weatherData["Date"] = weatherData["Timestamp"].dt.date
    weatherData = weatherData.sort_values(by="Timestamp")


    # Calculating Values for Interpolation: (y2 - y1) / (x2 - x1) * (x - x1) + y1 = y

    model = np.poly1d(poly_fit)
    weatherData["CO2_Kg_Per_Hour_Projected"] = model(weatherData["RH_percent"]) * scale

    # Plotting hour-by-hour line/scatter plot
    mainPlot.add_trace(go.Scatter(x=weatherData["Timestamp"], y=weatherData["CO2_Kg_Per_Hour_Projected"], mode='markers+lines', name = f"Contactor Type: {contactor}", marker_color = colors[contactor], yaxis= 'y1'))

    # Building Month Based Bar Chart
    # Getting the month as a number
    year, month = zip(*np.array(weatherData["Month_Year"].str.split("-")))

    month = pd.Series(month)

    # finding average RH then multiplying because not every day has data
    weatherData["HoursInMonth"] = month.map(hours_in_month)
    weatherData["CO2_Kg_Per_Month_Projected"] = weatherData["CO2_Kg_Per_Hour_Projected"] * weatherData["HoursInMonth"]
    weatherData["Month_Year"] = pd.to_datetime(weatherData["Month_Year"])

    # Adding up RH from each Day (won't fully line up with month predictions) 
    dayMerge = weatherData.groupby("Date").agg({"CO2_Kg_Per_Hour_Projected": "sum"}).reset_index()
    dayMerge.rename(columns = {"CO2_Kg_Per_Hour_Projected": "CO2_Kg_Per_Day_Projected"}, inplace = True)

    # Plotting Day-Based Bar Chart
    dayBar.add_trace(go.Bar(x=dayMerge["Date"], y=dayMerge["CO2_Kg_Per_Day_Projected"], marker_color = colors[contactor],  name = f"Contactor Type: {contactor}"))


    # Building Month based Production Summary Table for 8 DAC
    monthSummary = weatherData.groupby("Month").agg({"CO2_Kg_Per_Month_Projected": ["mean", "min", "max"]})
    monthSummary.columns = ["Mean Production CO2", "Min Production CO2", "Max Production CO2"]

    # Formatting Month Based Summary Table
    monthSummary = monthSummary.dropna().reset_index()
    monthSummary["Mean Production CO2"] = monthSummary["Mean Production CO2"].astype(int)
    monthSummary["Max Production CO2"] = monthSummary["Max Production CO2"].astype(int)
    monthSummary["Min Production CO2"] = monthSummary["Min Production CO2"].astype(int)
    monthSummary["Month"] = monthSummary["Month"].astype(str).map(monthNumToName)

    # Plotting Month Based Bar Chart
    monthMerge = weatherData.groupby("Month_Year").agg({"CO2_Kg_Per_Month_Projected" : "mean"}).reset_index()

    # Dividing by 1000 to switch value to metric tons
    monthBar.add_trace(go.Bar(x=monthMerge["Month_Year"], y=monthMerge["CO2_Kg_Per_Month_Projected"] / 1000, marker_color = colors[contactor],  name = f"Contactor Type: {contactor}"))


  mainPlot.update_layout(xaxis_title = "Date", yaxis_title = "CO2 Production Volume (kg/hr)", width=7000, height = 600)



  st.plotly_chart(mainPlot, use_container_width = True)

  #newFig.update_layout(xaxis_title = "Date", yaxis_title = "CO2 Production Volume (kg/hr)",  legend = dict(groupclick = "toggleitem"))
  #newFig.show()

  dayBar.update_layout(xaxis_title = "Date", yaxis_title = "CO2 Production Volume (kg)", barmode = "group",   bargap = .2, legend = dict(groupclick = "toggleitem"))
  st.plotly_chart(dayBar) 

  #pivotScatter.update_layout(xaxis_title = "RH Regime", yaxis_title = "Production Volume (kg/hr)")
  #pivotScatter.show()
  
  st.plotly_chart(monthBar)
  monthBar.update_layout(xaxis_title = "Month", yaxis_title = "CO2 Production Volume (Tons)")

  #return fig

# Code to get list of rhPaths'

rhPath = st.sidebar.selectbox("Choose Weather File", get_bucket_list("weatherdatabucket"), index = None)
st.write("You chose:", rhPath)
# DOING IT WITHOUT STREAMLIT rhPath = input("Enter RH and Temperature File (.csv or .xlsx)")
with st.spinner('Calculating...'):
  time.sleep(5)
  perfEstFuncPolynom(rhPath)