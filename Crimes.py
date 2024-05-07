########################################
#       STENTOUMIS NIKOLAOS            #
#                                      #
########################################
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import time
import csv

# Get When The App Started
StartTime = time.perf_counter()

# Creating The Output Path Where The CSVs And PNGs Will Be Stored
OutputPath = os.getcwd()
OutputPath = OutputPath + "/SparkExports"

# Creating Spark Session
print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), "Creating Spark Session")
try:
    spark = SparkSession.builder.appName("LACrimes").getOrCreate()
except Exception as e:
    print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), f"Error Creating Spark Session: {e}")
    EndTime = time.perf_counter()
    ExecutionTime = EndTime - StartTime
    print(f"Execution time: {ExecutionTime} seconds")
    exit()

spark.sparkContext.setLogLevel("WARN")

# Declaring The Schema
print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), "Declaring Schema")
Areas_Schema = StructType([
    StructField("area_id", IntegerType(), True),
    StructField("area", StringType(), True)
])

Premises_Schema = StructType([
    StructField("premis_id", IntegerType(), True),
    StructField("premis_desc", StringType(), True)
])

Crimes_Schema = StructType([
    StructField("crime_id", IntegerType(), True),
    StructField("crime_desc", StringType(), True)
])

Weapons_Schema = StructType([
    StructField("weapon_id", IntegerType(), True),
    StructField("weapon", StringType(), True)
])

VictimDescent_Schema = StructType([
    StructField("descent_id", StringType(), True),
    StructField("descent", StringType(), True)
])

CaseStatus_Schema = StructType([
    StructField("status_id", StringType(), True),
    StructField("status_desc", StringType(), True)
])

CriminalCases_Schema = StructType([
    StructField("case_id", IntegerType(), True),
    StructField("date_occured", DateType(), True),
    StructField("area_id", IntegerType(), True),
    StructField("crime_id", IntegerType(), True),
    StructField("victim_age", IntegerType(), True),
    StructField("victim_sex", StringType(), True),
    StructField("victim_descent_id", StringType(), True),
    StructField("premis_id", IntegerType(), True),
    StructField("Weapon_used_id", IntegerType(), True),
    StructField("case_status_id", StringType(), True)
])

# CSV Files Use '|' As delimiter
delimiter = "|"

# Creating DataFrames From Dataset
print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), "Reading The Dataset")
try:
    Areas_df = spark.read.option("delimiter", delimiter).schema(Areas_Schema).csv("areas.csv", header=True)
    Crimes_df = spark.read.option("delimiter", delimiter).schema(Crimes_Schema).csv("crimes.csv", header=True)
    Premises_df = spark.read.option("delimiter", delimiter).schema(Premises_Schema).csv("premises.csv", header=True)
    Weapons_df = spark.read.option("delimiter", delimiter).schema(Weapons_Schema).csv("weapons.csv", header=True)
    VictimDescent_df = spark.read.option("delimiter", delimiter).schema(VictimDescent_Schema).csv("victim_descent.csv", header=True)
    CaseStatus_df = spark.read.option("delimiter", delimiter).schema(CaseStatus_Schema).csv("case_status.csv", header=True)
    CriminalCases_df = spark.read.option("delimiter", delimiter).schema(CriminalCases_Schema).csv("criminal_cases.csv", header=True)
except Exception as e:
    print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), f"Error Reading From CSV Files: {e}")
    try:
        spark.stop()
        EndTime = time.perf_counter()
        ExecutionTime = EndTime - StartTime
        print(f"Execution time: {ExecutionTime} seconds")
        exit()
    except Exception as e:
        print(f"Error Stopping Spark Session: {e}")
        EndTime = time.perf_counter()
        ExecutionTime = EndTime - StartTime
        print(f"Execution time: {ExecutionTime} seconds")
        exit()

# Creating Tables From DataFrames
print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), "Creating Tables From DataFrames")
try:
    Areas_df.createOrReplaceTempView("Areas")
    Crimes_df.createOrReplaceTempView("Crimes")
    Premises_df.createOrReplaceTempView("Premises")
    Weapons_df.createOrReplaceTempView("Weapons")
    VictimDescent_df.createOrReplaceTempView("VictimDescent")
    CaseStatus_df.createOrReplaceTempView("CaseStatus")
    CriminalCases_df.createOrReplaceTempView("CriminalCases")
except Exception as e:
    print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), f"Error Creating Tables From DataFrames: {e}")
    try:
        spark.stop()
        EndTime = time.perf_counter()
        ExecutionTime = EndTime - StartTime
        print(f"Execution time: {ExecutionTime} seconds")
        exit()
    except Exception as e:
        print(f"Error Stopping Spark Session: {e}")
        EndTime = time.perf_counter()
        ExecutionTime = EndTime - StartTime
        print(f"Execution time: {ExecutionTime} seconds")
        exit()

# Queries Definitions
CrimesPerAreaQuery = """
SELECT area AS Area, premis_desc AS Premise, COUNT(Premises.premis_id) AS NumberOfCrimes
FROM CriminalCases
JOIN Premises ON  CriminalCases.premis_id = Premises.premis_id
JOIN Areas ON Areas.area_id = CriminalCases.area_id 
GROUP BY area, premis_desc
ORDER BY area, NumberOfCrimes DESC
"""

TopTenCrimeTypesQuery = """
SELECT crime_desc AS Top10Crimes, COUNT(crime_desc) AS NumberOfCrimes
FROM CriminalCases
JOIN Crimes ON CriminalCases.crime_id = Crimes.crime_id
GROUP BY crime_desc 
ORDER BY NumberOfCrimes DESC LIMIT 10
"""

CrimesPerMonthQuery = """
SELECT YEAR(date_occured) AS Year, MONTH(date_occured) AS Month, COUNT(MONTH(date_occured)) AS CrimesPerMonth
FROM CriminalCases
GROUP BY YEAR(date_occured), MONTH(date_occured)
ORDER BY Year, Month
"""

CaseStatusPerCrimeQuery = """
SELECT crime_desc AS Crime, status_desc AS CaseStatus, COUNT(status_desc) AS NumberOfCrimes
FROM CriminalCases
JOIN Crimes ON CriminalCases.crime_id = Crimes.crime_id
JOIN CaseStatus ON CriminalCases.case_status_id = CaseStatus.status_id
GROUP BY crime_desc, status_desc
ORDER BY crime_desc, status_desc
"""

CrimesPerCountryGenderAgeQuery = """
SELECT descent AS Descent, victim_sex AS Sex, victim_age AS Age, COUNT(crime_id) AS NumberOfCrimes
FROM CriminalCases
JOIN VictimDescent ON CriminalCases.victim_descent_id = VictimDescent.descent_id
GROUP BY CUBE(descent, victim_sex, victim_age)
"""
# Processing Queries2
print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), "Processing Queries")
try:
    CrimesPerArea = spark.sql(CrimesPerAreaQuery)
    time.sleep(1)
    CrimesPerArea.createOrReplaceTempView("CrimesPerAreaTBL")

    TopTenCrimeTypes = spark.sql(TopTenCrimeTypesQuery)
    time.sleep(1)
    TopTenCrimeTypes.createOrReplaceTempView("TopTenCrimeTypesTBL")

    CrimesPerMonth = spark.sql(CrimesPerMonthQuery)
    time.sleep(1)
    CrimesPerMonth.createOrReplaceTempView("CrimesPerMonthTBL")

    CaseStatusPerCrime = spark.sql(CaseStatusPerCrimeQuery)
    time.sleep(1)
    CaseStatusPerCrime.createOrReplaceTempView("CaseStatusPerCrimeTBL")

    CrimesPerCountryGenderAge = spark.sql(CrimesPerCountryGenderAgeQuery)
    time.sleep(1)
    CrimesPerCountryGenderAge.createOrReplaceTempView("CrimesPerCountryGenderAgeTBL")

    CrimesPerArea.show()
    TopTenCrimeTypes.show()
    CrimesPerMonth.show()
    CaseStatusPerCrime.show()
    CrimesPerCountryGenderAge.show()
except Exception as e:
    print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), f"Error Processing Queries:{e}")
    try:
        spark.stop()
        EndTime = time.perf_counter()
        ExecutionTime = EndTime - StartTime
        print(f"Execution time: {ExecutionTime} seconds")
        exit()
    except Exception as e:
        print(f"Error Stopping Spark Session: {e}")
        EndTime = time.perf_counter()
        ExecutionTime = EndTime - StartTime
        print(f"Execution time: {ExecutionTime} seconds")
        exit()

# Creating Charts
print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), "Creating Charts")
try:
    # Files To Save Plots
    TopTenCrimesPlotName = "TopTenCrimesPlot.png"
    CrimesPerMonthPlotName = "CrimesPerMonthPlot.png"
    TopTenCrimesPlot = os.path.join(OutputPath, TopTenCrimesPlotName)
    CrimesPerMonthPlot = os.path.join(OutputPath, CrimesPerMonthPlotName)

    # Checking If Plot Files Already Exist
    if os.path.exists(TopTenCrimesPlot):
        os.remove(TopTenCrimesPlot)
    if os.path.exists(CrimesPerMonthPlot):
        os.remove(CrimesPerMonthPlot)

    # DataFrame To Pandas DataFrame
    TopTenCrimesPandas = TopTenCrimeTypes.toPandas()
    # Creating Plot
    plt.figure(figsize=(15, 6))
    plt.bar(TopTenCrimesPandas["Top10Crimes"], TopTenCrimesPandas["NumberOfCrimes"], color='Purple')

    # Declaring Labels
    plt.xlabel('Crime Types')
    plt.ylabel('Number of Crimes')
    plt.title('Top Ten Crimes')

    # Rotating For Better Readability
    plt.xticks(rotation=10, ha='right', fontsize=6)
    # Saving Plot
    plt.savefig(TopTenCrimesPlot)
    # Showing Plot
    plt.show(block=False)
    plt.pause(2)

    # DataFrame To Pandas DataFrame
    CrimesPerMonthPandas = CrimesPerMonth.toPandas()
    # Transforming DataFrame To A Matrix
    heatmap_data = CrimesPerMonthPandas.pivot_table('CrimesPerMonth', 'Month', 'Year', fill_value=0)
    heatmap_data = heatmap_data.applymap(lambda x: round(x) if pd.notnull(x) else x)

    # Creating Plot
    plt.figure(figsize=(8, 6))
    table = plt.table(cellText=heatmap_data.values,
                      colLabels=heatmap_data.columns,
                      rowLabels=heatmap_data.index,
                      loc='center', cellLoc='center', colColours=['#ffff00']*len(heatmap_data.columns))

    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.2)

    plt.axis('off')
    plt.title('CrimesPerMonth')
    # Saving Plot
    plt.savefig(CrimesPerMonthPlot)
    # Showing Plot
    plt.show(block=False)
    plt.pause(2)
except Exception as e:
    print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), f"Error Creating Charts: {e}")

# Checking If Export Folder Exists. If Not Create It
print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), "Checking If Export Folder Exists")
if not os.path.exists(OutputPath):
    print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), "Export Folder Does Not Exist. I Am Going To Create It")
    try:
        os.makedirs(OutputPath)
    except Exception as e:
        print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), f"Error Creating Export Folder: {e}")
        try:
            spark.stop()
            EndTime = time.perf_counter()
            ExecutionTime = EndTime - StartTime
            print(f"Execution time: {ExecutionTime} seconds")
            exit()
        except Exception as e:
            print(f"Error Stopping Spark Session: {e}")
            EndTime = time.perf_counter()
            ExecutionTime = EndTime - StartTime
            print(f"Execution time: {ExecutionTime} seconds")
            exit()

# Declaring CSV FilesNames
CrimesPerAreaFileName = "CrimesPerArea.csv"
TopTenCrimeTypesFileName = "TopTenCrimes.csv"
CrimesPerMonthFileName = "CrimesPerMonth.csv"
CaseStatusPerCrimeFileName = "CaseStatusPerCrime.csv"
CrimesPerCountryGenderAgeFileName = "CrimesPerCountryGenderAge.csv"

# Declaring CSVs Full Path
CrimesPerAreaFile = os.path.join(OutputPath, CrimesPerAreaFileName)
TopTenCrimeTypesFile = os.path.join(OutputPath, TopTenCrimeTypesFileName)
CrimesPerMonthFile = os.path.join(OutputPath, CrimesPerMonthFileName)
CaseStatusPerCrimeFile = os.path.join(OutputPath, CaseStatusPerCrimeFileName)
CrimesPerCountryGenderAgeFile = os.path.join(OutputPath, CrimesPerCountryGenderAgeFileName)

# Checking If CSV Files Already Exist. If They Do Remove Them First
try:
    if os.path.exists(CrimesPerAreaFile):
        os.remove(CrimesPerAreaFile)
    if os.path.exists(TopTenCrimeTypesFile):
        os.remove(TopTenCrimeTypesFile)
    if os.path.exists(CrimesPerMonthFile):
        os.remove(CrimesPerMonthFile)
    if os.path.exists(CaseStatusPerCrimeFile):
        os.remove(CaseStatusPerCrimeFile)
    if os.path.exists(CrimesPerCountryGenderAgeFile):
        os.remove(CrimesPerCountryGenderAgeFile)
except Exception as e:
    print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), f"Error Removing One Or More CSVs: {e}")
    try:
        spark.stop()
        EndTime = time.perf_counter()
        ExecutionTime = EndTime - StartTime
        print(f"Execution time: {ExecutionTime} seconds")
        exit()
    except Exception as e:
        print(f"Error Stopping Spark Session: {e}")
        EndTime = time.perf_counter()
        ExecutionTime = EndTime - StartTime
        print(f"Execution time: {ExecutionTime} seconds")
        exit()

# Exporting Data
print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), "Exporting Query Results To CSVs")
try:
    CrimesPerArea.write.csv(OutputPath, header=True, mode="append")
    TopTenCrimeTypes.write.csv(OutputPath, header=True, mode="append")
    CrimesPerMonth.write.csv(OutputPath, header=True, mode="append")
    CaseStatusPerCrime.write.csv(OutputPath, header=True, mode="append")
    CrimesPerCountryGenderAge.write.csv(OutputPath, header=True, mode="append")

    # Removing _SUCCESS File
    SuccessFileName = "_SUCCESS"
    SuccessFile = os.path.join(OutputPath, SuccessFileName)
    if os.path.exists(SuccessFile):
        os.remove(SuccessFile)
    files = os.listdir(OutputPath)

    # Keywords To Search For In CSVs To Understand The File Based In Its Contents
    KeyWord1 = "Descent"
    KeyWord2 = "CaseStatus"
    Keyword3 = "Year"
    KeyWord4 = "Top10Crimes"
    KeyWord5 = "Premise"

    # Iterate Through Files Delete .crc Files And Rename .csv Files
    for file in files:

        # If Corresponding File Is Detected Set To True
        CrimesPerAreaBL = False
        TopTenCrimesBL = False
        CrimesPerMonthBL = False
        CaseStatusPerCrimeBL = False
        CrimesPerCountryGenderAgeFileBL = False

        # If File EndsWith .crc Remove It
        if file.endswith('.crc'):
            FilePath = os.path.join(OutputPath, file)
            os.remove(FilePath)
        # If Ends With .csv Rename It
        elif file.endswith('.csv'):
            CurrentFilePath = os.path.join(OutputPath, file)
            with open(CurrentFilePath, "r") as csv_file:
                csv_reader = csv.reader(csv_file)

                # Getting Header Of CSV
                header = next(csv_reader, None)

                # For Each Column Check For Specific Words That Identifies Each File
                if header:
                    for column in header:
                        if KeyWord1 in column:
                            CrimesPerCountryGenderAgeFileBL = True
                            break
                        elif KeyWord2 in column:
                            CaseStatusPerCrimeBL = True
                            break
                        elif Keyword3 in column:
                            CrimesPerMonthBL = True
                            break
                        elif KeyWord4 in column:
                            TopTenCrimesBL = True
                            break
                        elif KeyWord5 in column:
                            CrimesPerAreaBL = True
                            break
            # Depending On Which File Is Iterated Give It Correct Name
            if CrimesPerAreaBL:
                current_file_path = os.path.abspath(CurrentFilePath)
                new_file_path = os.path.abspath(CrimesPerAreaFile)
                os.rename(current_file_path, new_file_path)
            elif TopTenCrimesBL:
                current_file_path = os.path.abspath(CurrentFilePath)
                new_file_path = os.path.abspath(TopTenCrimeTypesFile)
                os.rename(current_file_path, new_file_path)
            elif CrimesPerMonthBL:
                current_file_path = os.path.abspath(CurrentFilePath)
                new_file_path = os.path.abspath(CrimesPerMonthFile)
                os.rename(current_file_path, new_file_path)
            elif CaseStatusPerCrimeBL:
                current_file_path = os.path.abspath(CurrentFilePath)
                new_file_path = os.path.abspath(CaseStatusPerCrimeFile)
                os.rename(current_file_path, new_file_path)
            elif CrimesPerCountryGenderAgeFileBL:
                current_file_path = os.path.abspath(CurrentFilePath)
                new_file_path = os.path.abspath(CrimesPerCountryGenderAgeFile)
                os.rename(current_file_path, new_file_path)
except Exception as e:
    print(f"Error Exporting Data: {e}")
    try:
        spark.stop()
        EndTime = time.perf_counter()
        ExecutionTime = EndTime - StartTime
        print(f"Execution time: {ExecutionTime} seconds")
        exit()
    except Exception as e:
        print(f"Error Stopping Spark Session: {e}")
        EndTime = time.perf_counter()
        ExecutionTime = EndTime - StartTime
        print(f"Execution time: {ExecutionTime} seconds")
        exit()


# Stopping Spark Session
print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), "Stopping Spark Session")
try:
    spark.stop()
    EndTime = time.perf_counter()
    ExecutionTime = EndTime - StartTime
    print(f"Execution time: {ExecutionTime} seconds")
except Exception as e:
    print(f"Error Stopping Spark Session: {e}")
    EndTime = time.perf_counter()
    ExecutionTime = EndTime - StartTime
    print(f"Execution time: {ExecutionTime} seconds")