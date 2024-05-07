# Data Analysis Using Spark

This is the documentation of the project, explaining the main functions and the methodology followed. The basic script with all the requests is Crimes.py
The main parts of the task are shown in the list below in the order in which they are executed.

- Declaration of Schema
- Reading data from CSV files
- Create tables from the data read from CSV files.
- Declaration of SQL Queries
- Executing SQL Queries
- Creating the requested Plots and saving them
- Creating CSV and saving the results returned by the Queries 

The application creates the schema and reads the CSV files from the folder in which it is running. That is, it does not need to be given a path, it assumes that the files are in the folder in which the initial execution is performed. It is worth mentioning here that the application performs error checking at every important step, which means that if something is not completed correctly during reading, it will stop by printing the corresponding error message. 

Once the read is completed, it creates the corresponding tables from the DataFrames created during the read process, to be used for executing the queries.

The next step is to declare the queries and then execute them in the same order as the job requests. 
Once the part of executing the queries is successfully completed, the next step is to create the plots for two of the queries. A bar chart was chosen for the 2nd query and a table (after converting the DataFrame to a matrix), as they were considered ideal for representing the specific data.

All the data(plots and query results), are stored in a folder created when the application is run called SparkExports, inside the folder in which the application is initially run. Before creating them, a check is performed to see if the files already exist. If they exist, they are deleted and new ones are created. The files are named to indicate their contents. Because the name given by SPARK is random, after the creation of each CSV a parse of each header is performed and depending on its content a corresponding renaming is done. In addition, when SPARK creates the CSV files in which the results from the queries are stored, it also creates some additional files. These files are detected by the application and deleted so that only the desired files are in the SparkExports folder. 

Throughout its execution, the application prints each step that it is executing at any given time so that the user is aware of where in the execution it is.
