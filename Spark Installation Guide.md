# Spark Instuctions

Spark was installed and used on Ubuntu

## What is Spark?

Spark, short for Apache Spark, is an open-source, distributed computing system that is designed for big data processing and analytics. It was initially developed at the AMPLab at the University of California, Berkeley, and later donated to the Apache Software Foundation, which now maintains it.

Spark provides an interface for programming entire clusters with implicit data parallelism and fault tolerance. It supports a wide range of data processing tasks, including batch processing, real-time stream processing, machine learning, graph processing, and interactive SQL queries.

One of the key features of Spark is its in-memory computing capabilities, which allow it to perform computations much faster than traditional disk-based systems like Hadoop MapReduce. Spark can efficiently cache data in memory across multiple nodes in a cluster, reducing the need to read data from disk for subsequent computations.

## How To Install Spark

### Prerequisites

Ensure that python is installed.

* sudo apt update
* sudo apt install python3

### Main Installation

Visit Apache Spark website (https://spark.apache.org/downloads.html) and choose the latest stable version.

Copy the download link for the pre-built package for Hadoop and download it using wget like the example below. 

* wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz

Navigate to the directory where you downloaded Spark and extract it using tar

* tar -zxvf spark-<version>-bin-hadoop<version>.tgz

Then we move Spark to a preferred location

* sudo mv spark-<version>-bin-hadoop<version> /opt/spark

Then we have to set some enviroment variables. 

* Open the .bashrc file in your home directory using a text editor and add the following lines at the end of the file. 
    * export SPARK_HOME=/opt/spark   # Change this path if you moved Spark to a different location
    * export PATH=$PATH:$SPARK_HOME/bin
    * export PYSPARK_PYTHON=python3   # Specify Python version if necessary

Verify the installation by running the following command
* pyspark

## Starting Spark
First we start the Master node.
* start-master.sh

We can check the server's interface using the below method in a browser. 
* localhost:8080

Then we start a slave node. 
* start-slave.sh <master_node_hostname_or_ip>:7077


