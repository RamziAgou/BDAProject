# BDEProject

This project is a engineering student's project. It's a big data ecosystem project.
The goal of this project is to use big data tools to create and train a prediction model. In our case, we chose Twitter Sentiment Analysis.
During this project, we used some tools like Kafka and Spark.

## Prerequisites

For this project you will need to install and setup some tools to use all of the aspect of the project.
First of all we were on a Linux operating system, but it might also works easily on MAC.

- The Kafka installation folder is already on the github, you just have to clone the repository.
- For running Kafka, first, we need to start the zookeeper and then Kafka server :
- -  Run Zookeeper : in the Kafka installation folder, use the following command to start the zookeeper server 

        > **./bin/zookeeper-server-start.sh config/zookeeper.properties**
        > Note : You can check the zookeeper.properties file if you want to change the client port (2181 by default)
        
- - Start Kafka server: Use the following command to run Kafka
        
     > **./bin/kafka-server-start.sh config/server.properties**
     > Note : You can also check the server.properties file if you want to change the zookeeper to connect, or the directory of the logs files etc...
     
    
