# BDEProject

This project is a engineering student's project. It's a big data ecosystem project.
The goal of this project is to use big data tools to create and train a prediction model. In our case, we chose Twitter Sentiment Analysis.
During this project, we used some tools like Kafka and Spark.

## What to do

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
     
    
- Now to access Twitter streaming APIs, we need to sign in for Twitter developer account and get the following OAuth authentification details : 
- - CustomerKey
- - CustomerSecret
- - AccessToken
- - AccessTokenSecret

> We let our authentification here for you M.LEONARD but we tend to delete it after your correction

- Now you can already see your streaming flow with the following command (still on the kafka installation folder). You should see tweets displaying on your console now: 

> python twitter_streaming.py

> Note : You will maybe need to install some packages depend of what you don't have. If you get error with "No module named kafka" for example you just have to pip install kafka

- Here the topic is twitterstream, if you want to change its name you can change it in the **[twitter_streaming.py]**(kafka-2.7.0-src/twitter_streaming.py). You can also change the subject of the search, we use "Vaccine" but it can be everything else.

- - Now that Kafka streaming is working, we have to make Spark connecting to this stream :
- - To that you just have the execute the following command (this time in your base project directory): 

     > python3 spark_streaming.py
     
     > Note : You will maybe need to install some packages depend of what you don't have. If you get error with "No module named tweepy" for example you just have to pip3 install tweepy
     
- Now you should see tweets and just below the beginning of the tweet (often the user's name) and its prediction. For the prediction you should know that 0 = negative sentiment, 2 = neutral sentiment, 4 = positive sentiment. All the tweets and prediction are saved in the **[stream_data]**(stream_data) folder.


## More 

In the github you also have the model that we trained in the **[train_model.py]**(train_model.py).

The data we get from kaggle, here is the link https://www.kaggle.com/kazanova/sentiment140
