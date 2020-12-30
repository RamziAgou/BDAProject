import json, os
import time, sys
from datetime import datetime
import findspark
# Add the streaming package and initialize
findspark.add_packages(["org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0"])
findspark.init()
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from predictor import Predictor
from pyspark.sql import Row

#trending topic extraction
from nltk import pos_tag
from nltk.tokenize import word_tokenize, RegexpTokenizer

def predict_store(time, rdd):
    
    label = { 0 : 'negative', 2 : 'neutral', 4 : 'positive'}
    print("========= %s =========" % str(time))
    if not rdd.isEmpty():
        # Convert RDD[String] to RDD[Row] to DataFrame
        cols = ['id', 'screen_name', 'text', 'followers', 'created_at']
        rowRdd = rdd.map(lambda w: Row(id=w['id'], screen_name=w['screen_name'], \
                                       text=w['text'], followers=w['followers'], created_at=w['created_at']))

        df = spark.createDataFrame(rowRdd)
        #make prediction use saved model
        prediction = pred.predict(df)
        prediction.select('text', 'prediction').show()

        #save as jsonl.gz
        path = 'stream_data/{}'.format(os.environ['Today'])
        #hadoop doesn't support colon in file naming
        convert_time = time.strftime('%H-%M-%S')
        despath = os.path.join(path, "{}.jsonl.gz".format(convert_time))
        prediction.toJSON().saveAsTextFile(despath, 'org.apache.hadoop.io.compress.GzipCodec')

        #save to mongodb for publishing
        new_cols = ['id', 'screen_name', 'text', 'followers','created_at', 'prediction']
        convert_dict = lambda x: {d:x[d] for d in new_cols}
        pred_rdd = prediction.rdd.map(convert_dict)
    else:
        print("erreur vide")

def find_trends(text):
    words = []
    for w in text.split():
        if not w.startswith('@') and not w.startswith('#') and w != 'RT':
            words.append(w)
    words = tokenizer.tokenize((" ").join(words))
    word_tags = pos_tag(words)
    return [word for word, tag in word_tags if tag == 'NN']


if __name__ == "__main__":
    #setting up environment
    APP_NAME = 'Twitter Sentiment Analysis'
    os.environ['Today'] = datetime.now().strftime('%Y-%m-%d')
    PERIOD=20
    BROKERS='localhost:9092'
    TOPIC= 'twitterstream'
    duration=100
    try:
        sc
    except:
        conf = SparkConf().set("spark.default.paralleism", 1)
        spark = pyspark.sql.SparkSession.builder \
                                        .master("local[4]") \
                                        .appName(APP_NAME) \
                                        .config(conf=conf)  \
                                        .getOrCreate()
        sc = spark.sparkContext
        #create a streaming context with batch interval 10 sec

    #initialize predictor
    pred = Predictor()
    tokenizer = RegexpTokenizer(r'\w+')

    #a new ssc needs to be started after a previous ssc is stopped
    ssc = StreamingContext(sc, PERIOD)

    #create stream receivers
    stream = KafkaUtils.createDirectStream(
              ssc,
              [TOPIC],
              {
                "metadata.broker.list": BROKERS,
              }
    )
    tweets = stream.map(lambda x: json.loads(x[1])).map(lambda x: json.loads(x))
    
    #filter commercials
    filtered_tweets = tweets.filter(lambda x: 'https' not in x['text'])
    # DataFrame operations inside your streaming program
    features = filtered_tweets.map(lambda x: {'id': x['id'], 'screen_name': x['user']['screen_name'], 'text': x['text'], 'followers': x['user']['followers_count'], 'created_at': x['created_at']})

    #tweets.pprint()
    features.pprint()

    #find trending topic
    #filtered_tweets.foreachRDD(lambda x: find_trends(x['text']))

    #predict and store feature extracted tweets
    features.foreachRDD(predict_store)
    ssc.start()

    try:
        stream_time = int(sys.argv[1])
    except:
        stream_time = 5

    time.sleep(stream_time*60)

    ssc.stop(stopSparkContext=False, stopGraceFully=True)