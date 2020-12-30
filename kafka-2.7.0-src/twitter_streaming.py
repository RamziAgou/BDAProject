import json
import sys
import tweepy
from kafka import KafkaProducer, KafkaClient
from tweepy import OAuthHandler, Stream, API
from tweepy.streaming import StreamListener

class TstreamListener(StreamListener):
    def __init__(self, api):
        self.api = api
        super(StreamListener, self).__init__()
        client = KafkaClient("localhost:9092")
        self.producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'))

    def on_data(self, data):
        """
        Called whenever new data arrives as live stream
        """
        text = json.loads(data)['text'].encode('utf-8')
        print(text)
        try:
            self.producer.send('twitterstream', data)
        except Exception as e:
            print (e)
            return False
        return True

    def on_error(self, status_code):
        print ("Error received in kafka producer")
        return True #don't kill the stream

    def on_timeout(self):
        return True

if __name__ == '__main__':


    consumer_key = 'clcZktfmstM96nvSUjFHAa3fX'
    consumer_secret = 'c6cQyak6noAb7lFc3j2dXbNHU9IGzzFen0v1EFo6p0cbfz2BDy'
    access_token = '1273089309738926086-EGo5KbNZc37wrnjLpxClo56CVMh2n6'
    access_token_secret = 'f4lFuCCAB9ApFD0pjELNUYPYdUmOFIAOl85cqW0qn7Awk'

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = API(auth)
    
    stream = Stream(auth, listener=TstreamListener(api))
 
    tracked = ['Vaccine']
    stream.filter(track=tracked, languages=['en'])
