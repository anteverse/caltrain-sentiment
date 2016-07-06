from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import pika
import json
import config


f = file('config.conf')
cfg = config.Config(f)

# Credentials
access_token = cfg.sentiment.access_token
access_token_secret = cfg.sentiment.access_token_secret
consumer_key = cfg.sentiment.consumer_key
consumer_secret = cfg.sentiment.consumer_secret

connection = pika.BlockingConnection(pika.ConnectionParameters(host=cfg.sentiment.RABBIT_HOST))
channel = connection.channel()
channel.queue_declare(queue=cfg.sentiment.QUEUE_TOPIC)


class Listener(StreamListener):

    def on_data(self, data):
        decoded = json.loads(data)

        if "text" in decoded:
            if decoded["text"]:
                text = decoded["text"]

                channel.basic_publish(exchange='', routing_key=cfg.sentiment.QUEUE_TOPIC, body=json.dumps({"text": text}))

                print 'SENT to consumer: ' + json.dumps({"text": text})

        return True

    def on_error(self, status_code):
        if status_code == 420:
            # disconnecting
            return False


if __name__ == '__main__':
    # twitter auth
    l = Listener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    # get english tweets with #caltrain hashtags and more
    stream.filter(languages=["en"], track=["caltrain"])