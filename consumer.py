import re
import pika
import config
import json
from datetime import datetime
from textblob import TextBlob


# import shared config
f = file('config.conf')
cfg = config.Config(f)

# Rabbit channel listener
connection = pika.BlockingConnection(pika.ConnectionParameters(host=cfg.sentiment.RABBIT_HOST))
channel = connection.channel()
channel.queue_declare(queue=cfg.sentiment.QUEUE_TOPIC)

# connection = pika.BlockingConnection(pika.ConnectionParameters(host=cfg.sentiment.RABBIT_HOST))
channel2 = connection.channel()
channel2.queue_declare(queue=cfg.sentiment.PREPARING_QUEUE)

# issue tracker
tracker = {}


def callback(ch, method, properties, body):
    decoded = json.loads(body)
    print(" [x] Received %r" % body)

    # text of the tweet in 'text' key
    if 'text' not in decoded:
        print(" [-] Invalid JSON")
    else:
        # Valid json

        # acknowlegde delivery
        ch.basic_ack(delivery_tag=method.delivery_tag)

        # sentiment analysis
        testimonial = TextBlob(decoded['text'])

        if testimonial.sentiment.polarity < 0:
            # mtweet is negative, but that doesn't mean there's a delay
            # gathering info
            direction = {'NB':0, 'SB':0}
            if 'SB' in decoded['text']:
                direction['SB'] = 1
            if 'NB' in decoded['text']:
                direction['NB'] = 1

            train = ''
            m = re.match('\d{3}', decoded['text'])
            if m:
                print m.group(0)
                train = m.group(0)

            message = json.dumps({
                'direction': direction,
                'train': train,
                'subjectivity': testimonial.sentiment.subjectivity,
                'polarity': testimonial.sentiment.polarity,
                'text': decoded['text']
            })

            channel2.basic_publish(exchange='', routing_key=cfg.sentiment.PREPARING_QUEUE, body=message)


if __name__ == "__main__":
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback, queue=cfg.sentiment.QUEUE_TOPIC, no_ack=False)
    channel.start_consuming()