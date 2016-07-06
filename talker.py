import pika
import config
import json
import operator
from datetime import datetime


# import shared config
f = file('config.conf')
cfg = config.Config(f)

# Rabbit channel listener
connection = pika.BlockingConnection(pika.ConnectionParameters(host=cfg.sentiment.RABBIT_HOST))
channel = connection.channel()
channel.queue_declare(queue=cfg.sentiment.PREPARING_QUEUE)

# issue tracker
tracker = {}

# message tracker


def sanitize_tracker():
    return {key: tracker[key] for key in tracker if key >= (datetime.utcnow() - datetime.timedelta(hours=1)).isoformat()}


def eval_density():
    if len(tracker) > 1:
        times = tracker.keys()
        last30 = [k for k in times if k >= (datetime.utcnow() - datetime.timedelta(minutes=30)).isoformat()]
        last15 = [k for k in last30 if k >= (datetime.utcnow() - datetime.timedelta(minutes=15)).isoformat()]
        if len(last15) >= len(tracker) / 4:
            return 2
        if len(last30) >= len(tracker) / 2:
            return 1
    return 0


def find_train_that_has_issue():
    _dic = {}
    for v in tracker.values():
        if not v.train == '':
            if v.train not in _dic:
                _dic[v.train] = 0
            _dic[v.train] += 1
    if not _dic == {}:
        return sorted(_dic.items(), key=operator.itemgetter(1), reverse=True)[0]
    return ''


def callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)

    decoded = json.loads(body)
    global tracker

    # sanitize
    tracker = sanitize_tracker()

    # add new entry
    now = datetime.utcnow()
    global tracker
    if tracker[now.isoformat()]:
        now = now + datetime.timedelta(seconds=1)

    tracker[now.isoformat()] = decoded
    return


if __name__ == "__main__":
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback, queue=cfg.sentiment.PREPARING_QUEUE, no_ack=False)
    channel.start_consuming()