import json
import logging
import pprint
import sys

import bson
import click
import requests
from kafka import KafkaConsumer, TopicPartition


@click.command()
@click.option('--kafka-server', envvar='KAFKA_SERVER', required=True)
@click.option('--follow/--no-follow', '-f', help='indefinitely follow event stream')
@click.option('--rewind', '-r', default=-1, help='rewind the stream')
@click.option('--filter', default=None, help='filter by event type')
@click.option('--forward', default=None, help='forward event to http hook')
@click.option('--debug/--no-debug', '-d', help='debug logging')
def show_last_events(kafka_server, rewind=-1, follow=False, debug=False, filter=None, forward=None):
    """Simple program that shows last events from gitlab queue."""
    consumer_timeout_ms = 1000
    if debug:
        logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    if follow:
        consumer_timeout_ms = -1
    consumer = KafkaConsumer(fetch_max_wait_ms=10000, consumer_timeout_ms=consumer_timeout_ms,
                             bootstrap_servers=[kafka_server])
    partition = TopicPartition('gitlabbson', 0)
    consumer.assign([partition])
    if rewind == -1:
        consumer.seek_to_beginning()
    else:
        consumer.seek_to_end()
        consumer.seek(partition, consumer.position(partition) - rewind)
    for x in consumer:
        event = bson.loads(x.value)
        if filter is not None:
            typ = event.get('object_kind', event.get('event_name'))
            if typ != filter:
                continue
        pprint.pprint(event)
        if forward is not None:
            event = json.loads(json.dumps(event).replace(":30302/", "/"))
            r = requests.post(forward, json=event)
            print(r.content)
if __name__ == '__main__':
    show_last_events()
