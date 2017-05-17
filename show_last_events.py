import logging
import sys

import click
from kafka import KafkaConsumer, TopicPartition


@click.command()
@click.option('--kafka-server', envvar='KAFKA_SERVER', required=True)
@click.option('--follow/--no-follow', '-f', help='indefinitely follow event stream')
@click.option('--debug/--no-debug', '-d', help='debug logging')
def show_last_events(kafka_server, follow=False, debug=False):
    """Simple program that shows last events from gitlab queue."""
    consumer_timeout_ms = 100
    if debug:
        logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    if follow:
        consumer_timeout_ms = -1
    consumer = KafkaConsumer(fetch_max_wait_ms=10000, consumer_timeout_ms=consumer_timeout_ms,
                             bootstrap_servers=[kafka_server])
    consumer.assign([TopicPartition('gitlabjson', 0)])
    consumer.seek_to_beginning()
    for x in consumer:
        click.echo(x.value)

if __name__ == '__main__':
    show_last_events()
