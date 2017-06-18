import hashlib
import logging
import sys

import bson
import click
import requests
from kafka import KafkaConsumer


@click.command()
@click.option('--kafka-server', envvar='KAFKA_SERVER', required=True)
@click.option('--forward', required=True, envvar='FORWARD_SERVER', help='forward event to http hook')
@click.option('--debug/--no-debug', '-d', help='debug logging')
def forward(kafka_server, debug=False, forward=None):
    """Simple program that forward events from gitlab queue."""
    if debug:
        logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

    server_hash = hashlib.sha1(forward).hexdigest()[:5]

    consumer = KafkaConsumer('gitlabbson', group_id="forwardevent" + server_hash, fetch_max_wait_ms=10000,
                             bootstrap_servers=[kafka_server])
    for x in consumer:
        event = bson.loads(x.value)
        typ = event.get('object_kind')
        if typ in ("push", "tag_push", "merge_request"):
            codebase = event.get("repository", event.get('target', {})).get("name")
            r = requests.post(forward, params={'codebase': codebase}, json=event)
        print(r.content)


if __name__ == '__main__':
    forward()
