import json
import logging
import os
import sys
import time
from subprocess import Popen

import bson
import pytest
import requests
from kafka import KafkaConsumer

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


@pytest.fixture(scope="module")
def application():
    print("starting application")
    env = os.environ.copy()
    env['TOPIC_PREFIX'] = "test"
    p = Popen(["python", "application.py"], env=env)
    up = False
    while not up:
        try:
            requests.get("http://localhost:5000")
            up = True
        except Exception:
            time.sleep(.1)
    yield p  # provide the fixture value
    p.terminate()


def test_json(application):
    consumer = KafkaConsumer('testgitlabjson', group_id='test', fetch_max_wait_ms=100,
                             bootstrap_servers=[os.environ['KAFKA_SERVER']])
    partitions = consumer.poll(1000)
    requests.post("http://localhost:5000", json={'a': 1, 'b': 2})
    partitions = consumer.poll(10000)
    message = partitions.values()[0][0]
    assert message.topic == 'testgitlabjson'
    assert json.loads(message.value) == dict(a=1, b=2)


def test_bson(application):
    consumer = KafkaConsumer('testgitlabbson', fetch_max_wait_ms=100, group_id='test',
                             bootstrap_servers=[os.environ['KAFKA_SERVER']])
    partitions = consumer.poll(1000)
    requests.post("http://localhost:5000", json={'a': 1, 'b': 2})
    partitions = consumer.poll(10000)
    message = partitions.values()[0][0]
    assert message.topic == 'testgitlabbson'
    assert bson.loads(message.value) == dict(a=1, b=2)
