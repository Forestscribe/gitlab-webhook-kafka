import json
import logging
import os
import sys

import bson
from flask import Flask, abort, request
from kafka import KafkaProducer

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers=os.environ["KAFKA_SERVER"])
TOPIC_PREFIX = os.environ.get("TOPIC_PREFIX", "")


@app.route("/", methods=["POST"])
def hook():
    TOKEN = os.environ.get("XGitlabToken")
    if TOKEN is not None:
        if request.headers.get('X-Gitlab-Token') != TOKEN:
            abort(401)
            return
    content = request.get_json(silent=True)
    js = json.dumps(content)
    producer.send(TOPIC_PREFIX + 'gitlabjson', js)
    producer.flush()
    bs = bson.dumps(content)
    producer.send(TOPIC_PREFIX + 'gitlabbson', bs)
    producer.flush()
    return "OK"


@app.route("/", methods=["GET"])
def ready():
    return "OK"

if __name__ == "__main__":
    app.run(host="0.0.0.0")
