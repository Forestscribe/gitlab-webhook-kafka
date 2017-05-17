gitlab-webhook-kafka
==

This is a simple flask application for forwarding gitlab event hooks to Kafka for independent consumption.

Simplifies Gitlab configuration and make the event stream more reliable.

    (GitLab)  == HTTP ==> (gitlab-webhook-kafka) ==> (kafka)


    (kafka)  =>  BuildBot
    (kafka)  =>  DependencyFetcherBot
    (kafka)  =>  PRBot
    (kafka)  =>  MetricsBot

Development
==

This project uses pipenv

    # once
    sudo pip install pipenv
    pipenv install


Tests
==

Tests are e2e only, and based on pytest. They need an available Kafka development server

    export KAFKA_SERVER=<ip>:<port>
    pytests


show_last_events.py
==

This script is a test script which shows the list of events recorded in kafka.
Also shows basic usage on how to fetch a topic from kafka

    export KAFKA_SERVER=<ip>:<port>
    python show_last_events.py
