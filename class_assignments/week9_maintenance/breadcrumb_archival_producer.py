#!/usr/bin/env python

import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import json
import time

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: value = {value:12}".format(
                topic=msg.topic(), value=msg.value().decode('utf-8')))

    # Produce data by selecting random values from these lists.
    topic = "archivetest"
    file_read = open('/home/velapure/sample_data/bcsample.json')
    sample_json_data = json.load(file_read)
    
    count = 0
    for each_data in sample_json_data:

        each_data_in_bytes = json.dumps(each_data)
        producer.produce(topic, each_data_in_bytes, callback = delivery_callback)
        count += 1

        if count%5 == 0:
           time.sleep(2)
        if count%15 == 0:
            producer.flush()

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()
