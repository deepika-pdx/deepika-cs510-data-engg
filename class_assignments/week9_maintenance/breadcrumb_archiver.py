#!/usr/bin/env python

import sys
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
import json
import zlib
from google.cloud import storage
import os

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Create Consumer instance
    consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    topic = "archivetest"
    consumer.subscribe([topic], on_assign=reset_offset)
    breadcrumb_json_object_list = []
    msgs_saved_to_gcp_storage = False
    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
                if not msgs_saved_to_gcp_storage:
                    # Write breadcrumb data to file
                    breadcrumb_file = open("/home/velapure/data_archival/breadcrumb_data/breadcrumb_json_data.json", "wb")
                    breadcrumb_data_string = json.dumps([breadcrumb for breadcrumb in breadcrumb_json_object_list])
                    breadcrumb_file.write(breadcrumb_data_string.encode('utf-8'))
                    breadcrumb_file.close()

                    # Compress the file with the json objects
                    breadcrumb_data_to_be_stored = breadcrumb_data_string.encode('utf-8')
                    compressed_breadcrumb_data = zlib.compress(breadcrumb_data_to_be_stored, zlib.Z_BEST_COMPRESSION)
                    compress_ratio = (float(len(breadcrumb_data_to_be_stored)) - float(len(compressed_breadcrumb_data))) / float(len(breadcrumb_data_to_be_stored))
                    print('Compressed: %d%%' % (100.0 * compress_ratio))
                    
                    # Store breadcrumb data to GCP bucket
                    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'trimet-data-engg-project-98103ed97033.json'
                    storage_client = storage.Client()
                    bucket = storage_client.bucket("data-archival-bucket")
                    blob = bucket.blob("breadcrumb_archived_data")
                    with blob.open("wb") as f:
	                    f.write(compressed_breadcrumb_data)

                    msgs_saved_to_gcp_storage = True
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Extract the message value and add it to the list.
                breadcrumb_json_object_list.append(msg.value().decode('utf-8'))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
