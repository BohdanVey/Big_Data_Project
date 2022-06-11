import json
import argparse
import os
import time

from sseclient import SSEClient as EventSource
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import pandas as pd
import datetime
import threading
import real_date_update
from populate_db.db_interact import CassandraClient,handle_entry

def set_time(dt_hour, dt_min):
    if dt_hour < 10:
        dt_hour = "0" + str(dt_hour)
    if dt_min < 10:
        dt_min = "0" + str(dt_min)
    return str(dt_hour) + ":" + str(dt_min)


def create_kafka_producer(bootstrap_server):
    try:
        producer = KafkaProducer(bootstrap_servers=bootstrap_server,
                                 value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    except NoBrokersAvailable:
        print('No broker found at {}'.format(bootstrap_server))
        raise

    if producer.bootstrap_connected():
        print('Kafka producer connected!')
        return producer
    else:
        print('Failed to establish connection!')
        exit(1)


def save_as_pandas(df, data):
    df = pd.concat([df, pd.DataFrame({
        'domain': data['meta']['domain'],
        'username': data['performer']['user_text'],
        'userid': data['performer']['user_id'],
        'isBot': data['performer']['user_is_bot'],
        'page_title': data['page_title']
    }, index=[0])], ignore_index=True)

    return df


def parse_command_line_arguments():
    parser = argparse.ArgumentParser(description='EventStreams Kafka producer')

    parser.add_argument('--bootstrap_server', default='kafka-server:9092', help='Kafka bootstrap broker(s) (host[:port])',
                        type=str)
    parser.add_argument('--topic_name', default='wikipedia-events', help='Destination topic name', type=str)
    parser.add_argument('--events_to_produce', help='Kill producer after n events have been produced', type=int,
                        default=1000)

    return parser.parse_args()


def send_threads(df, time_start, time_end):
    t1 = threading.Thread(target=real_date_update.first_task_precompute, args=[df.copy(), time_start, time_end])
    t1.start()
    t2 = threading.Thread(target=real_date_update.second_task_precompute, args=[df.copy(), time_start, time_end])
    t2.start()
    t3 = threading.Thread(target=real_date_update.third_task_precompute, args=[df.copy(), time_start, time_end])
    t3.start()
    # real_date_update.first_task_precompute(df.copy(), time_start, time_end)
    # real_date_update.second_task_precompute(df.copy(), time_start, time_end)
    # real_date_update.third_task_precompute(df.copy(), time_start, time_end)


if __name__ == "__main__":
    # parse command line arguments
    args = parse_command_line_arguments()
    now_hour = datetime.datetime.now().hour
    time_start = set_time(datetime.datetime.now().hour, datetime.datetime.now().minute)
    # init producer
    producer = create_kafka_producer(args.bootstrap_server)
    df = pd.DataFrame(columns=['domain', 'username', 'userid', 'isBot', 'page_title'])
    # init dictionary of namespaces

    # used to parse user type
    user_types = {True: 'bot', False: 'human'}

    # consume websocket
    url = 'https://stream.wikimedia.org/v2/stream/page-create'

    host = 'cassandra-node1'
    port = 9042
    keyspace = 'wiki_keyspace'
    client = CassandraClient(host, port, keyspace)
    client.connect()

    print('Messages are being published to Kafka topic')
    messages_count = 0
    total_msg = {'data': []}
    for event in EventSource(url):
        if event.event == 'message':
            try:

                event_data = json.loads(event.data)
                total_msg['data'].append(event_data)
                handle_entry(client,event_data)
                print("READY")
                if messages_count % 100 == 0:
                    print(messages_count)
                messages_count += 1
                if datetime.datetime.now().hour != now_hour:
                    now_hour = datetime.datetime.now().hour
                    time_end = set_time(datetime.datetime.now().hour, datetime.datetime.now().minute)
                    print("START")
                    send_threads(df.copy(), time_start, time_end)
                    print("FINISH")

                    time_start = time_end
                    df = pd.DataFrame(columns=['domain', 'username', 'userid', 'isBot', 'page_title'])

                    print("SAVED", time_start)
                df = save_as_pandas(df, event_data)
                print("HERE")
            except ValueError:
                pass
            except KeyError:
                pass
