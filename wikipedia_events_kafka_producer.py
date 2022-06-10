import json
import argparse
import time

from sseclient import SSEClient as EventSource
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import pandas as pd
import datetime
import threading
import real_date_update
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

def save_as_pandas(df,data):
    df = pd.concat([df,pd.DataFrame({
        'domain': data['meta']['domain'],
        'username':data['performer']['user_text'],
        'userid':data['performer']['user_id'],
        'isBot':data['performer']['user_is_bot'],
        'page_title':data['page_title']
    }, index=[0])],ignore_index = True)

    return df
def parse_command_line_arguments():
    parser = argparse.ArgumentParser(description='EventStreams Kafka producer')

    parser.add_argument('--bootstrap_server', default='localhost:9092', help='Kafka bootstrap broker(s) (host[:port])', type=str)
    parser.add_argument('--topic_name', default='wikipedia-events', help='Destination topic name', type=str)
    parser.add_argument('--events_to_produce', help='Kill producer after n events have been produced', type=int, default=1000)

    return parser.parse_args()


if __name__ == "__main__":
    # parse command line arguments
    args = parse_command_line_arguments()
    now_minute = datetime.datetime.now().minute
    time_start = str(datetime.datetime.now().hour) + ':' + str(datetime.datetime.now().minute)
    # init producer
    producer = create_kafka_producer(args.bootstrap_server)
    df = pd.DataFrame(columns=['domain','username','userid','isBot','page_title'])
    # init dictionary of namespaces

    # used to parse user type
    user_types = {True: 'bot', False: 'human'}

    # consume websocket
    url = 'https://stream.wikimedia.org/v2/stream/page-create'
    
    print('Messages are being published to Kafka topic')
    messages_count = 0
    total_msg = {'data':[]}
    for event in EventSource(url):
        print("HERE")
        if event.event == 'message':
            try:
                event_data = json.loads(event.data)
                total_msg['data'].append(event_data)
                if datetime.datetime.now().minute != now_minute:
                    now_minute = datetime.datetime.now().minute
                    time_end = str(datetime.datetime.now().hour) + ':' + str(datetime.datetime.now().minute)
                    df = pd.DataFrame(columns=['domain', 'username', 'userid', 'isBot', 'page_title'])
                    t1 = threading.Thread(target=real_date_update.first_task_precompute,args=[df.copy(),time_start,time_end])
                    time_start = time_end
                    t1.start()

                df = save_as_pandas(df,event_data)
            except:
                pass
