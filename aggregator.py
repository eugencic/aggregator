from flask import Flask, request
from threading import Thread
import requests
from time import sleep
import queue

app = Flask(__name__)

threads = []

producer_queue = queue.Queue()
producer_queue.join()

consumer_queue = queue.Queue()
consumer_queue.join()

@app.route('/producer_aggregator', methods = ['GET', 'POST'])

def producer_aggregator():
    data = request.get_json()
    print(f'Order nr.{data["order_id"]} is received from client nr.{data["client_id"]}\n')
    sleep(3)
    split_producer_order(data)
    return {'isSuccess': True}

@app.route('/consumer_aggregator', methods = ['GET', 'POST'])

def consumer_aggregator():
    data = request.get_json()
    print(f'Order nr.{data["order_id"]} is received from the storehouse\n')
    sleep(3)
    split_consumer_order(data)
    return {'isSuccess': True}

def split_producer_order(input_order):
    order = {
        'order_id': input_order['order_id'],
        'client_id': input_order['client_id'],
        'product_id': input_order['product_id'],
    }
    producer_queue.put(order)

def send_to_consumer(name):
    try:
        order = producer_queue.get()
        producer_queue.task_done()
        payload = dict({'order_id': order['order_id'], 'client_id': order['client_id'], 'product_id': order['product_id']})
        print(f'Order nr.{order["order_id"]} is sent to the storehouse\n')
        requests.post('http://localhost:4080/consumer', json = payload, timeout = 0.0000000001)
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
        pass
    
def split_consumer_order(input_order):
    order = {
        'order_id': input_order['order_id'],
        'client_id': input_order['client_id'],
        'product_id': input_order['product_id'],
    }
    consumer_queue.put(order)

def send_to_producer(name):
    try:
        order = consumer_queue.get()
        consumer_queue.task_done()
        payload = dict({'order_id': order['order_id'], 'client_id': order['client_id'], 'product_id': order['product_id']})
        print(f'Order nr.{order["order_id"]} is sent to the client\n')
        requests.post('http://localhost:4000/producer', json = payload, timeout = 0.0000000001)
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
        pass
    
def run_aggregator():
    aggregator_thread = Thread(target = lambda: app.run(host = '0.0.0.0', port = 4040, debug = False, use_reloader = False), daemon = True)
    threads.append(aggregator_thread)
    sleep(2)
    producer_thread_name = 1
    consumer_thread_name = 11
    for _ in range(7):
        to_consumer_thread = Thread(target = send_to_consumer, args = (producer_thread_name,), name = str(producer_thread_name))
        threads.append(to_consumer_thread)
        producer_thread_name += 1
        to_producer_thread = Thread(target = send_to_producer, args = (consumer_thread_name,), name = str(consumer_thread_name))
        threads.append(to_producer_thread)
        consumer_thread_name += 1
    for thread in threads:
        thread.start()
        sleep(2)
    for thread in threads:
        thread.join()

if __name__ == '__main__':
    run_aggregator()