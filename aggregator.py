from flask import Flask, request
from threading import Thread
import requests
import random
from time import sleep
import queue

# Create a Flask object called app
app = Flask(__name__)

# Array to store the threads
threads = []

# Queue of orders received from the clients
producer_queue = queue.Queue()
producer_queue.join()

# App route
@app.route('/producer_aggregator', methods = ['GET', 'POST'])

def producer_aggregator():
    data = request.get_json()
    print(f'Order nr.{data["order_id"]} is received from client nr.{data["client_id"]}')
    sleep(1)
    split_order(data)
    return {'isSuccess': True}

def split_order(input_order):
    order = {
        'order_id': input_order['order_id'],
        'client_id': input_order['client_id'],
        'product_id': input_order['product_id'],
    }
    producer_queue.put(order)

def send_to_consumer(name):
    try:
    # Get order from the order queue
        order = producer_queue.get()
        producer_queue.task_done()
        payload = dict({'order_id': order['order_id'], 'client_id': order['client_id'], 'product_id': order['product_id']})
        print(f'Order nr.{order["order_id"]} is sent to the storehouse.')
        requests.post('http://localhost:4080/aggregator', json = payload, timeout = 0.0000000001)
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
        pass
    
def run_producer():
    producer_thread = Thread(target=lambda: app.run(host = '0.0.0.0', port = 4040, debug = False, use_reloader = False), daemon = True)
    producer_thread.start()
    sleep(2)
    producer_thread_name = 1
    # Create thread Client
    for _ in range(7):
        thread = Thread(target = send_to_consumer, args = (producer_thread_name,), name = str(producer_thread_name))
        threads.append(thread)
        producer_thread_name += 1
    for thread in threads:
        thread.start()
        sleep(3)
    for thread in threads:
        thread.join()
    while True:
        pass
    
if __name__ == '__main__':
    run_producer()