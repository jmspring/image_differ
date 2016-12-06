"""
Given a message in a queue, calculates the diff of two images.  If
the difference is large enough, issue a notification.
"""

import json
import time
from math import floor
import io
from itertools import izip
from os import environ
import threading
import atexit

from flask import Flask
from flask import render_template
from flask import Response
from flask import request

from azure.common import AzureMissingResourceHttpError
from azure.storage.blob import BlockBlobService
from azure.servicebus import (
    ServiceBusService,
    Message,
)

from PIL import Image

app = Flask(__name__)

# stats
stats = {
    'processing': {
        'total': 0,
        'largest_difference': 0,
        'last_difference': 0,
        'last_timestamp': 0,
    },
    'queues': {
    },
    'eventhubs': {
    },
    'image_store': {
        'read': {
            'total': 0,
            'last_image': None,
            'errors': 0,
            'last_error': None
        },
        'delete': {
            'total': 0,
            'last_image': None,
            'errors': 0,
            'last_error': None
        }

    }
}

# handlers
shutdown_requested = False
diff_thread = None
retrieve_thread = None

def stats_update_image_stats(difference, capture_time):
    global stats
    stats['processing']['total'] = stats['processing']['total'] + 1 
    stats['processing']['last_difference'] = difference
    stats['processing']['last_timestamp'] = capture_time
    if difference > stats['processing']['largest_difference']:
        stats['processing']['largest_difference'] = difference

def stats_update_image_store(operation, name, error):
    global stats
    stats['image_store'][operation]['total'] = stats['image_store'][operation]['total'] + 1
    stats['image_store'][operation]['last_image'] = name
    if error is not None:
        stats['image_store'][operation]['errors'] = stats['image_store'][operation]['errors'] + 1
        stats['image_store'][operation]['last_error'] = error

def stats_update_queues_and_hubs(sbtype, name, success, error):
    global stats
    if name not in stats[sbtype]:
        stats[sbtype][name] = {
            'messages': 0,
            'errors': 0,
            'last_error': None
        }
    if success:
        stats[sbtype][name]['messages'] = stats[sbtype][name]['messages'] + 1
    else:
        stats[sbtype][name]['errors'] = stats[sbtype][name]['errors'] + 1
        stats[sbtype][name]['last_error'] = error 

def stats_update_queues(name, success, error = None):
    stats_update_queues_and_hubs('queues', name, success, error)

def stats_update_eventhubs(name, success, error = None):
    stats_update_queues_and_hubs('eventhubs', name, success, error)

# Retrieve configuration from environment
def environment_variables():
    env = {
        'storageAccount': environ.get('AZURE_STORAGE_ACCOUNT_NAME', None),
        'storageAccountKey': environ.get('AZURE_STORAGE_ACCOUNT_KEY', None),
        'storageAccountContainer': environ.get('AZURE_STORAGE_ACCOUNT_CONTAINER_NAME', None),
        'alertThreshold': environ.get('IMAGE_DIFFERENCE_ALERT_THRESHOLD', 5),
        'checkInterval': environ.get('IMAGE_CHECK_INTERVAL', 10),
        'serviceBusNamespace': environ.get('AZURE_SERVICE_BUS_NAMESPACE', None),
        'serviceBusImageQueue': environ.get('AZURE_SERVICE_BUS_IMAGE_QUEUE', None),
        'serviceBusNotificationQueue': environ.get('AZURE_SERVICE_BUS_NOTIFICATION_QUEUE', None),
        'serviceBusSharedAccessName': environ.get('AZURE_SERVICE_BUS_SHARED_ACCESS_NAME', None),
        'serviceBusSharedAccessKey': environ.get('AZURE_SERVICE_BUS_SHARED_ACCESS_KEY', None),
        'eventHubNamespace': environ.get('AZURE_EVENT_HUB_NAMESPACE', None),
        'eventHubName': environ.get('AZURE_EVENT_HUB_NAME', None),
        'eventHubSharedAccessName': environ.get('AZURE_EVENT_HUB_SHARED_ACCESS_NAME', None),
        'eventHubSharedAccessKey': environ.get('AZURE_EVENT_HUB_SHARED_ACCESS_KEY', None),
    }
    return env

def required_environment_vars_set(env):
    if env['storageAccount'] != None and \
            env['storageAccountKey'] != None and \
            env['storageAccountContainer'] != None and \
            env['serviceBusNamespace'] != None and \
            env['serviceBusImageQueue'] != None and \
            env['serviceBusNotificationQueue'] != None and \
            env['serviceBusSharedAccessName'] != None and \
            env['serviceBusSharedAccessKey'] != None and \
            env['eventHubNamespace'] != None and \
            env['eventHubName'] != None and \
            env['eventHubSharedAccessName'] != None and \
            env['eventHubSharedAccessKey'] != None:
        return True
    return False

def shutdown_server():
    global diff_thread
    global shutdown_requested

    shutdown_requested = True

    # shutdown flask
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

    # wait for difference thread to shut down
    if diff_thread:
        t = diff_thread
        diff_thread = None
        t.join()

def diff_images(image1_bytes, image2_bytes):
    image1 = Image.open(io.BytesIO(image1_bytes))
    image2 = Image.open(io.BytesIO(image2_bytes))

    if image1.mode != image2.mode:
        # different types of images
        return -1
    if image1.size != image2.size:
        # different sizes
        return -1

    pairs = izip(image1.getdata(), image2.getdata())
    if len(image1.getbands()) == 1:
        # for gray-scale jpegs
        dif = sum(abs(p1-p2) for p1, p2 in pairs)
    else:
        dif = sum(abs(c1-c2) for p1, p2 in pairs for c1, c2 in zip(p1, p2))

    ncomponents = image1.size[0] * image1.size[1] * 3
    difference = (dif / 255.0 * 100) / ncomponents

    return difference

def sbus_send_message(namespace, access_name, access_key, entity, body, is_event_hub=False):
    sbus_service = ServiceBusService(namespace,
                                     shared_access_key_name=access_name,
                                     shared_access_key_value=access_key)
    message = Message(body)
    error = None
    try:
        if not is_event_hub:
            sbus_service.send_queue_message(entity, message)
        else:
            sbus_service.send_event(entity, message)
    except Exception as e:
        error = str(e)
    if is_event_hub:
        stats_update_eventhubs(entity, (error == None), error)
    else:
        stats_update_queues(entity, (error == None), error)

def sbus_recv_message(namespace, access_name, access_key, entity, timeout=10):
    sbus_service = ServiceBusService(namespace,
                                     shared_access_key_name=access_name,
                                     shared_access_key_value=access_key)
    error = None
    message = None
    try:
        message = sbus_service.receive_queue_message(entity, False, timeout)
        if message.body == None:
            return None
        message = message.body
    except Exception as e:
        error = str(e)
    stats_update_queues(entity, (error == None), error)
    return message

def blob_retrieve_blob_bytes(account, key, container, name):
    blob_service = BlockBlobService(account_name=account, account_key=key)
    error = None
    try:
        blob = blob_service.get_blob_to_bytes(container, name)
    except AzureMissingResourceHttpError as e:
        blob = None
        error = str(e)
    except Exception as ex:        
        blob = None
        error = str(e)
    stats_update_image_store('read', name, error)
    return blob

def blob_delete_blob(account, key, container, name):
    blob_service = BlockBlobService(account_name=account, account_key=key)
    error = None
    try:
        blob_service.delete_blob(container, name)
    except AzureMissingResourceHttpError as e:
        error = str(e)
    except Exception as ex:
        error = str(e)
    stats_update_image_store('delete', name, error)

class ProcessQueueMessages:
    messages = None;
    messageLock = None;

    def __init__(self):
        self.messages = {}
        self.messageLock = threading.Lock()

    def insert(self, key, message):
        print 'key: {}, message: {}'.format(key, json.dumps(message))
        with self.messageLock:
            self.messages[key] = message

    def retrieve(self):
        message = None
        with self.messageLock:
            if len(self.messages.keys()) > 0:
                key = sorted(self.messages.keys())[0]
                message = self.messages.pop(key)
                print 'key: {}, message: {}'.format(key, json.dumps(message))
        return message

def process_message_loop(messageQueue):
    global shutdown_requested

    env = {}
    environment_ready = False
    while not shutdown_requested:
        processed_packet = False
        if not environment_ready:
            env = environment_variables()
            if required_environment_vars_set(env):
                environment_ready = True
                continue
            else:
                time.sleep(0.5)
        else:
            message = sbus_recv_message(env['serviceBusNamespace'],
                                            env['serviceBusSharedAccessName'],
                                            env['serviceBusSharedAccessKey'],
                                            env['serviceBusImageQueue'],
                                            5)
            packet = None
            if message:
                try:
                    packet = json.loads(message)
                except Exception as e:
                    packet = None
                if packet:
                    messageQueue.insert(packet['timestamp'], packet)
            if not packet:
                time.sleep(0.25)

def image_difference_loop(messageQueue):
    global shutdown_requested

    blob_service = None
    sbus_service = None

    last_check = 0
    env = {}
    environment_ready = False
    while not shutdown_requested:
        processed_packet = False

        if not environment_ready:
            env = environment_variables()
            if required_environment_vars_set(env):
                environment_ready = True
                continue
        else:
            message = messageQueue.retrieve()
            if message != None:
                packet = message
                current_blob_name = packet['current_image']
                prior_blob_name = packet['prior_image']
                capture_time = packet['timestamp']
                current_blob = None
                prior_blob = None

                print 'Current: {}, Prior: {}, Timestamp: {}'.format(current_blob_name, prior_blob_name, capture_time)
            
                # Retrieve the blobs, skip the packet if either one missing
                start = time.time()
                current_blob = blob_retrieve_blob_bytes(env['storageAccount'],
                                                        env['storageAccountKey'],
                                                        env['storageAccountContainer'],
                                                        current_blob_name)
                end = time.time();
                print 'Load current: {}'.format(end - start)
                start = end
                prior_blob = blob_retrieve_blob_bytes(env['storageAccount'],
                                                      env['storageAccountKey'],
                                                      env['storageAccountContainer'],
                                                      prior_blob_name)
                end = time.time();
                print 'Load prior: {}'.format(end - start)

                if current_blob and prior_blob:
                    # determine the difference
                    start = time.time()
                    difference = diff_images(current_blob.content, prior_blob.content)
                    end = time.time()
                    print 'Difference: {}'.format(end-start)
                    stats_update_image_stats(difference, capture_time)

                    packet = {
                        'timestamp': capture_time,
                        'difference': difference
                    }
                    if difference > env['alertThreshold']:
                        # send a message to the notifier to alert about the difference detected
                        sbus_send_message(env['serviceBusNamespace'],
                                        env['serviceBusSharedAccessName'],
                                        env['serviceBusSharedAccessKey'],
                                        env['serviceBusNotificationQueue'],
                                        json.dumps(packet))

                    # send difference to eventhub for analysis and processing
                    start = time.time()
                    sbus_send_message(env['eventHubNamespace'],
                                    env['eventHubSharedAccessName'],
                                    env['eventHubSharedAccessKey'],
                                    env['eventHubName'],
                                    json.dumps(packet),
                                    True)
                    end = time.time()
                    print 'Send event: {}'.format(end-start)

                    processed_packet = True

                # clean up
                if prior_blob:
                    # do some clean up
                    start = time.time()
                    blob_delete_blob(env['storageAccount'],
                                     env['storageAccountKey'],
                                     env['storageAccountContainer'],
                                     prior_blob_name)
                    end = time.time()
                    print'Delete prior: {}'.format(end-start)
                current_blob = None
                prior_blob = None
        if not processed_packet:
            time.sleep(1)

def create_app():
    @app.route('/config')
    def config():
        output = json.dumps(environment_variables(), indent=4) + '\n'
        return Response(output, mimetype='text/plain')

    @app.route('/shutdown')
    def shutdown():
        shutdown_server()
        return Response('ok\n', mimetype='text/plain')

    @app.route('/stats')
    def stats():
        global stats
        output = '{}\n'.format(json.dumps(stats, sort_keys=True, indent=4, separators=(',', ': ')))
        return Response(output, mimetype='text/plain')

    @app.route('/')
    def index():
        return render_template('index.html',
                            powered_by=environ.get('POWERED_BY', 'Deis'))

    def interrupt():
        global diff_thread
        global retrieve_thread
        global shutdown_requested

        shutdown_requested = True

        if diff_thread:
            diff_thread.join()
            diff_thread = None

    def start_differ():
        global diff_thread
        global retrieve_thread
        messageQueue = ProcessQueueMessages()
        diff_thread = threading.Thread(target=image_difference_loop, args=(messageQueue,))
        diff_thread.start()
        retrieve_thread = threading.Thread(target=process_message_loop, args=(messageQueue,))
        retrieve_thread.start()

    start_differ()
    atexit.register(interrupt)

    return app


if __name__ == '__main__':
    app = create_app()

    # Bind to PORT if defined, otherwise default to 5000.
    port = int(environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
