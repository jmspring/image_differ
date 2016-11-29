"""
Example python app with the Flask framework: http://flask.pocoo.org/
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

from azure.storage.blob import BlockBlobService

from PIL import Image

app = Flask(__name__)

# stats
total_images_processed = 0
largest_image_difference = -1
last_image_difference = -1

# handlers
shutdown_requested = False
diff_thread = None
scrubber_thread = None

# Retrieve configuration from environment
def environment_variables():
    env = {
        'storageAccount': environ.get('AZURE_STORAGE_ACCOUNT_NAME', None),
        'storageAccountKey': environ.get('AZURE_STORAGE_ACCOUNT_KEY', None),
        'storageAccountContainer': environ.get('AZURE_STORAGE_ACCOUNT_CONTAINER_NAME', None),
        'alertThreshold': environ.get('IMAGE_DIFFERENCE_ALERT_THRESHOLD', 10),
        'checkInterval':  environ.get('IMAGE_CHECK_INTERVAL', 10),
        'scrubInterval': environ.get('IMAGE_SCRUB_INTERVAL', 600)
    }
    return env

def required_environment_vars_set(env):
    if env['storageAccount'] != None and \
            env['storageAccountKey'] != None and \
            env['storageAccountContainer'] != None:
        return True
    return False

def shutdown_server():
    global diff_thread
    global scrubber_thread
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
    if scrubber_thread:
        t = scrubber_thread
        scrubber_thread = None
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

def image_difference_loop():
    global shutdown_requested
    global last_image_difference
    global total_images_processed
    global largest_image_difference

    blob_service = None

    last_check = 0
    env = {}
    while not shutdown_requested:
        if not blob_service:
            env = environment_variables()
            if required_environment_vars_set(env):
                blob_service = BlockBlobService(account_name=env['storageAccount'],
                                                account_key=env['storageAccountKey'])
                continue
        else:
            now = int(floor(time.time()))
            if now - last_check > env['checkInterval']:
                last_check = now
                blob_filter = '{}'.format(now)[0:-3]
                blobs = list(blob_service.list_blobs(env['storageAccountContainer'],
                                                     prefix=blob_filter))
                blob_count = len(blobs)

                if blob_count > 2:
                    current_blob_name = blobs[blob_count-1].name
                    prior_blob_name = blobs[blob_count-2].name
                    current_blob = blob_service.get_blob_to_bytes(env['storageAccountContainer'],
                                                                current_blob_name)
                    prior_blob = blob_service.get_blob_to_bytes(env['storageAccountContainer'],
                                                                prior_blob_name)

                    last_image_difference = diff_images(current_blob.content, prior_blob.content)
                    if last_image_difference > largest_image_difference:
                        largest_image_difference = last_image_difference
                    total_images_processed = total_images_processed + 1

                    current_blob = None
                    prior_blob = None
                    blobs = None
        time.sleep(0.5)

def image_scrubber_loop():
    global shutdown_requested

    blob_service = None

    last_check = 0
    env = {}
    while not shutdown_requested:
        if not blob_service:
            env = environment_variables()
            if required_environment_vars_set(env):
                blob_service = BlockBlobService(account_name=env['storageAccount'],
                                                account_key=env['storageAccountKey'])
                continue
        else:
            now = int(floor(time.time()))
            if now - last_check > env['scrubInterval']:
                last_check = now
                preserve_range = int(floor(now / 1000))
                preserve_prefix = '{}'.format(preserve_range)
                blobs = list(blob_service.list_blobs(env['storageAccountContainer']))

                for blob in blobs:
                    if not blob.name.startswith(preserve_prefix):
                        blob_service.delete_blob(env['storageAccountContainer'], blob.name)
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
        output = 'total images: {}\n' \
                'last image difference: {}\n' \
                'largest different: {}\n'.format(total_images_processed,
                                                last_image_difference,
                                                largest_image_difference)
        return Response(output, mimetype='text/plain')

    @app.route('/')
    def index():
        return render_template('index.html',
                            powered_by=environ.get('POWERED_BY', 'Deis'))

    def interrupt():
        global diff_thread
        global scrubber_thread
        global shutdown_requested

        shutdown_requested = True

        if diff_thread:
            diff_thread.join()
            diff_thread = None
        if scrubber_thread:
            scrubber_thread.join()
            scrubber_thread = None

    def start_differ():
        global scrubber_thread
        global diff_thread
        diff_thread = threading.Thread(target=image_difference_loop)
        diff_thread.start()
        scrubber_thread = threading.Thread(target=image_scrubber_loop)
        scrubber_thread.start()

    start_differ()
    atexit.register(interrupt)

    return app


if __name__ == '__main__':
    app = create_app()

    # Bind to PORT if defined, otherwise default to 5000.
    port = int(environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
    