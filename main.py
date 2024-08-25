from fastapi import FastAPI, HTTPException
import json
import requests

import redis
import time
import traceback
import logging
from typing import Callable, Optional
import threading


app = FastAPI()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class RedisStreamConsumer:
    def __init__(
        self,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        redis_host: str = 'redis',
        redis_port: int = 6379,
        postprocess_func: Optional[Callable[[dict], None]] = None
    ):
        """Initialize the RedisStreamConsumer with stream name, group name, and consumer name."""
        self.stream_name = stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.postprocess_func = postprocess_func
        startup_nodes = [{"host": "10.128.0.3", "port": 6379}]
        host = "10.4.57.166" # 10.4.57.166
        self.redis_client = redis.Redis(host=host, port=6379, decode_responses=True) # Redis(startup_nodes=startup_nodes, decode_responses=True)
        redis_host = "10.4.57.166" # "10.128.0.3" # startup_nodes[0]["host"]
        redis_port = 6379 # startup_nodes[0]["port"]
        self.stop_flag = False

        try:
            self.redis_client.ping()
            logger.info(f'Connected to Redis on {redis_host}:{redis_port}')
        except redis.ConnectionError as e:
            logger.error(f'Failed to connect to Redis: {e}')
            raise e

        # Create the consumer group if it doesn't exist
        try:
            logger.info(f"Creating consumer group '{self.group_name}' for stream '{self.stream_name}'")
            self.redis_client.xgroup_create(self.stream_name, self.group_name, id='0', mkstream=True)
        except redis.exceptions.ResponseError as e:
            # Ignore if the group already exists
            if "BUSYGROUP" in str(e):
                logger.info(f"Consumer group '{self.group_name}' already exists for stream '{self.stream_name}'")
            else:
                logger.error(f"Error creating consumer group: {e}")
                raise

    # def consume(self):
    #     logger.info(f"Starting to consume messages from stream '{self.stream_name}'")
    #     try:
    #         while not self.stop_flag:
    #             logger.info(f'Waiting for messages in the loop for {self.stream_name}')
    #             try:
    #                 logger.info('Verify stream length')
    #                 stream_length = self.redis_client.xlen(self.stream_name)
    #                 logger.info(stream_length)
    #                 logger.info('Read all messages to verify they exist')
    #                 all_messages = self.redis_client.xrange(self.stream_name, min='-', max='+', count=10)
    #                 logger.info(all_messages)
    #                 logger.info('Going on')
    #                 stream_name = self.stream_name
    #                 last_id = '0-0'  #'>'
    #                 logger.info("Starting to consume messages from the last_id")
    #                 messages = self.redis_client.xread({stream_name: last_id}, count=10, block=5000)
    #                 logger.info(f"Messages: {messages}")
    #                 if messages:
    #                     for message in messages:
    #                         stream_name, message_data = message
    #                         for message_id, data in message_data:
    #                             try:
    #                                 current_time = int(time.time() * 1000)
    #                                 timestamp = int(data.get('timestamp', 0))
    #                                 expiration_threshold = int(data.get('expiration_threshold', 0))
    #
    #                                 if current_time - timestamp > expiration_threshold:
    #                                     logger.info(f"Message {message_id} expired, skipping.")
    #                                     continue
    #                                 logger.info(f"Consumed message ID {message_id}: {data}")
    #                                 if self.postprocess_func:
    #                                     logger.info(f"Applying postprocess function to message ID {message_id}")
    #                                     self.postprocess_func(data)
    #                                 self.redis_client.xack(self.stream_name, self.group_name, message_id)
    #                                 logger.info(f"Acknowledged message ID {message_id}")
    #                             except Exception as e:
    #                                 logger.error(f"Error processing message {message_id}: {e}")
    #                                 traceback.print_exc()
    #                 else:
    #                     logger.info("No new messges received")
    #             except Exception as e:
    #                 logger.error(f'Unexpected error in consumer: {e}', exc_info=True)
    #
    #     except Exception as e:
    #         logger.error(f"Error consuming messages: {e}")
    #         traceback.print_exc()
    #     finally:
    #         self.cleanup()

    def consume(self):
        """Start consuming messages from the Redis stream."""
        logger.info(f"Starting to consume messages from stream '{self.stream_name}' as '{self.consumer_name}'")
        try:
            while not self.stop_flag:
                logger.info(f'Waiting for messages in the loop for {self.stream_name}')
                try:
                    logger.info(f'Trying to inspect the group for the {self.stream_name}')
                    groups_info = self.redis_client.xinfo_groups(self.stream_name)
                    logger.info(groups_info)
                    logger.info('Trying to inspect pending messages')
                    pending_messages = self.redis_client.xpending(self.stream_name, self.group_name)
                    logger.info(pending_messages)
                    messages = self.redis_client.xreadgroup(self.group_name, self.consumer_name, {self.stream_name: '>'}, count=10, block=5000)
                    logger.info(f"Messages: {messages}")
                    if messages:
                        for message in messages:
                            stream_name, message_data = message
                            for message_id, data in message_data:
                                try:
                                    current_time = int(time.time() * 1000)
                                    timestamp = int(data.get('timestamp', 0))
                                    expiration_threshold = int(data.get('expiration_threshold', 0))

                                    if current_time - timestamp > expiration_threshold:
                                        logger.info(f"Message {message_id} expired, skipping.")
                                        continue
                                    logger.info(f"Consumed message ID {message_id}: {data}")
                                    if self.postprocess_func:
                                        logger.info(f"Applying postprocess function to message ID {message_id}")
                                        self.postprocess_func(data)
                                    self.redis_client.xack(self.stream_name, self.group_name, message_id)
                                    logger.info(f"Acknowledged message ID {message_id}")
                                except Exception as e:
                                    logger.error(f"Error processing message {message_id}: {e}")
                                    traceback.print_exc()
                    else:
                        logger.info("No new messges received")
                except Exception as e:
                    logger.error(f'Unexpected error in consumer: {e}', exc_info=True)

        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
            traceback.print_exc()
        finally:
            self.cleanup()

    def cleanup(self):
        logger.info(f"Cleaning up resources for stream: {self.stream_name}")
        self.redis_client.close()

    def stop(self):
        logger.info(f"Stop signal received for stream: {self.stream_name}")
        self.stop_flag = True


# Example post-processing function
def example_postprocess(data: dict):
    logger.info(f"Post-processed message: {data}")


# Function to start a consumer process
def start_consumer_process(consumer):
    consumer.consume()


consumer_threads = {}
consumer_instances = {}


def send_to_webhook(message):
    logger.info('SENDING TO WEBHOOK')
    URL = "https://fastapi-demo-jjfx7unn3a-uc.a.run.app/rate"
    r = requests.post(URL, data=json.dumps(message))
    logger.info(r.status_code)
    logger.info(r.json())
    logger.info(f"Sent to webhook: {message}")


# Define a simple postprocess function for consumers
def process_message(message):
    logger.info(f"Processing message: {message}")


# Function to start a consumer in a thread
def start_consumer_thread(consumer):
    consumer.consume()


@app.post("/start-consumer/{stream_name}")
async def start_consumer(stream_name: str):
    if stream_name in consumer_threads:
        raise HTTPException(status_code=400, detail=f"Consumer for stream {stream_name} is already running.")

    logger.info(f"Starting consumer for stream {stream_name}")
    group_name = f"group-{stream_name}"
    consumer_name = f"consumer-{stream_name}"

    # Create the consumer instance
    consumer = RedisStreamConsumer(
        stream_name=stream_name,
        group_name=group_name,
        consumer_name=consumer_name,
        postprocess_func=send_to_webhook # process_message
    )

    # Save the consumer instance
    consumer_instances[stream_name] = consumer

    # Create and start a new thread for the consumer
    thread = threading.Thread(target=start_consumer_thread, args=(consumer,), daemon=True)
    thread.start()

    # Save the thread in the dictionary
    consumer_threads[stream_name] = thread

    return {"message": f"Consumer for stream {stream_name} started successfully."}


@app.post("/stop-consumer/{stream_name}")
async def stop_consumer(stream_name: str):
    thread = consumer_threads.get(stream_name)
    consumer = consumer_instances.get(stream_name)

    if not thread or not consumer:
        raise HTTPException(status_code=404, detail=f"No running consumer found for stream {stream_name}.")

    logger.info(f"Stopping consumer for stream {stream_name}")

    consumer.stop()
    thread.join()

    del consumer_threads[stream_name]
    del consumer_instances[stream_name]

    return {"message": f"Consumer for stream {stream_name} stopped successfully."}

@app.get("/")
def get_root():
    return {'redis_consumer_health_check': 'OK'}
