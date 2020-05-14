from aiokafka import AIOKafkaConsumer
import asyncio

import argparse
import random
import time
from uuid import uuid4

from cassandra.cluster import Cluster
from confluent_kafka import DeserializingConsumer
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.serialization import StringSerializer

from lclf.custom.avro import AvroDeserializer
from lclf.schemas.event_schema_all import EventSchema, EventHeaderSchema, EnrichedEventSchema



loop = asyncio.get_event_loop()

async def consume():

    #-c 35.181.155.182 -b 35.180.127.210:9092 -s http://35.180.127.210:8081 -t event_ma_banque  -o enriched_event_ma_banque -g  enrich
    topic = "event_ma_banque"
    bootstrap_servers = "35.180.127.210:9092"
    group = "zahir"


    schema_str = EventSchema
    schema_enriched_event_str = EnrichedEventSchema

    sr_conf = {'url': "http://35.180.127.210:8081"}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_deserializer = AvroDeserializer(schema_str, schema_registry_client)
    string_deserializer = StringDeserializer('utf_8')


    avro_serializer = AvroSerializer(schema_enriched_event_str,
                                     schema_registry_client)

    consumer_conf = {'bootstrap.servers': bootstrap_servers,
                     'key.deserializer': string_deserializer,
                     'value.deserializer': avro_deserializer,
                     'group.id': group + str(random.Random()),
                     'auto.offset.reset': "earliest"}


    consumer = AIOKafkaConsumer(
        'my_topic', 'my_other_topic',
        loop=loop, bootstrap_servers='localhost:9092',
        group_id="my-group")
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            print("consumed: ", msg.topic, msg.partition, msg.offset,
                  msg.key, msg.value, msg.timestamp)
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

loop.run_until_complete(consume())