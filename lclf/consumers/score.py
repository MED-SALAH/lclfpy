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
from lclf.schemas.event_schema_all import EventSchema, EventHeaderSchema, EnrichedEventSchema, GET_ENRICHED_DATA_QUERY
from cassandra.query import dict_factory


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for Event {}: {}".format(msg.key(), err))
        return
    print('Event  {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(args):
    topic = args.topic
    outputtopic = args.outputtopic

    schema_str = EventSchema
    schema_enriched_event_str = EnrichedEventSchema

    sr_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_deserializer = AvroDeserializer(schema_str, schema_registry_client)
    string_deserializer = StringDeserializer('utf_8')

    avro_serializer = AvroSerializer(schema_enriched_event_str,
                                     schema_registry_client)

    consumer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.deserializer': string_deserializer,
                     'value.deserializer': avro_deserializer,
                     'group.id': args.group + str(random.Random()),
                     'auto.offset.reset': "earliest"}

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])

    cluster = Cluster([args.host])
    session = cluster.connect("datascience")
    session.row_factory = dict_factory

    producer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': avro_serializer}

    producer = SerializingProducer(producer_conf)

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            start = time.time()
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            evt = msg.value()

            if evt is not None:
                row = session.execute(GET_ENRICHED_DATA_QUERY, (evt["EventHeader"]["acteurDeclencheur"]["idPersonne"],)).one()

                print("row =>", row)
                if row:
                    evt['EnrichedData'] = row
                    #evt['EventBusinessContext'] = evt["EventBusinessContext"][1]

                    producer.produce(topic=outputtopic, key=str(uuid4()), value=evt, on_delivery=delivery_report)
                    producer.flush()

                    print("Time spent ", time.time() - start)


        except Exception:
            print('Exception')
            continue

    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Consumer Example client with "
                                                 "serialization capabilities")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="example_serde_avro",
                        help="Consumer group")
    parser.add_argument('-c', dest="host", required=True,
                        help="Cassandra host")

    parser.add_argument('-o', dest="outputtopic", default="example_serde_avro",
                        help="Topic name")

    main(parser.parse_args())