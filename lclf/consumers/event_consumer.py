import argparse
import random
import time

from cassandra.cluster import Cluster
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringDeserializer

from lclf.custom.avro import AvroDeserializer
from lclf.schemas.event_schema_all import EventSchema


def main(args):
    topic = args.topic

    schema_str = EventSchema

    sr_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_deserializer = AvroDeserializer(schema_str,schema_registry_client)
    string_deserializer = StringDeserializer('utf_8')

    consumer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.deserializer': string_deserializer,
                     'value.deserializer': avro_deserializer,
                     'group.id': args.group+str(random.Random()),
                     'auto.offset.reset': "earliest"}

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])

    cluster = Cluster([args.host])
    session = cluster.connect("datascience")


    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            start = time.time()
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            evt =  msg.value()

            # print(myFunc())

            query = f"""
            insert into event (
                        "eventid" ,
                        "eventbc",
                        "eventcontent"
                        )
                        VALUES (%s, %s, %s)


                    """

            print(f"Query={query}")
            # session.execute(query)
            session.execute(query, (evt["EventHeader"]["eventId"], evt["EventBusinessContext"][0], evt["EventBusinessContext"][1]))

            if evt is not None:
                print(evt["EventBusinessContext"][1])
                # print("evt ==>", evt["EventHeader"]["eventId"])
                # print("evt ==>", evt["EventBusinessContext"][0])
                # print("evt ==>", evt["EventBusinessContext"][1])



        except KeyboardInterrupt:
            break

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

    main(parser.parse_args())