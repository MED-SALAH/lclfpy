import argparse

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from lclf.schemas.event_schema import EventHeader
from lclf.utils.utils import toNametuple
import random
from cassandra.cluster import Cluster
import time

def main(args):
    topic = args.topic

    schema_str = EventHeader

    sr_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_deserializer = AvroDeserializer(schema_str,
                                         schema_registry_client)
    string_deserializer = StringDeserializer('utf_8')

    consumer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.deserializer': string_deserializer,
                     'value.deserializer': avro_deserializer,
                     'group.id': args.group+str(random.Random()),
                     'auto.offset.reset': "latest"}

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

            evt = toNametuple("EventHeader", msg.value())

            query = f"""

            insert into EventHeader (
                            "eventid" ,
                            "datetimeref" ,
                            "nomenclatureev" ,
                            "canal" , 
                            "media" ,
                            "schemaversion",
                            "headerversion",
                            "serveur",
                            "adresseip",
                            "idtelematique",
                            "idpersonne")

                            VALUES (
                            '{evt.eventId}' ,
                            {evt.dateTimeRef} ,
                            'test' ,
                            {evt.canal} , 
                            {evt.media} ,
                            '{evt.schemaVersion}',
                            '{evt.headerVersion}',
                            '{evt.serveur}',
                            '',
                            '',
                            '')

"""

            #print(f"Query={query}")
            session.execute(query)
            elapsed_time = (time.time() - start)
            print(elapsed_time)

            if evt is not None:
                #print("evt ==>", evt)
                elapsed_time = (time.time() - start)
                print(elapsed_time)
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