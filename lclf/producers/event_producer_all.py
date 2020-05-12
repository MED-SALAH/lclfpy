import argparse
from uuid import uuid4

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer

from lclf.schemas.event_schema_all import EventSchema
import random
import time




def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(args):
    topic = args.topic
    schema_str = EventSchema

    schema_registry_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_str,
                                     schema_registry_client)

    producer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': avro_serializer}

    producer = SerializingProducer(producer_conf)

    value = {
            "EventHeader": {"eventId": "ZAHIR1"},
            "EventBusinessContext":  {"numeroCompteBeneficiaire": "Numero 123T"}
         }

    producer.produce(topic=topic, key=str(uuid4()), value=value, on_delivery=delivery_report)

    producer.flush()



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="SerializingProducer Example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")

    main(parser.parse_args())