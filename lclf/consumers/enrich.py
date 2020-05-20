import argparse
import asyncio
import random
import sys
import time
from functools import partial, wraps
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

from lclf.utils.utils import copy_keys, flat_content


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for Event {}: {}".format(msg.key(), err))
        return
    print('Event  {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def async_wrap(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)
    return run


def enrich(evt, session, producer, outputtopic):
    idPers = evt["EventHeader"]["acteurDeclencheur"]["idPersonne"]

    row = session.execute(GET_ENRICHED_DATA_QUERY,(idPers,)).one()
    if row:
        evt['EnrichedData'] = row

        eventHeader = evt['EventHeader']
        acteurDeclencheur = eventHeader["acteurDeclencheur"]

        enrichedEvent = eventHeader

        enrichedEvent['adresseIP'] = acteurDeclencheur["adresseIP"]
        enrichedEvent['idPersonne'] = acteurDeclencheur["idPersonne"]
        enrichedEvent['idTelematique'] = acteurDeclencheur["idTelematique"]


        copy_keys(row, enrichedEvent)

        eventName = evt['EventBusinessContext'][0].replace("com.bnpparibas.dsibddf.event.","")
        enrichedEvent['eventBC'] = eventName
        eventContext = evt['EventBusinessContext'][1]
        paylibVADEventBusinessContextSchema = """
        {
          "doc": "Schéma pour l'événement 0001D00000000014J PAYLIB-ACTIVATION-VAD",
          "fields": [
            {
              "name": "idContrat",
              "type": "string"
            },
            {
              "name": "idPrestation",
              "type": "string"
            },
            {
              "doc": "Au format Timestamp UNIX",
              "logicalType": "timestamp-millis",
              "name": "dateActivation",
              "type": "long"
            },
            {
              "name": "listeCartes",
              "type": {
                "items": {
                  "fields": [
                    {
                      "name": "numeroCarte",
                      "type": "string"
                    },
                    {
                      "name": "cartePreferentielle",
                      "type": "boolean"
                    },
                    {
                      "doc": "Au format MM/AAAA",
                      "logicalType": "date",
                      "name": "dateFinValidite",
                      "type": "string"
                    }
                  ],
                  "name": "carte",
                  "type": "record"
                },
                "type": "array"
              }
            }
          ],
          "name": "PaylibVADEventBusinessContext",
          "type": "record"
        }
        """

        flatten_event = eventContext

        if(eventName.__contains__("PaylibVADEventBusinessContext")):
            flatten_event = flat_content(eventContext, paylibVADEventBusinessContextSchema)

        copy_keys(flatten_event, enrichedEvent)

        enrichedEvent['EventBusinessContext'] = eventContext

        producer.produce(topic=outputtopic, key=str(uuid4()), value=enrichedEvent, on_delivery=delivery_report)
        producer.flush()

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
                     'auto.offset.reset': "latest"}

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
            enrich(evt, session, producer, outputtopic)

        except Exception:
            print('Exception', sys.exc_info()[0])
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
