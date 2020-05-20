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
from lclf.schemas.event_schema_all import EventSchema, EventHeaderSchema, EnrichedEventSchema, GET_ENRICHED_DATA_QUERY, \
    GET_ENRICHED_EVENT_QUERY, MetricSchema
from cassandra.query import dict_factory

from lclf.utils.utils import stat_rocess


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for Event {}: {}".format(msg.key(), err))
        return
    print('Event  {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(args):
    topic = args.topic
    outputtopic = args.outputtopic

    schema_enriched_event_str = EnrichedEventSchema
    schema_metrics = MetricSchema

    sr_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(sr_conf)
    string_deserializer = StringDeserializer('utf_8')

    avro_serializer = AvroSerializer(schema_metrics,
                                     schema_registry_client)
    producer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': avro_serializer}

    producer = SerializingProducer(producer_conf)

    avro_deserializer = AvroDeserializer(schema_enriched_event_str, schema_registry_client)

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

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            start = time.time()
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            evt = msg.value()

            rows = session.execute(GET_ENRICHED_EVENT_QUERY, (evt["idPersonne"],))
            if rows:
                print(f"rows={rows.all().__len__()}")
                stat_rocess(rows)

                row["csp"] = get_value_column_enriched_data(row, "csp")
                row["paysNaissance"] = get_value_column_enriched_data(row, "paysNaissance")


                #get_value_column_event_content
                row['appVersion'] = get_value_column_event_content(row, "appVersion")
                row['montant'] = get_value_column_event_content(row, "montant")
                row['androidID'] = get_value_column_event_content(row, "androidID")

                del rows[0]['eventContent']

                time_spent = time.time() - start

                #producer.produce(topic=outputtopic, key=str(uuid4()), value={'metricName':"hystorize",'time':elapsed_time}, on_delivery=delivery_report)
                #producer.flush()




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