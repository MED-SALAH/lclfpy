import argparse
import random
import time

from cassandra.cluster import Cluster
from confluent_kafka import DeserializingConsumer
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from lclf.custom.avro import AvroDeserializer
from lclf.schemas.event_schema_all import EnrichedEventSchema, MetricSchema
import fastavro
import ast

from lclf.utils.utils import insert_enriched_event_to_cassandra, Datafield, transform_enriched_event_to_cassandra_model
from influxdb import InfluxDBClient




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
    schema_dict = ast.literal_eval(schema_enriched_event_str)
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


    avro_deserializer = AvroDeserializer(schema_enriched_event_str,schema_registry_client)

    consumer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.deserializer': string_deserializer,
                     'value.deserializer': avro_deserializer,
                     'group.id': args.group+str(random.Random()),
                     'auto.offset.reset': "earliest"}

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])

    cluster = Cluster([args.host])
    session = cluster.connect("datascience")

    cluster.register_user_type('datascience', 'datafield', Datafield)

    client_influxdb = InfluxDBClient('35.181.155.182', 8086, "dbsaleh2")
    # client_influxdb = InfluxDBClient(url="http://35.181.155.182:8086 , "mydb")


    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            start = time.time()
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            evt = msg.value()

            query = f"""
            insert into eventenrich (
                        "eventId" ,
                        "dateTimeRef",
                        "nomenclatureEv",
                        "canal",
                        "media",
                        "schemaVersion",
                        "headerVersion",
                        "serveur",
                        "adresseIP",
                        "idTelematique",
                        "idPersonne",
                        "dateNaissance",
                        "paysResidence",
                        "paysNaissance",
                        "revenusAnnuel",
                        "csp",
                        "eventBC",
                        "eventContent"
                        )
                        VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s)
                    """

            #eventBc = evt["EventBusinessContext"][0].replace("com.bnpparibas.dsibddf.event.","")
            eventBc = evt["eventBC"].replace("com.bnpparibas.dsibddf.event.","")
            eventContent = evt["EventBusinessContext"][1]

            transformed_event = transform_enriched_event_to_cassandra_model(evt, eventBc, schema_dict, eventContent)

            insert_enriched_event_to_cassandra(transformed_event, session, query)

            elapsed_time = (time.time() - start)

        except Exception as e:
            print(f"Exception => {e}")
            continue

        query = 'SELECT * FROM metrics'
        result = client_influxdb.query(query, database="dbsaleh2")
        print(result)

        data=[]

        print(elapsed_time)
        metrics = [{
            "measurement": "metrics",
            "fields":{
                "metricName" : "hystorize",
                "timeforhystorize": elapsed_time
            }
        }]
        data.append(metrics)

        # client_influxdb.write_points("hystorize",elapsed_time, database="dbsaleh2")
        client_influxdb.write_points(metrics, database="dbsaleh2")
        producer.produce(topic=outputtopic, value={'metricName':"hystorize",'time':elapsed_time}, on_delivery=delivery_report)
        producer.flush()

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