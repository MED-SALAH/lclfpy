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


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for Event {}: {}".format(msg.key(), err))
        return
    print('Event  {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


class Datafield(object):
    def __init__(self, name,value,datatype,isnullable):
        self.name = name
        self.value = value
        self.datatype = datatype
        self.isnullable = isnullable


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
                     'auto.offset.reset': "latest"}

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])

    cluster = Cluster([args.host])
    session = cluster.connect("datascience")

    cluster.register_user_type('datascience', 'datafield', Datafield)



    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            start = time.time()
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            evt = msg.value()
            print(evt)

            # print(myFunc())

            # query = f"""
            # insert into eventenrich (
            #             "eventheader" ,
            #             "enricheddata",
            #             "eventbc",
            #             "eventcontent"
            #             )
            #             VALUES (%s, %s, %s, %s)
            #
            #
            #         """
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

            # print(f"Query={query}")
            # print(evt)

            # eventId = evt["EventHeader"]["eventId"]
            eventBc = evt["EventBusinessContext"][0].replace("com.bnpparibas.dsibddf.event.","")
            eventContent = evt["EventBusinessContext"][1]

            # acteurDeclencheur = evt["EventHeader"]["acteurDeclencheur"]
            #
            # eventHeader = evt["EventHeader"]
            #
            # enrichedData = evt["EnrichedData"]
            print(schema_dict["fields"][16]["type"][0]["name"])

            if schema_dict["fields"][16]["type"][0]["name"] == eventBc:
                sch = schema_dict["fields"][16]["type"][0]["fields"]
                newEventContent = []
                for i in eventContent:
                    for j in sch:
                        if j["name"] == i:
                            if j["type"] == 'string':
                                newEventContent.append(Datafield(i,
                                                                 eventContent[i],
                                                                 j["type"],
                                                                 False
                                                                 ))
                                break
                            else:
                                newEventContent.append(Datafield(i,
                                                                 eventContent[i],
                                                                 j["type"][0],
                                                                 True
                                                                 ))
                                break


                session.execute(query, (evt["eventId"], evt["dateTimeRef"], evt["nomenclatureEv"], evt["canal"], evt["media"],
                                        evt["schemaVersion"], evt["headerVersion"], evt["serveur"], evt["adresseIP"], evt["idTelematique"],
                                        evt["idPersonne"], evt["dateNaissance"], evt["paysResidence"], evt["paysNaissance"],
                                        evt["revenusAnnuel"], evt["csp"], eventBc, set(newEventContent)))
            else:
                sch = schema_dict["fields"][16]["type"][1]["fields"]
                newEventContent = []
                for i in eventContent:
                    for j in sch:
                        if j["name"] == i:
                            if j["type"] == 'string':
                                newEventContent.append(Datafield(i,
                                                                 eventContent[i],
                                                                 j["type"],
                                                                 False
                                                                 ))
                                break
                            elif j["type"] == 'int':
                                newEventContent.append(Datafield(i,
                                                                 str(eventContent[i]),
                                                                 j["type"],
                                                                 False
                                                                 ))
                                break

                            else :
                                newEventContent.append(Datafield(i,
                                                                 eventContent[i],
                                                                 j["type"][0],
                                                                 True
                                                                 ))
                                break
                # print(len(newEventContent))
                # print(newEventHeader, newEnrichedData, eventBc, set(newEventContent))

                session.execute(query, (evt["eventId"], evt["dateTimeRef"], evt["nomenclatureEv"], evt["canal"], evt["media"],
                                 evt["schemaVersion"], evt["headerVersion"], evt["serveur"], evt["adresseIP"],
                                 evt["idTelematique"],evt["idPersonne"], evt["dateNaissance"], evt["paysResidence"],
                                 evt["paysNaissance"], evt["revenusAnnuel"], evt["csp"], eventBc, set(newEventContent)))


            elapsed_time = (time.time() - start)
            print(elapsed_time)


        except KeyboardInterrupt:
            break


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