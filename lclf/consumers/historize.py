import argparse
import random
import time

from cassandra.cluster import Cluster
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringDeserializer

from lclf.custom.avro import AvroDeserializer
from lclf.schemas.event_schema_all import EnrichedEventSchema
import fastavro
import ast


class Datafield(object):
    def __init__(self, name,value,datatype,isnullable):
        self.name = name
        self.value = value
        self.datatype = datatype
        self.isnullable = isnullable

class EnrichedData(object):
    def __init__(self,datenaissance, paysresidence, paysnaissance, revenusannuel, csp):
        self.csp = csp
        self.datenaissance = datenaissance
        self.paysresidence = paysresidence
        self.paysnaissance = paysnaissance
        self.revenusannuel = revenusannuel


class ActeurDeclencheur(object):
    def __init__(self,adresseip = None, idtelematique = None, idpersonne = None):
        self.adresseip = adresseip
        self.idtelematique = idtelematique
        self.idpersonne = idpersonne

class EventHeader(object):

    def __init__(self,
                 eventid = None,
                 datetimeref = None,
                 nomenclatureev = None,
                 canal = None,
                 media = None,
                 schemaversion = None,
                 headerversion = None,
                 serveur = None,
                 acteurdeclencheur = None
                 ):
        self.acteurdeclencheur = acteurdeclencheur
        self.serveur = serveur
        self.headerversion = headerversion
        self.schemaversion = schemaversion
        self.media = media
        self.canal = canal
        self.eventid = eventid
        self.nomenclatureev = nomenclatureev
        self.datetimeref = datetimeref


def main(args):
    topic = args.topic

    schema_enriched_event_str = EnrichedEventSchema
    schema_dict = ast.literal_eval(schema_enriched_event_str)
    # print(schema_dict)

    sr_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_deserializer = AvroDeserializer(schema_enriched_event_str,schema_registry_client)
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

    cluster.register_user_type('datascience', 'datafield', Datafield)


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
            insert into eventenrich (
                        "eventheader" ,
                        "eventbc",
                        "eventcontent",
                        "enrichedData"
                        )
                        VALUES (%s, %s, %s, %s)


                    """

            # print(f"Query={query}")

            eventId = evt["EventHeader"]["eventId"]
            eventBc = evt["EventBusinessContext"][0]
            eventContent = evt["EventBusinessContext"][1]

            acteurDeclencheur = evt["EventHeader"]["acteurDeclencheur"]

            eventHeader = evt["EventHeader"]

            enrichedData = evt["EnrichedData"]

            newActeurDeclencheur = ActeurDeclencheur(acteurDeclencheur["adresseIP"],acteurDeclencheur["idTelematique"],
                                                     acteurDeclencheur["idPersonne"])

            newEventHeader = EventHeader(eventHeader["eventId"],eventHeader["dateTimeRef"],eventHeader["nomenclatureEv"],
                                         eventHeader["canal"],eventHeader["media"],eventHeader["schemaVersion"],
                                         eventHeader["headerVersion"],eventHeader["serveur"],newActeurDeclencheur)

            newEnrichedData = EnrichedData(enrichedData["dateNaissance"],enrichedData["paysResidence"],
                                           enrichedData["paysNaissance"],enrichedData["revenusAnnuel"],
                                           enrichedData["csp"])

            if schema_dict["fields"][1]["type"][0]["name"] == eventBc:
                sch = schema_dict["fields"][1]["type"][0]["fields"]
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
                session.execute(query, (newEventHeader, eventBc, set(newEventContent), newEnrichedData))
            else:
                sch = schema_dict["fields"][1]["type"][1]["fields"]
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
                # print(newEventContent[0].value, newEventContent[0].name, newEventContent[0].datatype, newEventContent[0].isnullable)
                session.execute(query, (newEventHeader, eventBc, set(newEventContent), newEnrichedData))


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