import argparse
from uuid import uuid4

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer

from lclf.schemas.event_schema_all import Event as Ev
import random
import time

class Event(object):

    def __init__(self):
        self.EventHeader = EventHeader()
        self.EventBusinessContext = EventBusinessContext()

def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


class ActeurDeclencheur(object):
    def __init__(self,adresseIP = None, idTelematique = None, idPersonne = None):
        self.adresseIP = adresseIP
        self.idTelematique = idTelematique
        self.idPersonne = idPersonne

class EventHeader(object):

    def __init__(self,
                 eventId = None,
                 dateTimeRef = None,
                 nomenclatureEv = None,
                 canal = None,
                 media = None,
                 schemaVersion = None,
                 headerVersion = None,
                 serveur = None,
                 acteurDeclencheur = None
                 ):
        self.acteurDeclencheur = acteurDeclencheur
        self.serveur = serveur
        self.headerVersion = headerVersion
        self.schemaVersion = schemaVersion
        self.media = media
        self.canal = canal
        self.eventId = eventId
        self.nomenclatureEv = nomenclatureEv
        self.dateTimeRef = dateTimeRef

def eventHeader_to_dict(eventHeader, ctx):
    return dict(eventId=eventHeader.eventId,
                dateTimeRef=eventHeader.dateTimeRef,
                nomenclatureEv=eventHeader.nomenclatureEv,
                canal=eventHeader.canal,
                media=eventHeader.media,
                schemaVersion=eventHeader.schemaVersion,
                headerVersion=eventHeader.headerVersion,
                serveur=eventHeader.serveur)

class CanalnetEventBusinessContext(object):

    def __init__(self,
                 grilleIdent = None,
                 codeRetourServiceMetier = None,
                 referer = None,
                 browserVersion = None,
                 androidUDID = None,
                 iosIDFA = None,
                 appVersion = None,
                 idTmx = None):
        self.idTmx = idTmx
        self.appVersion = appVersion
        self.iosIDFA = iosIDFA
        self.androidUDID = androidUDID
        self.browserVersion = browserVersion
        self.referer = referer
        self.codeRetourServiceMetier = codeRetourServiceMetier
        self.grilleIdent = grilleIdent
        
class CanalribEventBusinessContext(object):
    
    def __init__(self,
                 numeroCompteBeneficiaire = None,
                 codePaysResidence = None,
                 codePaysResidenceIso = None,
                 adresseBeneficiaire = None,
                 nomCompletBeneficiaire = None,
                 idListeBeneficiaire = None,
                 idBeneficiaire = None,
                 modeValidation = None,
                 bicBeneficiaire = None,
                 idTmx = None):
        self.idTmx = idTmx
        self.bicBeneficiaire = bicBeneficiaire
        self.modeValidation = modeValidation
        self.idBeneficiaire = idBeneficiaire
        self.idListeBeneficiaire = idListeBeneficiaire
        self.nomCompletBeneficiaire = nomCompletBeneficiaire
        self.adresseBeneficiaire = adresseBeneficiaire
        self.codePaysResidenceIso = codePaysResidenceIso
        self.codePaysResidence = codePaysResidence
        self.numeroCompteBeneficiaire = numeroCompteBeneficiaire
        





def main(args):
    topic = args.topic
    schema_str = Ev

    schema_registry_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_str,
                                     schema_registry_client,
                                     eventHeader_to_dict)

    producer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': avro_serializer}

    producer = SerializingProducer(producer_conf)


    print("Producing user records to topic {}. ^C to exit.".format(topic))


    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.2)
        try:
            acteurDeclencheur = ActeurDeclencheur(adresseIP = "1.2.3.4",
                                                  idTelematique = "Id Telematique",
                                                  idPersonne = "id personne"
                                                  )
            eventHeader = EventHeader(eventId="iid",
                                      dateTimeRef=9999,
                                      nomenclatureEv="nomclature",
                                      canal=11,
                                      media=22,
                                      schemaVersion="v1",
                                      headerVersion="hv",
                                      serveur="serveur",
                                      acteurDeclencheur = acteurDeclencheur
                                      )
            producer.produce(topic=topic, key=str(uuid4()), value=eventHeader, on_delivery=delivery_report)
        except ValueError:
            print("Invalid input, discarding record...")
            continue

    print("\nFlushing records...")
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