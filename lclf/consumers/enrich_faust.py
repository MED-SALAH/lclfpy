import time
from random import random
from uuid import uuid4

import faust
from faust import Table
from faust.serializers import codecs


from cassandra.cluster import Cluster
from confluent_kafka import DeserializingConsumer
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.serialization import StringSerializer
from schema_registry.client import SchemaRegistryClient as AvroSchemaRegistryClient
from schema_registry.client.schema import AvroSchema
from schema_registry.serializers import FaustSerializer

from lclf.custom.avro import AvroDeserializer
from lclf.schemas.event_schema_all import EventSchema, EventHeaderSchema, EnrichedEventSchema, GET_ENRICHED_DATA_QUERY
from cassandra.query import dict_factory


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for Event {}: {}".format(msg.key(), err))
        return
    print('Event  {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def enrich(producer, session, evt, outputtopic, delivery_report):

                row = session.execute(GET_ENRICHED_DATA_QUERY,
                                      (evt["EventHeader"]["acteurDeclencheur"]["idPersonne"],)).one()
                if row:
                    evt['EnrichedData'] = row
                    enrichedEvent = {
                        "eventId": evt["EventHeader"]["eventId"],
                        "dateTimeRef": evt["EventHeader"]["dateTimeRef"],
                        "nomenclatureEv": evt["EventHeader"]["nomenclatureEv"],
                        "canal": evt["EventHeader"]["canal"],
                        "media": evt["EventHeader"]["media"],
                        "schemaVersion": evt["EventHeader"]["schemaVersion"],
                        "headerVersion": evt["EventHeader"]["headerVersion"],
                        "serveur": evt["EventHeader"]["serveur"],
                        "adresseIP": evt["EventHeader"]["acteurDeclencheur"]["adresseIP"],
                        "idTelematique": evt["EventHeader"]["acteurDeclencheur"]["idTelematique"],
                        "idPersonne": evt["EventHeader"]["acteurDeclencheur"]["idPersonne"],
                        "dateNaissance": row["dateNaissance"],
                        "paysResidence": row["paysResidence"],
                        "paysNaissance": row["paysNaissance"],
                        "revenusAnnuel": row["revenusAnnuel"],
                        "csp": row["csp"],
                        "EventBusinessContext": evt["EventBusinessContext"]
                    }

                    print(f'EnrichedEvent={enrichedEvent}')

                    producer.produce(topic=outputtopic, key=str(uuid4()), value=enrichedEvent,
                                     on_delivery=delivery_report)
                    producer.flush()


def start():

    EventSchema = {
        "doc": "fields[1] représente le header de l'evenement, fields[2] représente la partie businessContext",
        "fields": [
            {
                "name": "EventHeader",
                "type": {
                    "fields": [
                        {
                            "name": "eventId",
                            "type": "string"
                        },
                        {
                            "doc": "Au format Timestamp UNIX",
                            "logicalType": "timestamp-millis",
                            "name": "dateTimeRef",
                            "type": "long"
                        },
                        {
                            "doc": "Code Nomenclature de l'événement",
                            "name": "nomenclatureEv",
                            "type": "string"
                        },
                        {
                            "name": "canal",
                            "type": "int"
                        },
                        {
                            "name": "media",
                            "type": "int"
                        },
                        {
                            "name": "schemaVersion",
                            "type": "string"
                        },
                        {
                            "name": "headerVersion",
                            "type": "string"
                        },
                        {
                            "name": "serveur",
                            "type": "string"
                        },
                        {
                            "name": "acteurDeclencheur",
                            "type": {
                                "fields": [
                                    {
                                        "name": "adresseIP",
                                        "type": "string"
                                    },
                                    {
                                        "name": "idTelematique",
                                        "type": "string"
                                    },
                                    {
                                        "name": "idPersonne",
                                        "type": "string"
                                    }
                                ],
                                "name": "ActeurDeclencheur",
                                "type": "record"
                            }
                        }
                    ],
                    "name": "EventHeader",
                    "type": "record"
                }
            },
            {
                "name": "EventBusinessContext",
                "type": [
                    {
                        "doc": "Schéma pour l'événement 00000008H CANALNET",
                        "fields": [
                            {
                                "name": "grilleIdent",
                                "type": "string"
                            },
                            {
                                "name": "codeRetourServiceMetier",
                                "type": "string"
                            },
                            {
                                "name": "referer",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "browserVersion",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "androidUDID",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "iosIDFA",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "appVersion",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "Format UUID",
                                "name": "idTmx",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            }
                        ],
                        "name": "CanalnetEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000008C CANALRIB",
                        "fields": [
                            {
                                "name": "numeroCompteBeneficiaire",
                                "type": "string"
                            },
                            {
                                "name": "codePaysResidence",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "codePaysResidenceIso",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "adresseBeneficiaire",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "nomCompletBeneficiaire",
                                "type": "string"
                            },
                            {
                                "name": "idListeBeneficiaire",
                                "type": "string"
                            },
                            {
                                "name": "idBeneficiaire",
                                "type": "string"
                            },
                            {
                                "doc": "0: courrier; 1: SMS; 2: cle digitale",
                                "name": "modeValidation",
                                "type": "int"
                            },
                            {
                                "name": "bicBeneficiaire",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "Format UUID",
                                "name": "idTmx",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            }
                        ],
                        "name": "CanalribEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000008D CANALMODRIB",
                        "fields": [
                            {
                                "name": "numeroCompteBeneficiaire",
                                "type": "string"
                            },
                            {
                                "name": "idListeBeneficiaire",
                                "type": "string"
                            },
                            {
                                "name": "idBeneficiaire",
                                "type": "string"
                            },
                            {
                                "doc": "0: courrier; 1: SMS; 2: cle digitale",
                                "name": "modeValidation",
                                "type": "int"
                            },
                            {
                                "name": "bicBeneficiaire",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "Format UUID",
                                "name": "idTmx",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            }
                        ],
                        "name": "CanalmodribEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000008E CANALVALID",
                        "fields": [
                            {
                                "name": "numeroCompteBeneficiaire",
                                "type": "string"
                            },
                            {
                                "doc": "0: correct; 1: errone; 2: bloque",
                                "name": "codeRetourServiceMetier",
                                "type": "int"
                            },
                            {
                                "doc": "0: courrier; 1: SMS; 2: cle digitale",
                                "name": "modeValidation",
                                "type": "int"
                            },
                            {
                                "name": "bicBeneficiaire",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "Format UUID",
                                "name": "idTmx",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            }
                        ],
                        "name": "CanalvalidEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000008F EXEVIRINTER",
                        "fields": [
                            {
                                "name": "numeroCompteDebiteur",
                                "type": "string"
                            },
                            {
                                "name": "numeroCompteCrediteur",
                                "type": "string"
                            },
                            {
                                "doc": "0 : CAC ; 1 : externe",
                                "name": "titularite",
                                "type": [
                                    "int",
                                    "null"
                                ]
                            },
                            {
                                "doc": "Immediat ou Differe",
                                "name": "choixImmediateteExecution",
                                "type": "string"
                            },
                            {
                                "name": "montant",
                                "type": "float"
                            },
                            {
                                "name": "devise",
                                "type": "string"
                            },
                            {
                                "name": "libelleMotif",
                                "type": "string"
                            },
                            {
                                "doc": "vide si OK",
                                "name": "codeRetourServiceMetier",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "1 : execute ; 2 : rejete)",
                                "name": "resultatOperation",
                                "type": [
                                    "int",
                                    "null"
                                ]
                            },
                            {
                                "doc": "Au format Timestamp UNIX",
                                "logicalType": "timestamp-millis",
                                "name": "dateExecDemandee",
                                "type": [
                                    "long",
                                    "null"
                                ]
                            },
                            {
                                "name": "referenceOperation",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "bicCrediteur",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "Format UUID",
                                "name": "idTmx",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "0 : le virement n'a pas fait l'objet d'un declenchement d'AF - 1 :le virement a fait l'objet d'un declenchement d'AF",
                                "name": "declenchementAF",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "0 : AF demande KO - 1 : AF demande OK - vide si declenchementAF = 0",
                                "name": "validationAF",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "1 : SMS - 2 : Clé digitale - vide si declenchementAF = 0",
                                "name": "modeValidationAF",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            }
                        ],
                        "name": "ExevirinterEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000008G EXEVIRSEPA",
                        "fields": [
                            {
                                "name": "numeroCompteDebiteur",
                                "type": "string"
                            },
                            {
                                "name": "numeroCompteCrediteur",
                                "type": "string"
                            },
                            {
                                "doc": "0 : CAC ; 1 : externe)",
                                "name": "titularite",
                                "type": [
                                    "int",
                                    "null"
                                ]
                            },
                            {
                                "doc": "Immediat ou Differe",
                                "name": "choixImmediateteExecution",
                                "type": "string"
                            },
                            {
                                "name": "montant",
                                "type": "float"
                            },
                            {
                                "name": "devise",
                                "type": "string"
                            },
                            {
                                "name": "libelleMotif",
                                "type": "string"
                            },
                            {
                                "doc": "vide si OK",
                                "name": "codeRetourServiceMetier",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "1 : execute ; 2 : rejete",
                                "name": "resultatOperation",
                                "type": [
                                    "int",
                                    "null"
                                ]
                            },
                            {
                                "doc": "Au format Timestamp UNIX",
                                "logicalType": "timestamp-millis",
                                "name": "dateExecDemandee",
                                "type": [
                                    "long",
                                    "null"
                                ]
                            },
                            {
                                "name": "referenceOperation",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "bicCrediteur",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "Format UUID",
                                "name": "idTmx",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "0 : le virement n'a pas fait l'objet d'un declenchement d'AF - 1 :le virement a fait l'objet d'un declenchement d'AF",
                                "name": "declenchementAF",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "0 : AF demande KO - 1 : AF demande OK - vide si declenchementAF = 0",
                                "name": "validationAF",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "1 : SMS - 2 : Clé digitale - vide si declenchementAF = 0",
                                "name": "modeValidationAF",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            }
                        ],
                        "name": "ExevirsepaEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000008J CANALMOBVAL",
                        "fields": [
                            {
                                "name": "numeroMobile",
                                "type": "string"
                            },
                            {
                                "doc": "Nombre d'essai d'activation du numéro de téléphone - 0 si activation OK",
                                "name": "nbTentativeActivation",
                                "type": "int"
                            }
                        ],
                        "name": "CanalmobvalEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000008K CANALMOBMOD",
                        "fields": [
                            {
                                "name": "numeroMobile",
                                "type": "string"
                            },
                            {
                                "doc": "0:Courrier;2:Cle digitale",
                                "name": "modeValidation",
                                "type": "int"
                            }
                        ],
                        "name": "CanalmobmodEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000008L MODIFPLAFONDSCARTES",
                        "fields": [
                            {
                                "name": "numeroContratCarte",
                                "type": "string"
                            },
                            {
                                "name": "pourcentageDispoRetrait",
                                "type": "int"
                            },
                            {
                                "name": "pourcentageDispoPaiement",
                                "type": "int"
                            },
                            {
                                "doc": "Au format Timestamp UNIX",
                                "logicalType": "timestamp-millis",
                                "name": "dateEcheanceCarte",
                                "type": "long"
                            },
                            {
                                "name": "ancienPlafondRetrait",
                                "type": "int"
                            },
                            {
                                "name": "nouveauPlafondRetrait",
                                "type": "int"
                            },
                            {
                                "name": "ancienPlafondPaiement",
                                "type": "int"
                            },
                            {
                                "name": "nouveauPlafondPaiement",
                                "type": "int"
                            },
                            {
                                "name": "codeRetourServiceMetier",
                                "type": "string"
                            }
                        ],
                        "name": "ModifplafondscartesEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000008M ENROLSTAF",
                        "fields": [
                            {
                                "doc": "Soft Token Authentication Forte - id d'enrolement auprès de Atos",
                                "name": "idSTAF",
                                "type": "int"
                            },
                            {
                                "name": "nomPersonnalise",
                                "type": "string"
                            },
                            {
                                "name": "terminalConnection",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "terminalOsName",
                                "type": "string"
                            },
                            {
                                "name": "terminalOsVersion",
                                "type": "string"
                            },
                            {
                                "name": "terminalAppVersion",
                                "type": "string"
                            },
                            {
                                "name": "terminalLang",
                                "type": "string"
                            },
                            {
                                "name": "deviceModel",
                                "type": "string"
                            },
                            {
                                "doc": "1 : Mode OTP SMS ; 2 : Mode OTP vocal",
                                "name": "modeValidation",
                                "type": "int"
                            }
                        ],
                        "name": "EnrolstafEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000008N REMPLSTAF",
                        "fields": [
                            {
                                "doc": "Soft Token Authentication Forte - id d'enrolement auprès de Atos",
                                "name": "idSTAF",
                                "type": "int"
                            },
                            {
                                "name": "idSTAFPrecedent",
                                "type": "int"
                            },
                            {
                                "name": "nomPersonnalise",
                                "type": "string"
                            },
                            {
                                "name": "terminalConnection",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "terminalOsName",
                                "type": "string"
                            },
                            {
                                "name": "terminalOsVersion",
                                "type": "string"
                            },
                            {
                                "name": "terminalAppVersion",
                                "type": "string"
                            },
                            {
                                "name": "terminalLang",
                                "type": "string"
                            },
                            {
                                "name": "deviceModel",
                                "type": "string"
                            }
                        ],
                        "name": "RemplstafEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000008O RETRAITSTAF",
                        "fields": [
                            {
                                "doc": "Soft Token Authentication Forte - id d'enrolement auprès de Atos",
                                "name": "idSTAF",
                                "type": "int"
                            }
                        ],
                        "name": "RetraitstafEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000008P ADRFISC",
                        "fields": [
                            {
                                "name": "nouveauCodePostal",
                                "type": "string"
                            },
                            {
                                "name": "nouveauCommune",
                                "type": "string"
                            },
                            {
                                "name": "nouveauVoie",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "nouveauLieuDit",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "nouveauComplementAdresse",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "ancienCodePostal",
                                "type": "string"
                            },
                            {
                                "name": "ancienCommune",
                                "type": "string"
                            },
                            {
                                "name": "ancienVoie",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "ancienLieuDit",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "ancienComplementAdresse",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            }
                        ],
                        "name": "AdrfiscEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000008Q ADRCOUR",
                        "fields": [
                            {
                                "name": "nouveauPays",
                                "type": "string"
                            },
                            {
                                "name": "nouveauCodePostal",
                                "type": "string"
                            },
                            {
                                "name": "nouveauCommune",
                                "type": "string"
                            },
                            {
                                "name": "nouveauVoie",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "nouveauLieuDit",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "nouveauComplementAdresse",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "ancienPays",
                                "type": "string"
                            },
                            {
                                "name": "ancienCodePostal",
                                "type": "string"
                            },
                            {
                                "name": "ancienCommune",
                                "type": "string"
                            },
                            {
                                "name": "ancienVoie",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "ancienLieuDit",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "ancienComplementAdresse",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            }
                        ],
                        "name": "AdrcourEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000008R TRANSAGENCE",
                        "fields": [
                            {
                                "name": "nouveauCodeAgence",
                                "type": "string"
                            },
                            {
                                "name": "ancienCodeAgence",
                                "type": "string"
                            }
                        ],
                        "name": "TransagenceEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000008S IDENTIFICATION-FINGERPRINT",
                        "fields": [
                            {
                                "name": "codeRetourServiceMetier",
                                "type": "string"
                            },
                            {
                                "name": "idDevice",
                                "type": "string"
                            },
                            {
                                "name": "androidUDID",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "iosIDFA",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "appVersion",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "Format UUID",
                                "name": "idTmx",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            }
                        ],
                        "name": "IdentificationfingerprintEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000008T ENROLEMENT-FINGERPRINT",
                        "fields": [
                            {
                                "name": "idDevice",
                                "type": "string"
                            },
                            {
                                "name": "codeRetourServiceMetier",
                                "type": "string"
                            }
                        ],
                        "name": "EnrolementfingerprintEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000008U DESENROLEMENT-FINGERPRINT",
                        "fields": [
                            {
                                "name": "idDevice",
                                "type": "string"
                            },
                            {
                                "name": "codeRetourServiceMetier",
                                "type": "string"
                            }
                        ],
                        "name": "DesenrolementfingerpritEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000009D VALIDATION-MDP-TOUCHID",
                        "fields": [
                            {
                                "name": "idDevice",
                                "type": "string"
                            },
                            {
                                "name": "codeRetourServiceMetier",
                                "type": "string"
                            }
                        ],
                        "name": "ValidationmdptouchidEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000009E VPP-CREATION",
                        "fields": [
                            {
                                "name": "numeroCompteDebiteur",
                                "type": "string"
                            },
                            {
                                "name": "numeroCompteCrediteur",
                                "type": "string"
                            },
                            {
                                "name": "montant",
                                "type": "float"
                            },
                            {
                                "doc": "code periodicite : par exemple 1T pour trimestriel ou 1A pour annuel",
                                "name": "periodicite",
                                "type": "string"
                            },
                            {
                                "doc": "Au format Timestamp UNIX",
                                "logicalType": "timestamp-millis",
                                "name": "prochaineEcheance",
                                "type": "long"
                            },
                            {
                                "doc": "Au format Timestamp UNIX, vide si pas de date de Fin",
                                "logicalType": "timestamp-millis",
                                "name": "dateFin",
                                "type": [
                                    "long",
                                    "null"
                                ]
                            },
                            {
                                "name": "libelle1",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "libelle2",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "codeRetourServiceMetier",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "vide si OK",
                                "name": "libelleRetourServiceMetier",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "devise",
                                "type": "string"
                            },
                            {
                                "name": "refOperation",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            }
                        ],
                        "name": "VppcreationEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000009F VPP-MODIFICATION",
                        "fields": [
                            {
                                "name": "numeroCompteDebiteur",
                                "type": "string"
                            },
                            {
                                "name": "numeroCompteCrediteur",
                                "type": "string"
                            },
                            {
                                "name": "montant",
                                "type": "float"
                            },
                            {
                                "doc": "code periodicite : par exemple 1T pour trimestriel ou 1A pour annuel",
                                "name": "periodicite",
                                "type": "string"
                            },
                            {
                                "doc": "Au format Timestamp UNIX",
                                "logicalType": "timestamp-millis",
                                "name": "prochaineEcheance",
                                "type": "long"
                            },
                            {
                                "doc": "Au format Timestamp UNIX, vide si pas de date de Fin",
                                "logicalType": "timestamp-millis",
                                "name": "dateFin",
                                "type": [
                                    "long",
                                    "null"
                                ]
                            },
                            {
                                "name": "libelle1",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "libelle2",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "codeRetourServiceMetier",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "vide si OK",
                                "name": "libelleRetourServiceMetier",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "devise",
                                "type": "string"
                            },
                            {
                                "name": "refOperation",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            }
                        ],
                        "name": "VppmodificationEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000009G VPP-SUPPRESSION",
                        "fields": [
                            {
                                "name": "numeroCompteDebiteur",
                                "type": "string"
                            },
                            {
                                "name": "numeroCompteCrediteur",
                                "type": "string"
                            },
                            {
                                "name": "montant",
                                "type": "float"
                            },
                            {
                                "doc": "code periodicite : par exemple 1T pour trimestriel ou 1A pour annuel",
                                "name": "periodicite",
                                "type": "string"
                            },
                            {
                                "doc": "Au format Timestamp UNIX",
                                "logicalType": "timestamp-millis",
                                "name": "prochaineEcheance",
                                "type": "long"
                            },
                            {
                                "doc": "Au format Timestamp UNIX, vide si pas de date de Fin",
                                "logicalType": "timestamp-millis",
                                "name": "dateFin",
                                "type": [
                                    "long",
                                    "null"
                                ]
                            },
                            {
                                "name": "codeRetourServiceMetier",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "vide si OK",
                                "name": "libelleRetourServiceMetier",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "devise",
                                "type": "string"
                            }
                        ],
                        "name": "VppsuppressionEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000009J RIB-EDITION",
                        "fields": [
                            {
                                "name": "numeroCompte",
                                "type": "string"
                            }
                        ],
                        "name": "RibeditionEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000009L CODESECRET-MODIFICATION",
                        "fields": [
                            {
                                "name": "codeRetourServiceMetier",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            }
                        ],
                        "name": "CodesecretmodificationEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000009M CHEQ-COMMANDE",
                        "fields": [
                            {
                                "name": "typeEnvoi",
                                "type": "string"
                            },
                            {
                                "name": "typeChequier",
                                "type": "string"
                            },
                            {
                                "name": "nbChequier",
                                "type": "int"
                            },
                            {
                                "name": "adresseLigne1",
                                "type": "string"
                            },
                            {
                                "name": "adresseLigne2",
                                "type": "string"
                            },
                            {
                                "name": "codeRetourServiceMetier",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            }
                        ],
                        "name": "CheqcommandeEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000009N OPPOSITION-CHEQUE",
                        "fields": [
                            {
                                "doc": "chèques signés ou chèques vierges",
                                "name": "typeOpposition",
                                "type": "string"
                            },
                            {
                                "name": "numPremierCheque",
                                "type": "string"
                            },
                            {
                                "doc": "vide si chèques signés",
                                "name": "numDernierCheque",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "vide si chèques vierges",
                                "name": "montant",
                                "type": [
                                    "float",
                                    "null"
                                ]
                            },
                            {
                                "name": "codeMotif",
                                "type": "string"
                            },
                            {
                                "name": "refOpposition",
                                "type": "string"
                            },
                            {
                                "name": "codeRetourServiceMetier",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            }
                        ],
                        "name": "OppositionchequeEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000009O OPPOSITION-CARTE",
                        "fields": [
                            {
                                "name": "numCarteAnonymise",
                                "type": "string"
                            },
                            {
                                "name": "numeroCompte",
                                "type": "string"
                            },
                            {
                                "name": "codeMotif",
                                "type": "string"
                            },
                            {
                                "name": "codeRetourServiceMetier",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            }
                        ],
                        "name": "OppositioncarteEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000009P OPPOSITION-PRELEVEMENT",
                        "fields": [
                            {
                                "name": "numeroCompteCrypte",
                                "type": "string"
                            },
                            {
                                "name": "numEmetteur",
                                "type": "string"
                            },
                            {
                                "name": "nomEmetteur",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "referenceContrat",
                                "type": "string"
                            },
                            {
                                "name": "echeanceContrat",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "montant",
                                "type": [
                                    "float",
                                    "null"
                                ]
                            }
                        ],
                        "name": "OppositionprelevementEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000009Q BMM",
                        "fields": [],
                        "name": "BmmEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000009H DEMAT",
                        "fields": [
                            {
                                "name": "idDocument",
                                "type": "string"
                            },
                            {
                                "name": "codeRetourServiceMetier",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            }
                        ],
                        "name": "DematEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 00000009R BOURSE-PASSATION-ORDRE",
                        "fields": [
                            {
                                "name": "referenceOrdre",
                                "type": "string"
                            },
                            {
                                "name": "libelleOrdre",
                                "type": "string"
                            },
                            {
                                "name": "codeIsin",
                                "type": "string"
                            },
                            {
                                "name": "typeValeur",
                                "type": "string"
                            },
                            {
                                "name": "typeOrdre",
                                "type": "string"
                            },
                            {
                                "name": "place",
                                "type": "string"
                            },
                            {
                                "name": "statut",
                                "type": "string"
                            },
                            {
                                "name": "sens",
                                "type": "string"
                            },
                            {
                                "name": "devise",
                                "type": "string"
                            },
                            {
                                "name": "qteInitiale",
                                "type": "string"
                            },
                            {
                                "doc": "Au format Timestamp UNIX",
                                "logicalType": "timestamp-millis",
                                "name": "dateEnregistrement",
                                "type": "long"
                            },
                            {
                                "name": "origine",
                                "type": "string"
                            },
                            {
                                "name": "validite",
                                "type": "string"
                            },
                            {
                                "name": "ibanCompteTitre",
                                "type": "string"
                            },
                            {
                                "name": "ibanCompteEspece",
                                "type": "string"
                            },
                            {
                                "name": "montantBrutEstime",
                                "type": "float"
                            },
                            {
                                "name": "montantNetEstime",
                                "type": "float"
                            }
                        ],
                        "name": "BoursepassationordreEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 0001D00000000014I PAYLIB-ACTIVATION-PRODUIT ",
                        "fields": [
                            {
                                "name": "idConnexion",
                                "type": "string"
                            },
                            {
                                "name": "idContrat",
                                "type": "string"
                            },
                            {
                                "doc": "email du compte client",
                                "name": "idPaylib",
                                "type": "string"
                            },
                            {
                                "doc": "Au format Timestamp UNIX",
                                "logicalType": "timestamp-millis",
                                "name": "dateActivation",
                                "type": "long"
                            }
                        ],
                        "name": "PaylibProduitEventBusinessContext",
                        "type": "record"
                    },
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
                    },
                    {
                        "doc": "Schéma pour l'événement 0001D00000000014K PAYLIB-ACTIVATION-HCE",
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
                                "name": "topActivationManuelle",
                                "type": "boolean"
                            }
                        ],
                        "name": "PaylibHCEEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 0001Q00000000017L EXEVIRSEPAIP",
                        "fields": [
                            {
                                "name": "numeroCompteDebiteur",
                                "type": "string"
                            },
                            {
                                "name": "numeroCompteCrediteur",
                                "type": "string"
                            },
                            {
                                "name": "bicCrediteur",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "Immediat ou Differe",
                                "name": "choixImmediateteExecution",
                                "type": "string"
                            },
                            {
                                "name": "montant",
                                "type": "float"
                            },
                            {
                                "name": "devise",
                                "type": "string"
                            },
                            {
                                "name": "libelleMotif",
                                "type": "string"
                            },
                            {
                                "doc": "1 : execute ; 2 : rejete",
                                "name": "resultatOperation",
                                "type": [
                                    "int",
                                    "null"
                                ]
                            },
                            {
                                "doc": "vide si OK",
                                "name": "libelleRetourServiceMetier",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "0 : CAC ; 1 : externe",
                                "name": "titularite",
                                "type": [
                                    "int",
                                    "null"
                                ]
                            },
                            {
                                "doc": "Au format Timestamp UNIX",
                                "logicalType": "timestamp-millis",
                                "name": "dateExecDemandee",
                                "type": [
                                    "long",
                                    "null"
                                ]
                            },
                            {
                                "name": "referenceOperation",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "Format UUID",
                                "name": "idTmx",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            }
                        ],
                        "name": "ExevirsepaipEventBusinessContext",
                        "type": "record"
                    },
                    {
                        "doc": "Schéma pour l'événement 0002000000000019X DEMEXEVIRSEPA",
                        "fields": [
                            {
                                "name": "numeroCompteDebiteur",
                                "type": "string"
                            },
                            {
                                "name": "numeroCompteCrediteur",
                                "type": "string"
                            },
                            {
                                "name": "bicCrediteur",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "name": "nomBeneficiaire",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            },
                            {
                                "doc": "Immediat ou Differe",
                                "name": "choixImmediateteExecution",
                                "type": "string"
                            },
                            {
                                "doc": "0 : CAC ; 1 : externe",
                                "name": "titularite",
                                "type": "int"
                            },
                            {
                                "name": "montant",
                                "type": "float"
                            },
                            {
                                "name": "devise",
                                "type": "string"
                            },
                            {
                                "name": "libelleMotif",
                                "type": "string"
                            },
                            {
                                "doc": "Au format Timestamp UNIX",
                                "logicalType": "timestamp-millis",
                                "name": "dateExecDemandee",
                                "type": [
                                    "long",
                                    "null"
                                ]
                            },
                            {
                                "doc": "Format UUID",
                                "name": "idTmx",
                                "type": [
                                    "string",
                                    "null"
                                ]
                            }
                        ],
                        "name": "DemexevirsepaEventBusinessContext",
                        "type": "record"
                    }
                ]
            }
        ],
        "name": "Event",
        "namespace": "com.bnpparibas.dsibddf.event",
        "type": "record"
    }

    client = AvroSchemaRegistryClient(url="http://35.180.127.210:8081")

    # schema that we want to use. For this example we
    # are using a dict, but this schema could be located in a file called avro_user_schema.avsc
    avro_event_schema = AvroSchema(EventSchema)

    avro_event_serializer = FaustSerializer(client, "events", avro_event_schema)

    # function used to register the codec
    def avro_event_codec():
        return avro_event_serializer

    codecs.register('avro_event_codec', avro_event_codec())

    topic = "event_ma_banque"

    app = faust.App('myapp',
                    broker_consumer='kafka://35.180.127.210:9092',
                    broker_producer='kafka://35.180.127.210:9092')

    schema = faust.Schema(
        value_serializer='avro_event_codec'
    )

    topic = app.topic(topic, schema=schema)

    table: Table = app.Table('total_event2', default=int, partitions=1)

    cluster = Cluster(["35.181.155.182"])
    session = cluster.connect("datascience")
    session.row_factory = dict_factory

    schema_enriched_event_str = EnrichedEventSchema
    sr_conf = {'url': "http://35.180.127.210:8081"}
    schema_registry_client = SchemaRegistryClient(sr_conf)
    avro_serializer = AvroSerializer(schema_enriched_event_str,
                                     schema_registry_client)

    producer_conf = {'bootstrap.servers': "35.180.127.210:9092",
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': avro_serializer}

    producer = SerializingProducer(producer_conf)

    @app.agent(topic)
    async def myagent(stream):
        async for evt in stream:
            start = time.time()
            idPersonne = evt['EventHeader']['acteurDeclencheur']['idPersonne']

            oldCount = table[idPersonne]

            print(f"value={evt}")
            print(f"old={oldCount}")


            yield enrich(producer=producer, session=session, evt=evt, outputtopic="enriched_event_ma_banque", delivery_report=delivery_report)

            table[idPersonne] += 1
            delta = time.time() - start
            print(f"delta = {delta}")


    app.main()
if __name__ == '__main__':
    start()