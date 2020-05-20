import ast
import json

from lclf.schemas.event_schema_all import EnrichedEventSchema
from lclf.utils.utils import flat_content, transform_enriched_event_to_cassandra_model, Datafield


def test_transform_enriched_event_to_cassandra_model():
    #GIVEN
    schema_enriched_event_str = EnrichedEventSchema
    schema_dict = ast.literal_eval(schema_enriched_event_str)
    evt = {'eventId': 'zahir', 'dateTimeRef': 1589563205140, 'nomenclatureEv': 'zahir', 'canal': 42, 'media': 42, 'schemaVersion': 'zahir', 'headerVersion': 'zahir', 'serveur': 'zahir', 'adresseIP': 'zahir', 'idTelematique': 'zahir', 'idPersonne': 'zahir', 'dateNaissance': '21/08/1993', 'paysResidence': 'France', 'paysNaissance': 'Algerie', 'revenusAnnuel': 0.0, 'csp': 'etudiant', 'eventBC': 'CanalnetEventBusinessContext', 'EventBusinessContext': ('com.bnpparibas.dsibddf.event.CanalnetEventBusinessContext', {'grilleIdent': 'zahir', 'codeRetourServiceMetier': 'zahir', 'referer': 'zahir', 'browserVersion': 'zahir', 'androidUDID': 'zahir', 'iosIDFA': 'zahir', 'appVersion': 'zahir', 'idTmx': 'zahir'})}
    eventContent = {'grilleIdent': 'zahir', 'codeRetourServiceMetier': 'zahir', 'referer': 'zahir', 'browserVersion': 'zahir', 'androidUDID': 'zahir', 'iosIDFA': 'zahir', 'appVersion': 'zahir', 'idTmx': 'zahir'}
    eventBc = 'CanalnetEventBusinessContext'

    expectedEventContent = []
    expectedEventContent.append(Datafield('grilleIdent', 'zahir', 'string', False))
    expectedEventContent.append(Datafield('codeRetourServiceMetier', 'zahir', 'string', False))
    expectedEventContent.append(Datafield('referer', 'zahir', 'string', True))
    expectedEventContent.append(Datafield('browserVersion', 'zahir', 'string', True))
    expectedEventContent.append(Datafield('androidUDID', 'zahir', 'string', True))
    expectedEventContent.append(Datafield('iosIDFA', 'zahir', 'string', True))
    expectedEventContent.append(Datafield('appVersion', 'zahir', 'string', True))
    expectedEventContent.append(Datafield('idTmx', 'zahir', 'string', True))
    expected = ()
    expected = ('zahir', 1589563205140, 'zahir', 42, 42, 'zahir', 'zahir', 'zahir', 'zahir', 'zahir', 'zahir', '21/08/1993', 'France', 'Algerie', 0.0, 'etudiant', 'CanalnetEventBusinessContext')
    expected = expected + ((set(expectedEventContent)),)
    #WHEN
    result = transform_enriched_event_to_cassandra_model(evt, eventBc, schema_dict, eventContent)
    print("result is ",result)
    print("expect is ",expected)

    #THEN
    assert result[0:16] == expected[0:16]
    #assert result[17] == expected[17]

def test_flat_content():
    # GIVEN
    rightEventContext = dict()

    rightEventContext['idContrat'] = "AAA"
    rightEventContext['idPrestation'] = "AAA"
    rightEventContext['dateActivation'] = 1111111
    rightEventContext['listeCartes'] = [

        {"numeroCarte": "qsqsqs", "dateFinValidite": "06/2020"}
    ]

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
    eventContext = dict()

    eventContext['idContrat'] = "AAA"
    eventContext['idPrestation'] = "AAA"
    eventContext['dateActivation'] = 1111111
    eventContext['listeCartes'] = '[{"numeroCarte": "qsqsqs", "dateFinValidite": "06/2020"}]'

    expected = eventContext

    #WHEN
    flat_content(rightEventContext,paylibVADEventBusinessContextSchema)
    result = rightEventContext

    #THEN
    assert result == expected

if __name__ == '__main__':
    test_flat_content()
    test_transform_enriched_event_to_cassandra_model()