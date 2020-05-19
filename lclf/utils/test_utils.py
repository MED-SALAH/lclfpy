import json

from lclf.utils.utils import flat_content



def test_transform_enriched_event_to_cassandra_model():
    #GIVEN

    #WHEN

    #THEN
    print('test_transform_enriched_event_to_cassandra_model')

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
    print('result flat ===>',result)
    print('result flat ===>', expected)
    assert result == expected

if __name__ == '__main__':
    test_flat_content()