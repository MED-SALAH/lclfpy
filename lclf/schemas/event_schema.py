

EventHeader = """
 {
   "type" : "record",
   "namespace": "com.example",
   "name" : "EventHeader",
   "fields" : [
         {"name": "eventId", "type": "string"},
         {"name": "dateTimeRef",  "type": "long", "logicalType" : "timestamp-millis", "doc" : "Au format Timestamp UNIX"},
         {"name": "nomenclatureEv",  "type": "string", "doc" : "Code Nomenclature de l'événement"},
         {"name": "canal",  "type": "int"},
         {"name": "media",  "type": "int"},
         {"name": "schemaVersion",  "type": "string"},
         {"name": "headerVersion",  "type": "string"},
         {"name": "serveur",  "type": "string"}
    ]
 }
"""