

EventSchema = """
 {
	"namespace" : "com.example",
	"type" : "record",
	"name" : "Event",
	"doc" : "fields[1] représente le header de l'evenement, fields[2] représente la partie businessContext",
	"fields" : [
		{"name" : "EventHeader",
		 "type" : {
			  "type" : "record",
              "name" : "EventHeader",
			  "fields" : [
					{"name": "eventId", "type": "string"}
			   ]
			}
		 },
		{
		"name" : "EventBusinessContext",
			"type" : [
			{
				"type" : "record",
				"name" : "CanalnetEventBusinessContext",
				"doc" : "Schéma pour l'événement 00000008H CANALNET",
				"fields" : [
					{"name": "grilleIdent", "type": "string"},
					{"name": "codeRetourServiceMetier", "type": "string"}
			   ]
			},
			{
				"type" : "record",
				"name" : "CanalribEventBusinessContext",
				"doc" : "Schéma pour l'événement 00000008C CANALRIB",
				"fields" : [
					{"name": "numeroCompteBeneficiaire", "type": "string"}
			   ]
			}
		]
	}
	]
}
"""