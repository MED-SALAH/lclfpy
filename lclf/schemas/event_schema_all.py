

Event = """
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
					{"name": "eventId", "type": "string"},
					{"name": "dateTimeRef",  "type": "long", "logicalType" : "timestamp-millis", "doc" : "Au format Timestamp UNIX"},
					{"name": "nomenclatureEv",  "type": "string", "doc" : "Code Nomenclature de l'événement"},
					{"name": "canal",  "type": "int"},
					{"name": "media",  "type": "int"},
					{"name": "schemaVersion",  "type": "string"},
					{"name": "headerVersion",  "type": "string"},
					{"name": "serveur",  "type": "string"},
					{"name" : "acteurDeclencheur",
							"type" : {
							"type" : "record",
							"name" : "ActeurDeclencheur",
							"fields" : [
										{"name": "adresseIP",  "type": "string"},
										{"name": "idTelematique",  "type": "string"},
										{"name": "idPersonne",  "type": "string"}
										]
									}
						}
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
					{"name": "codeRetourServiceMetier", "type": "string"},
					{"name": "referer",  "type": ["string","null"]},
					{"name": "browserVersion",  "type": ["string","null"]},
					{"name": "androidUDID",  "type": ["string","null"]},
					{"name": "iosIDFA",  "type": ["string","null"]},
					{"name": "appVersion",  "type": ["string","null"]},
					{"name": "idTmx",  "type": ["string","null"], "doc" : "Format UUID"}
			   ]
			},
			{
				"type" : "record",
				"name" : "CanalribEventBusinessContext",
				"doc" : "Schéma pour l'événement 00000008C CANALRIB",
				"fields" : [
					{"name": "numeroCompteBeneficiaire", "type": "string"},
					{"name": "codePaysResidence",  "type": ["string","null"]},
					{"name": "codePaysResidenceIso",  "type": ["string","null"]},
					{"name": "adresseBeneficiaire",  "type": ["string","null"]},
					{"name": "nomCompletBeneficiaire", "type": "string"},
					{"name": "idListeBeneficiaire", "type": "string"},
					{"name": "idBeneficiaire", "type": "string"},
					{"name": "modeValidation", "type": "int",  "doc" : "0: courrier; 1: SMS; 2: cle digitale"},
					{"name": "bicBeneficiaire",  "type": ["string","null"]},
					{"name": "idTmx",  "type": ["string","null"], "doc" : "Format UUID"}
			   ]
			}
		]
	}
	]
}
"""