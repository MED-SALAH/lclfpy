
CREATE TYPE datafield (
  name text,
  value text,
  datatype text,
  isnullable boolean
);

create table eventenrich(
  "eventId" text,
  "dateTimeRef" double,
  "nomenclatureEv" text,
  "canal" int,
  "media" int,
  "schemaVersion" text,
  "headerVersion" text,
  "serveur" text,
  "adresseIP" text,
  "idTelematique" text,
  "idPersonne" text,
  "enrichedData" set<frozen <datafield>>,
  "eventBC" text,
  "eventContent" set<frozen <datafield>>,
  PRIMARY KEY ("idPersonne","dateTimeRef")


);


