CREATE TYPE acteurDeclen(
  adresseIP text,
  idTelematique text,
  idPersonne text
);

CREATE TYPE dataheader (
  eventId text,
  dateTimeRef double,
  nomenclatureEv text,
  canal int,
  media int,
  schemaVersion text,
  headerVersion text,
  serveur text,
  acteurDeclencheur frozen<acteurDeclen>
);

CREATE TYPE datafield (
  name text,
  value text,
  datatype text
);
CREATE TYPE dataEnrich (
  dateNaissance text,
  paysResidence text,
  paysNaissance text,
  revenusAnnuel float,
  csp text
);

create table eventenrich(
eventHeader frozen <dataheader> PRIMARY KEY,
eventBC text,
eventContent set<frozen <datafield>>,
enrichedData set<frozen <dataEnrich>>
);

create table EventHeader(
eventId text  PRIMARY KEY,
dateTimeRef timestamp,
nomenclatureEv text,
canal int, media int,
schemaVersion text,
headerVersion text,
serveur text,
adresseIP text,
idTelematique text,
idPersonne text);

create table event(
eventId text  PRIMARY KEY,
eventBC text,
eventContent set<frozen <datafield>>
);

CREATE TABLE person(
idPersonne text PRIMARY KEY,
dateNaissance text,
paysResidence text,
paysNaissance text,
revenusAnnuel float,
csp text
);