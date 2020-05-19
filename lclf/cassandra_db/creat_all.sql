
CREATE TYPE datafield (
  name text,
  value text,
  datatype text,
  isnullable boolean
);

create table eventenrich(
  eventId text PRIMARY KEY,
  dateTimeRef double,
  nomenclatureEv text,
  canal int,
  media int,
  schemaVersion text,
  headerVersion text,
  serveur text,
  adresseIP text,
  idTelematique text,
  idPersonne text,
  dateNaissance text,
  paysResidence text,
  paysNaissance text,
  revenusAnnuel float,
  csp text,
  eventBC text,
  eventContent set<frozen <datafield>>
);


