import json
from collections import namedtuple

class Datafield(object):
    def __init__(self, name,value,datatype,isnullable):
        self.name = name
        self.value = value
        self.datatype = datatype
        self.isnullable = isnullable

def toNametuple(className, dict_data):
    return namedtuple(
        className, dict_data.keys()
    )(*tuple(map(lambda x: x if not isinstance(x, dict) else toNametuple(x), dict_data.values())))


def copy_keys(row, enrichedEvent):
    for k in row.keys():
        enrichedEvent[k] = row[k]

def flat_content(rightEventContext, paylibVADEventBusinessContextSchema):
     rightEventContext["listeCartes"] = json.dumps(rightEventContext["listeCartes"])


def transform_enriched_event_to_cassandra_model(evt, eventBc, schema_dict, eventContent):
    sch = []
    for v in schema_dict["fields"][17]["type"]:
        if v["name"] == eventBc:
            sch = v["fields"]
            break

    newEventContent = []
    for i in eventContent:
        for j in sch:
            if j["name"] == i:
                if type(j["type"]) != list:
                    newEventContent.append(Datafield(i,
                                                     str(eventContent[i]),
                                                     j["type"],
                                                     False
                                                     ))
                    break

                else:
                    newEventContent.append(Datafield(i,
                                                     eventContent[i],
                                                     j["type"][0],
                                                     True
                                                     ))
                    break
    event_enrich = ()
    for k in evt.keys():
        if type(evt[k]) != tuple:
            event_enrich = event_enrich + ((evt[k]),)
    #event_enrich = event_enrich + (eventBc,)
    event_enrich = event_enrich + ((set(newEventContent)),)
    print ("set(newEventContent",newEventContent)
    print(event_enrich)
    return event_enrich

def insert_enriched_event_to_cassandra(transformed_event, session, query):

    session.execute(query, transformed_event)

def stat_process(idPersonne, rows):
    # mean = mean(len(rows))
    # print(mean)
    print(type(rows))

    # list = []
    timestamp = rows[0]["dateTimeRef"]
    delta = 0

    for row in rows:
        delta = delta + row["dateTimeRef"]-timestamp
        # list.append(val)
        print(delta)

        timestamp = row["dateTimeRef"]
    moyen = delta/({rows.all().__len__()})
    print(idPersonne, moyen)

def rec_process( rows, some, indice) :
    if rows[indice + 1]:
        some = some + rows[indice + 1]["dateTimeRef"] - rows.one()["dateTimeRef"]
        rec_process(rows,some,indice + 1)
    return some

