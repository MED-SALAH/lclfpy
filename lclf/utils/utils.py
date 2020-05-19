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


def enrich_db(evt, eventBc, schema_dict, eventContent, session, query):
    for v in schema_dict["fields"][16]["type"]:
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
    event_enrich = event_enrich + (eventBc,)
    event_enrich = event_enrich + ((set(newEventContent)),)
    session.execute(query, event_enrich)