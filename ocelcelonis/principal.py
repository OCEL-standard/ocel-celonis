import os
import shutil

import frozendict
import pandas as pd
import traceback
import ocel
from pycelonis import get_celonis


class OcelCelonisTransf:
    def __init__(self):
        self.tables = {}
        self.foreign_keys = set()
        self.object_types = set()
        self.transitions = set()

    def fix(self):
        for table in self.tables:
            self.tables[table] = list(dict(x) for x in self.tables[table])
            self.tables[table] = pd.DataFrame(self.tables[table])


def __add_row_to_table(oct, table, row):
    if table not in oct.tables:
        oct.tables[table] = set()
    oct.tables[table].add(frozendict.frozendict(row))


def __add_foreign_key(oct, source_table, source_column, target_table, target_column):
    oct.foreign_keys.add((source_table, source_column, target_table, target_column))
    #oct.foreign_keys.add((target_table, target_column, source_table, source_column))


def read_event(log, oct, ev_id, event, allowed_object_types=None):
    for objid in event["ocel:omap"]:
        obj = log["ocel:objects"][objid]
        objtype = obj["ocel:type"]
        if allowed_object_types is None or objtype in allowed_object_types:
            ev = {"EVID_" + objtype: ev_id + ":" + objid, "CASE_" + objtype: objid,
                  "ACT_" + objtype: event["ocel:activity"], "TIME_" + objtype: event["ocel:timestamp"],
                  "SORT_TIME_" + objtype: event["ocel:timestamp"]}
            oct.object_types.add(objtype)
            for att in event["ocel:vmap"]:
                ev["ATT_EVENT_" + att] = event["ocel:vmap"][att]
            __add_row_to_table(oct, objtype + "_EVENTS", ev)
            ob = {"CASE_" + objtype: objid}
            for att in obj["ocel:ovmap"]:
                ob["ATT_" + objtype + "_" + att] = obj["ocel:ovmap"][att]
            __add_row_to_table(oct, objtype + "_CASES", ob)
            __add_foreign_key(oct, objtype + "_EVENTS", "CASE_"+objtype, objtype + "_CASES", "CASE_"+objtype)
            for objid2 in event["ocel:omap"]:
                if objid != objid2:
                    obj2 = log["ocel:objects"][objid2]
                    objtype2 = obj2["ocel:type"]
                    if allowed_object_types is None or objtype2 in allowed_object_types:
                        if objtype < objtype2 or (objtype == objtype2 and objid <= objid2):
                            if objtype != objtype2:
                                oct.transitions.add((objtype, objtype2))
                                objrel = {"SOURCE_CASE_" + objtype: objid, "TARGET_CASE_" + objtype2: objid2}
                                __add_row_to_table(oct, "CONNECT_" + objtype + "_CASES_" + objtype2 + "_CASES", objrel)
                                __add_foreign_key(oct, "CONNECT_" + objtype + "_CASES_" + objtype2 + "_CASES", "SOURCE_CASE_" + objtype, objtype + "_CASES", "CASE_" + objtype)
                                __add_foreign_key(oct, "CONNECT_" + objtype + "_CASES_" + objtype2 + "_CASES", "TARGET_CASE_" + objtype2, objtype2 + "_CASES", "CASE_" + objtype2)
                                evrel = {"SOURCE_EVID_" + objtype: ev_id + ":" + objid,
                                         "TARGET_EVID_" + objtype2: ev_id + ":" + objid2}
                                __add_row_to_table(oct, "CONNECT_" + objtype + "_EVENTS_" + objtype2 + "_EVENTS", evrel)
                                if False:
                                    __add_foreign_key(oct, "CONNECT_" + objtype + "_EVENTS_" + objtype2 + "_EVENTS", "SOURCE_EVID_" + objtype, objtype + "_EVENTS", "EVID_" + objtype)
                                    __add_foreign_key(oct, "CONNECT_" + objtype + "_EVENTS_" + objtype2 + "_EVENTS", "TARGET_EVID_" + objtype2, objtype2 + "_EVENTS", "EVID_" + objtype2)


def read_ocel(log, allowed_object_types=None):
    objects_in_events = set()
    oct = OcelCelonisTransf()
    for ev_id, ev in log["ocel:events"].items():
        objects_in_events = objects_in_events.union(ev["ocel:omap"])
        read_event(log, oct, ev_id, ev, allowed_object_types=allowed_object_types)
    oct.fix()
    return oct


def export_as_csv(oct, target_path):
    try:
        shutil.rmtree(target_path)
    except:
        pass
    os.mkdir(target_path)
    for table in oct.tables:
        oct.tables[table].to_csv(os.path.join(target_path, table), index=False)


def export_foreign_keys(oct, target_path):
    fk_list = sorted(list(oct.foreign_keys), key=lambda x: (x[0], x[3], x[1], x[2]), reverse=True)
    F = open(target_path, "w")
    for fk in fk_list:
        F.write(str(fk)+"\n")
    F.close()


def export_knowledge_yaml(oct):
    ret = []
    ret.append("eventLogsMetadata:")
    ret.append("    eventLogs:")
    for ot in oct.object_types:
        ret.append("        - id: "+ot)
        ret.append("          displayName: "+ot)
        ret.append("          pql: '\""+ot+"_EVENTS\".\"ACT_"+ot+"\"'")
    ret.append("    transitions:")
    for trans in oct.transitions:
        ret.append("        - id: "+trans[0]+"_"+trans[1])
        ret.append("          displayName: "+trans[0]+"_"+trans[1])
        ret.append("          firstEventLogId: "+trans[0])
        ret.append("          secondEventLogId: "+trans[1])
        ret.append("          type: INTERLEAVED")
    return "\n".join(ret)


def export_model_yaml(oct):
    ret = []
    ret.append("settings:")
    ret.append("    eventLogs:")
    for ot in oct.object_types:
        ret.append("        - eventLog: "+ot)
    return "\n".join(ret)


def get_data_model(data_pool, name):
    try:
        return data_pool.datamodels.find(name)
    except:
        return data_pool.create_datamodel(name)


def upload_to_celonis(oct, data_pool, data_model):
    for table in oct.tables:
        try:
            data_pool.create_table(oct.tables[table], table, if_exists="error")
        except:
            traceback.print_exc()
        try:
            data_model.add_table_from_pool(table, table)
        except:
            traceback.print_exc()
    for fk in oct.foreign_keys:
        try:
            data_model.create_foreign_key(fk[2], fk[0],
                                      [(fk[3], fk[1])])
        except:
            traceback.print_exc()
    for objtype in oct.object_types:
        try:
            data_model.create_process_configuration(case_table=objtype+"_CASES", activity_table=objtype+"_EVENTS", case_column="CASE_"+objtype, activity_column="ACT_"+objtype, timestamp_column="TIME_"+objtype, sorting_column="SORT_TIME_"+objtype)
        except:
            traceback.print_exc()
    try:
        print("... loading data model ...")
        data_model.reload()
    except:
        traceback.print_exc()


def cli():
    print("\n\nWelcome")
    log_path = input("Insert the path to the OCEL event log -> ")
    log = ocel.import_log(log_path)
    object_types = set()
    for ev_id, ev in log["ocel:events"].items():
        for obj_id in ev["ocel:omap"]:
            object_types.add(log["ocel:objects"][obj_id]["ocel:type"])
    object_types = ",".join(sorted(list(object_types)))
    selected_object_types = input("Insert the object types to consider separated by a comma without space (default: "+object_types+"): ")
    if len(selected_object_types) == 0:
        selected_object_types = object_types
    selected_object_types = selected_object_types.split(",")
    oct = read_ocel(log, selected_object_types)
    output_mode = int(input("Insert 1) if you want as output the CSVs along with the foreign keys. Insert 2) if you want to connect directly to Celonis -> "))
    if output_mode == 1:
        output_csv(oct)
    elif output_mode == 2:
        output_celonis(oct)
    output_yaml(oct)


def output_csv(oct):
    fk_path = input("Input the TXT file path to which the foreign keys should be saved -> ")
    export_foreign_keys(oct, fk_path)
    csv_path = input("Input the folder to which the CSV should be saved -> ")
    export_as_csv(oct, csv_path)


def output_celonis(oct):
    url = input("Insert Celonis URL -> ")
    api = input("Insert Celonis API -> ")
    celonis = get_celonis(api_token=api, celonis_url=url)
    data_pool_name = input("Insert the name of the target data pool (must be empty) -> ")
    data_pool = celonis.pools.find(data_pool_name)
    data_model_name = input("Insert the name of the target data model (must be empty) -> ")
    data_model = get_data_model(data_pool, data_model_name)
    upload_to_celonis(oct, data_pool, data_model)


def output_yaml(oct):
    file_path = input("insert the path where the YAML should be inserted -> ")
    F = open(file_path, "w")
    stru = export_knowledge_yaml(oct)
    F.write("YAML for knowledge model:\n\n")
    F.write(stru)
    stru = export_model_yaml(oct)
    F.write("\n\nYAML for process model:\n\n")
    F.write(stru)
    F.write("\n")
    F.close()


if __name__ == "__main__":
    cli()
