from pycelonis import get_celonis
import pandas as pd
import traceback
import networkx as nx
import frozendict

url = "URL"
api = "API"

o2c = pd.read_csv("O2C.csv", dtype=str)
p2p = pd.read_csv("P2P.csv", dtype=str)

o2c = o2c[["case:concept:name", "concept:name", "time:timestamp", "VBELN"]]
o2c["time:timestamp"] = pd.to_datetime(o2c["time:timestamp"])
p2p = p2p[["case:concept:name", "concept:name", "time:timestamp", "EBELN"]]
p2p["time:timestamp"] = pd.to_datetime(p2p["time:timestamp"])
"""
o2c["@@index"] = o2c.index
p2p["@@index"] = p2p.index
o2c = o2c.sort_values(["case:concept:name", "time:timestamp", "@@index"])
p2p = p2p.sort_values(["case:concept:name", "time:timestamp", "@@index"])
del o2c["@@index"]
del p2p["@@index"]
"""

o2c_red = o2c[["case:concept:name", "VBELN"]].to_dict("r")
p2p_red = p2p[["case:concept:name", "EBELN"]].dropna(subset=["EBELN"]).to_dict("r")

map_vbeln = {x["VBELN"]: x["case:concept:name"] for x in o2c_red}
map_ebeln = {x["EBELN"]: x["case:concept:name"] for x in p2p_red}

o2c.columns = ["CASE_ID_LEFT", "ACTIVITY", "TIMESTAMP", "VBELN"]
p2p.columns = ["CASE_ID_RIGHT", "ACTIVITY", "TIMESTAMP", "VBELN"]

objects_o2c = {"CASE_ID_LEFT": list(o2c["CASE_ID_LEFT"].unique())}
objects_p2p = {"CASE_ID_RIGHT": list(p2p["CASE_ID_RIGHT"].unique())}

objects_o2c = pd.DataFrame(objects_o2c)
objects_p2p = pd.DataFrame(objects_p2p)

relations = []
for x in map_vbeln:
    if x in map_ebeln:
        relations.append({"CASE_ID_LEFT": map_vbeln[x], "CASE_ID_RIGHT": map_ebeln[x]})
relations = pd.DataFrame(relations)

celonis = get_celonis(url, api)
data_pool = celonis.pools.find("AB_SAP")
try:
    data_model = data_pool.create_datamodel("AB_SAP1")
except:
    data_model = data_pool.datamodels.find("AB_SAP1")

tables = [x.name for x in data_model.tables]
print(tables)
if "O2C_ACTIVITIES" not in tables:
    data_pool.create_table(o2c, "O2C_ACTIVITIES")
    data_pool.create_table(p2p, "P2P_ACTIVITIES")
    data_pool.create_table(objects_o2c, "O2C_CASES")
    data_pool.create_table(objects_p2p, "P2P_CASES")
    data_pool.create_table(relations, "RELATIONS_O2C_P2P")

    data_model.add_table_from_pool("O2C_ACTIVITIES", "O2C_ACTIVITIES")
    data_model.add_table_from_pool("P2P_ACTIVITIES", "P2P_ACTIVITIES")
    data_model.add_table_from_pool("O2C_CASES", "O2C_CASES")
    data_model.add_table_from_pool("P2P_CASES", "P2P_CASES")
    data_model.add_table_from_pool("RELATIONS_O2C_P2P", "RELATIONS_O2C_P2P")

    data_model.create_foreign_key("O2C_ACTIVITIES", "O2C_CASES", [("CASE_ID_LEFT", "CASE_ID_LEFT")])
    data_model.create_foreign_key("P2P_ACTIVITIES", "P2P_CASES", [("CASE_ID_RIGHT", "CASE_ID_RIGHT")])

    data_model.create_foreign_key("O2C_CASES", "RELATIONS_O2C_P2P", [("CASE_ID_LEFT", "CASE_ID_LEFT")])
    data_model.create_foreign_key("P2P_CASES", "RELATIONS_O2C_P2P", [("CASE_ID_RIGHT", "CASE_ID_RIGHT")])

    data_model.create_process_configuration(case_table="O2C_CASES", activity_table="O2C_ACTIVITIES", case_column="CASE_ID_LEFT", activity_column="ACTIVITY", timestamp_column="TIMESTAMP")
    data_model.create_process_configuration(case_table="P2P_CASES", activity_table="P2P_ACTIVITIES", case_column="CASE_ID_RIGHT", activity_column="ACTIVITY", timestamp_column="TIMESTAMP")
    data_model.reload()
