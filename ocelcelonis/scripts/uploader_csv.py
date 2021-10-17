from pycelonis import get_celonis
import pandas as pd
import traceback
import networkx as nx
import frozendict


def get_query(events_table1, events_table2, miner):
    query = "\"" + events_table1 + "\".\"CASE_ID_LEFT\", \"" + events_table1 + "\".\"ACTIVITY\", TRANSIT_COLUMN( "+miner+" ( \"" + events_table2 + "\".\"ACTIVITY\", \"" + events_table1 + "\".\"ACTIVITY\" ), \"" + events_table2 + "\".\"CASE_ID_RIGHT\"), TRANSIT_COLUMN( "+miner+" ( \"" + events_table2 + "\".\"ACTIVITY\", \"" + events_table1 + "\".\"ACTIVITY\" ), \"" + events_table2 + "\".\"ACTIVITY\")"
    return query


def get_data_frame(analysis, events_table1, events_table2, miner):
    query = get_query(events_table1, events_table2, miner)
    df = analysis.get_data_frame(get_query(events_table1, events_table2, miner))
    df.columns = ["CASE_ID_LEFT", "ACTIVITY_LEFT", "CASE_ID_RIGHT", "ACTIVITY_RIGHT"]
    df = df.sort_values(["CASE_ID_LEFT", "ACTIVITY_LEFT", "CASE_ID_RIGHT", "ACTIVITY_RIGHT"])

    return df


def transform_to_graph(activities_table_left, activities_table_right, case_relationships):
    stream_left = activities_table_left[["CASE_ID_LEFT", "ACTIVITY", "TIMESTAMP"]].to_dict("r")
    stream_right = activities_table_right[["CASE_ID_RIGHT", "ACTIVITY", "TIMESTAMP"]].to_dict("r")
    events = {}
    for ev in stream_left:
        if not ev["CASE_ID_LEFT"] in events:
            events[ev["CASE_ID_LEFT"]] = []
        events[ev["CASE_ID_LEFT"]].append((ev["ACTIVITY"], ev["TIMESTAMP"].timestamp(), ev["CASE_ID_LEFT"], "SOURCE"))
    for ev in stream_right:
        if not ev["CASE_ID_RIGHT"] in events:
            events[ev["CASE_ID_RIGHT"]] = []
        events[ev["CASE_ID_RIGHT"]].append((ev["ACTIVITY"], ev["TIMESTAMP"].timestamp(), ev["CASE_ID_RIGHT"], "TARGET"))

    case_relationships = case_relationships.to_dict("r")
    nodes = set(x["CASE_ID_LEFT"] for x in case_relationships).union(
        set(x["CASE_ID_RIGHT"] for x in case_relationships))
    G = nx.Graph()
    for n in nodes:
        G.add_node(n)
    for x in case_relationships:
        G.add_edge(x["CASE_ID_LEFT"], x["CASE_ID_RIGHT"])
    connected_components = list(nx.connected_components(G))

    return events, connected_components


def apply_python_interleaved_miner(activities_table_left, activities_table_right, case_relationships):
    events, connected_components = transform_to_graph(activities_table_left, activities_table_right, case_relationships)
    comp_cases = []
    interleavings = []
    for g in connected_components:
        comp_cases.append([])
        for n in g:
            comp_cases[-1] = comp_cases[-1] + events[n]
        comp_cases[-1] = sorted(comp_cases[-1], key=lambda x: (x[1], x[-1]))
        i = 0
        while i < len(comp_cases[-1]) - 1:
            seen_objects = set()
            j = i + 1
            while j < len(comp_cases[-1]):
                if comp_cases[-1][i][3] == comp_cases[-1][j][3] and comp_cases[-1][i][2] == comp_cases[-1][j][2]:
                    break
                if comp_cases[-1][i][3] == "SOURCE" and comp_cases[-1][j][3] == "TARGET":
                    if not comp_cases[-1][j][2] in seen_objects:
                        interleavings.append((comp_cases[-1][i][2], comp_cases[-1][i][0], comp_cases[-1][j][2],
                                              comp_cases[-1][j][0], comp_cases[-1][i][1], "S"))
                    seen_objects.add(comp_cases[-1][j][2])
                elif comp_cases[-1][i][3] == "TARGET" and comp_cases[-1][j][3] == "SOURCE":
                    if not comp_cases[-1][j][2] in seen_objects:
                        interleavings.append((comp_cases[-1][j][2], comp_cases[-1][j][0], comp_cases[-1][i][2],
                                              comp_cases[-1][i][0], comp_cases[-1][j][1], "T"))
                    seen_objects.add(comp_cases[-1][j][2])
                j = j + 1
            i = i + 1
    interleavings = pd.DataFrame(interleavings)
    interleavings.columns = ["CASE_ID_LEFT", "ACTIVITY_LEFT", "CASE_ID_RIGHT", "ACTIVITY_RIGHT", "TIMESTAMP",
                             "DIRECTION"]
    interleavings = interleavings[["CASE_ID_LEFT", "ACTIVITY_LEFT", "CASE_ID_RIGHT", "ACTIVITY_RIGHT"]]
    interleavings = interleavings.sort_values(["CASE_ID_LEFT", "ACTIVITY_LEFT", "CASE_ID_RIGHT", "ACTIVITY_RIGHT"])
    return interleavings


def apply_python_noninterleaved_miner(activities_table_left, activities_table_right, case_relationships):
    interleavings = apply_python_interleaved_miner(activities_table_left, activities_table_right, case_relationships)
    interleavings = interleavings.groupby(["ACTIVITY_LEFT", "CASE_ID_LEFT", "CASE_ID_RIGHT"]).first().reset_index()
    interleavings = interleavings.sort_values(["CASE_ID_LEFT", "ACTIVITY_LEFT", "CASE_ID_RIGHT", "ACTIVITY_RIGHT"])
    return interleavings


url = "URL"
api = "API"

events_table1 = "events41"
objects_table1 = "objects41"
events_table2 = "events42"
objects_table2 = "objects42"
relations_table = "relations4"

"""
events_table1 = "events21"
objects_table1 = "objects21"
events_table2 = "events22"
objects_table2 = "objects22"
relations_table = "relations2"
"""

events1 = pd.read_csv(events_table1+".csv")
events1["TIMESTAMP"] = pd.to_datetime(events1["TIMESTAMP"])
events2 = pd.read_csv(events_table2+".csv")
events2["TIMESTAMP"] = pd.to_datetime(events2["TIMESTAMP"])
relations = pd.read_csv(relations_table+".csv")
objects1 = pd.DataFrame({"CASE_ID_LEFT": events1["CASE_ID_LEFT"].unique()})
objects2 = pd.DataFrame({"CASE_ID_RIGHT": events2["CASE_ID_RIGHT"].unique()})

celonis = get_celonis(url, api)
data_pool = celonis.pools.find("AB_prova")
try:
    data_model = data_pool.create_datamodel("AB_prova1")
except:
    data_model = data_pool.datamodels.find("AB_prova1")

tables = [x.name for x in data_model.tables]
print(tables)
if events_table1 not in tables:
    data_pool.create_table(events1, events_table1)
    data_pool.create_table(objects1, objects_table1)
    data_pool.create_table(events2, events_table2)
    data_pool.create_table(objects2, objects_table2)
    data_pool.create_table(relations, relations_table)
    data_model.add_table_from_pool(events_table1, events_table1)
    data_model.add_table_from_pool(events_table2, events_table2)
    data_model.add_table_from_pool(objects_table1, objects_table1)
    data_model.add_table_from_pool(objects_table2, objects_table2)
    data_model.add_table_from_pool(relations_table, relations_table)
    data_model.create_foreign_key(events_table1, objects_table1, [("CASE_ID_LEFT", "CASE_ID_LEFT")])
    data_model.create_foreign_key(events_table2, objects_table2, [("CASE_ID_RIGHT", "CASE_ID_RIGHT")])
    data_model.create_foreign_key(objects_table1, relations_table, [("CASE_ID_LEFT", "CASE_ID_LEFT")])
    data_model.create_foreign_key(objects_table2, relations_table, [("CASE_ID_RIGHT", "CASE_ID_RIGHT")])
    data_model.create_process_configuration(case_table=objects_table1, activity_table=events_table1, case_column="CASE_ID_LEFT", activity_column="ACTIVITY", timestamp_column="TIMESTAMP")
    data_model.create_process_configuration(case_table=objects_table2, activity_table=events_table2, case_column="CASE_ID_RIGHT", activity_column="ACTIVITY", timestamp_column="TIMESTAMP")
    data_model.reload()

analysis = celonis.analyses.find("AB_prova22")

celonis_interleaved = get_data_frame(analysis, events_table1, events_table2, "TIMESTAMP_INTERLEAVED_MINER")
print("CELONIS INTERLEAVED")
print(celonis_interleaved)

our_interleaved = apply_python_interleaved_miner(events1, events2, relations)
print("OUR INTERLEAVED")
print(our_interleaved)

celonis_noninterleaved = get_data_frame(analysis, events_table1, events_table2, "TIMESTAMP_NONINTERLEAVED_MINER")
print("CELONIS NON INTERLEAVED")
print(celonis_noninterleaved)

our_non_interleaved = apply_python_noninterleaved_miner(events1, events2, relations)
print("OUR NON INTERLEAVED")
print(our_non_interleaved)
