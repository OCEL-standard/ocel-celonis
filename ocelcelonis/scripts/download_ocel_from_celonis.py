import pandas as pd
from pycelonis import get_celonis

url = "URL"
api = "API"

celonis = get_celonis(url, api)

data_pool = celonis.pools.find("AB_SAP")
data_model = data_pool.datamodels.find("AB_SAP1")
analysis = celonis.analyses.find("AB_SAP_analysis")

tables = {}
events = {}
objects = {}

for conf in data_model.process_configurations:
    tables[conf.activity_table.name] = conf

for tab in tables:
    activity_column = tables[tab].activity_column
    case_column = tables[tab].case_column
    timestamp_column = tables[tab].timestamp_column

    query = "\""+tab+"\".\""+case_column+"\", "+"\""+tab+"\".\""+activity_column+"\", "+"\""+tab+"\".\""+timestamp_column+"\""
    df = analysis.get_data_frame(query)
    df.columns = ["CASE_ID", "ACTIVITY", "TIMESTAMP"]
    df = df.to_dict("r")

    for ev in df:
        evv = (ev["ACTIVITY"], ev["TIMESTAMP"])
        if evv not in events:
            events[evv] = set()
        events[evv].add(tab+"_"+ev["CASE_ID"])

        objects[tab+"_"+ev["CASE_ID"]] = tab

for tab in tables:
    activity_column = tables[tab].activity_column
    case_column = tables[tab].case_column
    timestamp_column = tables[tab].timestamp_column
    for tab2 in tables:
        if tab != tab2:
            activity_column2 = tables[tab2].activity_column
            case_column2 = tables[tab2].case_column
            timestamp_column2 = tables[tab2].timestamp_column
            query = []
            query.append("\""+tab+"\".\""+activity_column+"\", ")
            query.append("\""+tab+"\".\""+timestamp_column+"\", ")
            query.append("TRANSIT_COLUMN( TIMESTAMP_INTERLEAVED_MINER (")
            query.append("\""+tab2+"\".\""+activity_column2+"\", ")
            query.append("\""+tab+"\".\""+activity_column+"\"), ")
            query.append("\""+tab2+"\".\""+case_column2+"\")")
            query = "".join(query)

            df = analysis.get_data_frame(query)
            df.columns = ["ACTIVITY", "TIMESTAMP", "CASE_ID_2"]
            df = df.to_dict("r")
            for ev in df:
                evv = (ev["ACTIVITY"], ev["TIMESTAMP"])
                events[evv].add(tab2+"_"+ev["CASE_ID_2"])

ocel_df = []
for index, evv in enumerate(events):
    dct = {"ocel:eid": "E"+str(index), "ocel:activity": evv[0], "ocel:timestamp": evv[1]}
    for obj in events[evv]:
        objtype = "ocel:type:"+objects[obj]
        if not objtype in dct:
            dct[objtype] = []
        dct[objtype].append(obj)
    ocel_df.append(dct)
ocel_df = pd.DataFrame(ocel_df)
#ocel_df = ocel_df.dropna(subset=["ocel:type:O2C_ACTIVITIES", "ocel:type:P2P_ACTIVITIES"], how="any")
#print(ocel_df)
ocel_df.to_csv("ocel.csv", index=False, na_rep="")
