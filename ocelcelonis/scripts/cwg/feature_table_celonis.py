import pandas as pd
from pycelonis import get_celonis
from pycelonis.pql import PQL, PQLColumn, PQLFilter


def one_hot_encoding(analysis, activity_table, case_id, attribute):
    query = PQL()

    query.add(PQLColumn(name="values", query="DISTINCT(\"" + activity_table + "\".\"" + attribute + "\")"))

    unique_values = list(analysis.get_data_frame(query)["values"])

    query = PQL()
    query.add(PQLColumn(name="caseid", query="\"" + activity_table + "\".\"" + case_id + "\""))

    for val in unique_values:
        query.add(PQLColumn(name="@@1henc_" + attribute + "@" + val,
                            query="SUM(CASE WHEN \"" + activity_table + "\".\"" + attribute + "\" = '" + val + "' THEN 1 ELSE 0 END)"))

    dataframe = analysis.get_data_frame(query)
    return dataframe


def paths_encoding(analysis, activity_table, case_id, attribute):
    query = PQL()

    query.add(PQLColumn(name="source_attribute", query="SOURCE(\"" + activity_table + "\".\"" + attribute + "\")"))
    query.add(PQLColumn(name="target_attribute", query="TARGET(\"" + activity_table + "\".\"" + attribute + "\")"))
    query.add(PQLColumn(name="count",
                        query="COUNT(SOURCE(\"" + activity_table + "\".\"" + case_id + "\"))"))  # dummy statement just for aggregation

    paths = analysis.get_data_frame(query)
    paths["source_target_concat"] = paths["source_attribute"] + paths["target_attribute"]
    paths = list(paths["source_target_concat"])

    query = PQL()
    query.add(PQLColumn(name="caseid", query="SOURCE(\"" + activity_table + "\".\"" + case_id + "\")"))

    for val in paths:
        query.add(PQLColumn(name="@@paths_enc_" + attribute + "_" + val,
                            query="SUM(CASE WHEN CONCAT(SOURCE(\"" + activity_table + "\".\"" + attribute + "\"), TARGET(\"" + activity_table + "\".\"" + attribute + "\")) = '" + val + "' THEN 1 ELSE 0 END)"))

    dataframe = analysis.get_data_frame(query)
    return dataframe


def paths_sum_performance(analysis, activity_table, case_id, attribute, timestamp_key):
    query = PQL()

    query.add(PQLColumn(name="source_attribute", query="SOURCE(\"" + activity_table + "\".\"" + attribute + "\")"))
    query.add(PQLColumn(name="target_attribute", query="TARGET(\"" + activity_table + "\".\"" + attribute + "\")"))
    query.add(PQLColumn(name="count",
                        query="COUNT(SOURCE(\"" + activity_table + "\".\"" + case_id + "\"))"))  # dummy statement just for aggregation

    paths = analysis.get_data_frame(query)
    paths["source_target_concat"] = paths["source_attribute"] + paths["target_attribute"]
    paths = list(paths["source_target_concat"])

    query = PQL()
    query.add(PQLColumn(name="caseid", query="SOURCE(\"" + activity_table + "\".\"" + case_id + "\")"))

    for val in paths:
        query.add(PQLColumn(name="@@paths_enc_sum_perf_" + attribute + "_" + val,
                            query="SUM(CASE WHEN CONCAT(SOURCE(\"" + activity_table + "\".\"" + attribute + "\"), TARGET(\"" + activity_table + "\".\"" + attribute + "\")) = '" + val + "' THEN SECONDS_BETWEEN(SOURCE(\"" + activity_table + "\".\"" + timestamp_key + "\"), TARGET(\"" + activity_table + "\".\"" + timestamp_key + "\")) ELSE NULL END)"))

    dataframe = analysis.get_data_frame(query)
    return dataframe


def time_from_case_start(analysis, activity_table, case_id, attribute, timestamp_key):
    query = PQL()

    query.add(PQLColumn(name="values", query="DISTINCT(\"" + activity_table + "\".\"" + attribute + "\")"))

    unique_values = list(analysis.get_data_frame(query)["values"])

    query = PQL()
    query.add(PQLColumn(name="caseid", query="TARGET(\"" + activity_table + "\".\"" + case_id + "\")"))

    for val in unique_values:
        query.add(PQLColumn(name="@@min_time_from_start_"+attribute+"_"+val, query="MIN(CASE WHEN TARGET(\"" + activity_table + "\".\"" + attribute + "\") = '"+val+"' THEN SECONDS_BETWEEN(SOURCE(\"" + activity_table + "\".\"" + timestamp_key + "\", FIRST_OCCURRENCE [ ] TO ANY_OCCURRENCE [ ]), TARGET(\"" + activity_table + "\".\"" + timestamp_key + "\")) ELSE NULL END)"))

    dataframe = analysis.get_data_frame(query)
    return dataframe


def time_to_case_end(analysis, activity_table, case_id, attribute, timestamp_key):
    query = PQL()

    query.add(PQLColumn(name="values", query="DISTINCT(\"" + activity_table + "\".\"" + attribute + "\")"))

    unique_values = list(analysis.get_data_frame(query)["values"])

    query = PQL()
    query.add(PQLColumn(name="caseid", query="TARGET(\"" + activity_table + "\".\"" + case_id + "\")"))

    for val in unique_values:
        query.add(PQLColumn(name="@@max_time_to_end_"+attribute+"_"+val, query="MAX(CASE WHEN SOURCE(\"" + activity_table + "\".\"" + attribute + "\", ANY_OCCURRENCE[] TO LAST_OCCURRENCE[]) = '"+val+"' THEN SECONDS_BETWEEN(SOURCE(\"" + activity_table + "\".\"" + timestamp_key + "\", ANY_OCCURRENCE[] TO LAST_OCCURRENCE[]), TARGET(\"" + activity_table + "\".\"" + timestamp_key + "\")) ELSE NULL END)"))

    dataframe = analysis.get_data_frame(query)
    return dataframe


def first_last_index_attribute_value(analysis, activity_table, case_id, attribute):
    query = PQL()

    query.add(PQLColumn(name="values", query="DISTINCT(\"" + activity_table + "\".\"" + attribute + "\")"))

    unique_values = list(analysis.get_data_frame(query)["values"])

    query = PQL()
    query.add(PQLColumn(name="caseid", query="\"" + activity_table + "\".\"" + case_id + "\""))

    for val in unique_values:
        query.add(PQLColumn(name="@@first_index_av_"+attribute+"_"+val, query="MIN(CASE WHEN \""+activity_table+"\".\""+attribute+"\" = '"+val+"' THEN INDEX_ACTIVITY_ORDER(\""+activity_table+"\".\""+attribute+"\") ELSE NULL END)"))
        query.add(PQLColumn(name="@@last_index_av_"+attribute+"_"+val, query="MAX(CASE WHEN \""+activity_table+"\".\""+attribute+"\" = '"+val+"' THEN INDEX_ACTIVITY_ORDER(\""+activity_table+"\".\""+attribute+"\") ELSE NULL END)"))

    dataframe = analysis.get_data_frame(query)
    return dataframe


def num_events(analysis, activity_table, case_id):
    query = PQL()
    query.add(PQLColumn(name="caseid", query="\"" + activity_table + "\".\"" + case_id + "\""))
    query.add(PQLColumn(name="@@feat_num_events", query="MAX(INDEX_ACTIVITY_ORDER(\""+activity_table+"\".\""+case_id+"\"))"))

    dataframe = analysis.get_data_frame(query)
    return dataframe


def case_duration(analysis, activity_table, case_id, timestamp_key):
    query = PQL()
    query.add(PQLColumn(name="caseid", query="TARGET(\"" + activity_table + "\".\"" + case_id + "\")"))
    query.add(PQLColumn(name="@@feat_case_duration", query="SECONDS_BETWEEN(SOURCE(\""+activity_table+"\".\""+timestamp_key+"\", FIRST_OCCURRENCE[] TO LAST_OCCURRENCE[]), TARGET(\""+activity_table+"\".\""+timestamp_key+"\"))"))

    dataframe = analysis.get_data_frame(query)
    return dataframe
