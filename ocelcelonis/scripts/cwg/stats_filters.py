def start_activities(analysis, activity_table, case_id, activity_key):
    query = PQL()
    query.add(PQLFilter("INDEX_ACTIVITY_ORDER(\"" + activity_table + "\".\"" + activity_key + "\") = 1"))
    query.add(PQLColumn(name="start_activity", query="\"" + activity_table + "\".\"" + activity_key + "\""))
    query.add(PQLColumn(name="count", query="COUNT(\"" + activity_table + "\".\"" + case_id + "\")"))

    dataframe = analysis.get_data_frame(query)
    return dataframe


def end_activities(analysis, activity_table, case_id, activity_key):
    query = PQL()
    query.add(PQLFilter("INDEX_ACTIVITY_ORDER_REVERSE(\"" + activity_table + "\".\"" + activity_key + "\") = 1"))
    query.add(PQLColumn(name="end_activity", query="\"" + activity_table + "\".\"" + activity_key + "\""))
    query.add(PQLColumn(name="count", query="COUNT(\"" + activity_table + "\".\"" + case_id + "\")"))

    dataframe = analysis.get_data_frame(query)
    return dataframe


def attribute_values(analysis, activity_table, case_id, attribute):
    query = PQL()
    query.add(PQLColumn(name="value", query="\"" + activity_table + "\".\"" + attribute + "\""))
    query.add(PQLColumn(name="count", query="COUNT(\"" + activity_table + "\".\"" + case_id + "\")"))

    dataframe = analysis.get_data_frame(query)
    return dataframe


def attribute_value_filter(analysis, activity_table, case_id, attribute, value):
    query = PQL()
    query.add(PQLFilter("\"" + activity_table + "\".\"" + attribute + "\" = '"+value+"'"))
    query.add(PQLColumn(name="caseid", query="DISTINCT(\"" + activity_table + "\".\"" + case_id + "\")"))

    dataframe = analysis.get_data_frame(query)
    return dataframe


def paths_filter(analysis, activity_table, case_id, attribute, path):
    query = PQL()
    query.add(PQLColumn(name="caseid", query="SOURCE(\"" + activity_table + "\".\"" + case_id + "\")"))
    query.add(PQLColumn(name="pf", query="SUM(CASE WHEN CONCAT(SOURCE(\"" + activity_table + "\".\"" + attribute + "\"), TARGET(\"" + activity_table + "\".\"" + attribute + "\")) = '"+path+"' THEN 1 ELSE 0 END)"))

    dataframe = analysis.get_data_frame(query)
    return dataframe


def paths_filter_2(analysis, activity_table, case_id, attribute, path):
    query = PQL()
    query.add(PQLFilter("CONCAT(SOURCE(\"" + activity_table + "\".\"" + attribute + "\"), TARGET(\"" + activity_table + "\".\"" + attribute + "\")) = '"+path+"'"))
    query.add(PQLColumn(name="caseid", query="DISTINCT(\"" + activity_table + "\".\"" + case_id + "\")"))

    dataframe = analysis.get_data_frame(query)
    return dataframe


def start_time_filter(analysis, activity_table, case_id, timestamp_key, min_timestmap, max_timestamp):
    query = PQL()
    query.add(PQLFilter("INDEX_ACTIVITY_ORDER(\"" + activity_table + "\".\"" + timestamp_key + "\") = 1"))
    query.add(PQLFilter("\""+activity_table+"\".\""+timestamp_key+"\" <= {d '"+max_timestamp+"'}"))
    query.add(PQLFilter("\""+activity_table+"\".\""+timestamp_key+"\" >= {d '"+min_timestmap+"'}"))
    query.add(PQLColumn(name="caseid", query="DISTINCT(\""+activity_table+"\".\""+case_id+"\")"))

    dataframe = analysis.get_data_frame(query)
    return dataframe


def end_time_filter(analysis, activity_table, case_id, timestamp_key, min_timestmap, max_timestamp):
    query = PQL()
    query.add(PQLFilter("INDEX_ACTIVITY_ORDER_REVERSE(\"" + activity_table + "\".\"" + timestamp_key + "\") = 1"))
    query.add(PQLFilter("\""+activity_table+"\".\""+timestamp_key+"\" <= {d '"+max_timestamp+"'}"))
    query.add(PQLFilter("\""+activity_table+"\".\""+timestamp_key+"\" >= {d '"+min_timestmap+"'}"))
    query.add(PQLColumn(name="caseid", query="DISTINCT(\""+activity_table+"\".\""+case_id+"\")"))

    dataframe = analysis.get_data_frame(query)
    return dataframe


def performance_filter(analysis, activity_table, case_id, timestamp_key, min_performance, max_performance):
    query = PQL()
    query.add(PQLFilter("SECONDS_BETWEEN(SOURCE(\"" + activity_table + "\".\"" + timestamp_key + "\"), TARGET(\"" + activity_table + "\".\"" + timestamp_key + "\")) >= "+str(min_performance)))
    query.add(PQLFilter("SECONDS_BETWEEN(SOURCE(\"" + activity_table + "\".\"" + timestamp_key + "\"), TARGET(\"" + activity_table + "\".\"" + timestamp_key + "\")) <= "+str(max_performance)))
    query.add(PQLColumn(name="caseid", query="DISTINCT(\""+activity_table+"\".\""+case_id+"\")"))

    dataframe = analysis.get_data_frame(query)
    return dataframe


def rework_count(analysis, activity_table, case_id, attribute_name, attribute_value):
    query = PQL()
    query.add(PQLFilter("\"" + activity_table + "\".\"" + attribute_name + "\" = '"+attribute_value+"'"))
    query.add(PQLColumn(name="caseid", query="\""+activity_table+"\".\""+case_id+"\""))
    query.add(PQLColumn(name="count", query="COUNT(\""+activity_table+"\".\""+case_id+"\")"))

    dataframe = analysis.get_data_frame(query)
    return dataframe
