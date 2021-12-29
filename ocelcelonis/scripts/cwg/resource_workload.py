from pycelonis import get_celonis
from pycelonis.pql import PQL, PQLColumn, PQLFilter

celonis_url = "AAAAAAAAAAA"
api_key = open("api_key.txt", "r").read()

celonis = get_celonis(celonis_url=celonis_url, api_token=api_key)
analysis = celonis.analyses.find("BBBBBBBBBBBBB")

activity_table = "\"activity_table_csv\""
case_table = "\"case_table_csv\""
resource_column = "\"org:resource\""
case_id = "\"case:concept:name\""

pql = PQL()
pql.add(PQLColumn(name="resource", query=activity_table+"."+resource_column))
pql.add(PQLColumn(name="count", query="COUNT("+activity_table+"."+resource_column+")"))

resources = sorted(list(set(analysis.get_data_frame(pql)["resource"])))

pql = PQL()
pql.add(PQLColumn(name="case", query=case_table+"."+case_id))

for res in resources:
    q1 = "PU_SUM("+case_table+", CASE WHEN "+activity_table+"."+resource_column+" = '"+res+"' THEN 1 ELSE 0 END)"
    q2 = "PU_AVG("+case_table+", RUNNING_SUM(CASE WHEN "+activity_table+"."+resource_column+" = '"+res+"' THEN 1 WHEN ACTIVITY_LAG("+activity_table+"."+resource_column+", 1) = '"+res+"' THEN -1 ELSE 0 END))"
    print(q1)
    print(q2)
    pql.add(PQLColumn(name="@@res_count_"+res, query=q1))
    pql.add(PQLColumn(name="@@res_workload_"+res, query=q2))

dataframe = analysis.get_data_frame(pql)
print(dataframe)

dataframe.to_csv("output.csv", index=False)
