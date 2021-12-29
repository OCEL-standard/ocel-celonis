from pycelonis import get_celonis
from pycelonis.pql import PQL, PQLColumn, PQLFilter

celonis_url = "AAAAAAAAAAA"
api_key = open("api_key.txt", "r").read()

celonis = get_celonis(celonis_url=celonis_url, api_token=api_key)
analysis = celonis.analyses.find("BBBBBBBBBBBBB")

q1 = "\"case_table_csv\".\"case:concept:name\""
q2 = "PU_FIRST(\"case_table_csv\", \"activity_table_csv\".\"concept:name\")"
q3 = "PU_LAST(\"case_table_csv\", \"activity_table_csv\".\"concept:name\")"
q4 = "PU_COUNT(\"case_table_csv\", \"activity_table_csv\".\"concept:name\")"
q5 = "PU_MIN ( \"case_table_csv\", RUNNING_SUM( CASE WHEN INDEX_ACTIVITY_ORDER ( \"activity_table_csv\".\"concept:name\" ) = 1 THEN 1 WHEN INDEX_ACTIVITY_ORDER_REVERSE ( \"activity_table_csv\".\"concept:name\" ) = 1 THEN -1 ELSE 0 END, ORDER BY ( \"activity_table_csv\".\"time:timestamp\" ) ) )"
q6 = "PU_AVG ( \"case_table_csv\", RUNNING_SUM( CASE WHEN INDEX_ACTIVITY_ORDER ( \"activity_table_csv\".\"concept:name\" ) = 1 THEN 1 WHEN INDEX_ACTIVITY_ORDER_REVERSE ( \"activity_table_csv\".\"concept:name\" ) = 1 THEN -1 ELSE 0 END, ORDER BY ( \"activity_table_csv\".\"time:timestamp\" ) ) )"
q7 = "PU_MAX ( \"case_table_csv\", RUNNING_SUM( CASE WHEN INDEX_ACTIVITY_ORDER ( \"activity_table_csv\".\"concept:name\" ) = 1 THEN 1 WHEN INDEX_ACTIVITY_ORDER_REVERSE ( \"activity_table_csv\".\"concept:name\" ) = 1 THEN -1 ELSE 0 END, ORDER BY ( \"activity_table_csv\".\"time:timestamp\" ) ) )"

print(q1)
print(q2)
print(q3)
print(q4)
print(q5)
print(q6)
print(q7)

pql = PQL()
pql.add(PQLColumn(name="caseid", query=q1))
pql.add(PQLColumn(name="start_activity", query=q2))
pql.add(PQLColumn(name="end_activity", query=q3))
pql.add(PQLColumn(name="num_events", query=q4))
pql.add(PQLColumn(name="min_wip", query=q5))
pql.add(PQLColumn(name="avg_wip", query=q6))
pql.add(PQLColumn(name="max_wip", query=q7))

dataframe = analysis.get_data_frame(pql)
print(dataframe)

dataframe.to_csv("output.csv", index=False)
