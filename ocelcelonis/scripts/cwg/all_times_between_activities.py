from pycelonis.pql import PQL, PQLColumn, PQLFilter

from pycelonis import get_celonis

celonis_url = "wil-personal.eu-1.celonis.cloud"
api_token = open("api_token.txt").read().strip()

celonis = get_celonis(celonis_url=celonis_url, api_token=api_token)

analysis = celonis.analyses.find("AB_RECEIPT_ANALYSIS")

query = PQL()
query.add(PQLColumn(name="source_activity", query="SOURCE(\"receipt_xes\".\"concept:name\")"))
query.add(PQLColumn(name="target_activity", query="TARGET(\"receipt_xes\".\"concept:name\")"))
query.add(PQLColumn(name="passed_time", query="SECONDS_BETWEEN(SOURCE(\"receipt_xes\".\"time:timestamp\"), TARGET(\"receipt_xes\".\"time:timestamp\"))"))
query.add(PQLColumn(name="case_id", query="TARGET(\"receipt_xes\".\"CASE ID\")"))

dataframe = analysis.get_data_frame(query)

print(dataframe)
dataframe.to_csv("time.csv", index=False)
