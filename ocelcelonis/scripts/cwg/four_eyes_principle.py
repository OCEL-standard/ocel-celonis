from pycelonis.pql import PQL, PQLColumn, PQLFilter

from pycelonis import get_celonis

celonis_url = "wil-personal.eu-1.celonis.cloud"
api_token = open("api_token.txt").read().strip()

celonis = get_celonis(celonis_url=celonis_url, api_token=api_token)

analysis = celonis.analyses.find("AB_RECEIPT_ANALYSIS")

query = PQL()
query.add(PQLFilter("SOURCE(\"receipt_xes\".\"org:resource\", REMAP_VALUES ( \"receipt_xes\".\"concept:name\", ['T02 Check confirmation of receipt', 'T02 Check confirmation of receipt'], ['T04 Determine confirmation of receipt', 'T04 Determine confirmation of receipt'], NULL )) = TARGET(\"receipt_xes\".\"org:resource\")"))
query.add(PQLColumn(name="case", query="\"receipt_xes\".\"CASE ID\""))

dataframe = analysis.get_data_frame(query)
print(dataframe)
