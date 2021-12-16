import pm4py
from pycelonis.pql import PQL, PQLColumn, PQLFilter


def repr_petri_celonis(net, im, fm, map_transition_names=True):
    transitions = (transition.name for transition in net.transitions)
    arcs = [(arc.source.name, arc.target.name) for arc in net.arcs]
    # Invisible transitions get no labels
    labels = [(transition.label, transition.name) for transition in net.transitions
              if transition.label is not None]

    if map_transition_names:
        trans_id_map = {transition.name: f'T{i}' for i, transition in enumerate(net.transitions)}

        def _check_and_map(x):
            # Only map transition ids (places are left out)
            if x in trans_id_map:
                return trans_id_map[x]
            return x

        transitions = (trans_id_map[transition] for transition in transitions)
        arcs = ((_check_and_map(src), _check_and_map(tgt)) for src, tgt in arcs)
        labels = ((label, trans_id_map[transition]) for label, transition in labels)

    places_str = " ".join(f'\"{place.name}\"' for place in net.places)
    transitions_str = " ".join(f'\"{transition}\"' for transition in transitions)
    arcs_str = " ".join(f'[\"{src}\" \"{tgt}\"]' for src, tgt in arcs)
    labels_str = " ".join(f'[\'{label}\' \"{transition}\"]' for label, transition in labels)
    im_str = " ".join(f'\"{place.name}\"' for place in im)
    fm_str = " ".join(f'\"{place.name}\"' for place in fm)

    return f"[ {places_str} ],\n" \
           f"[ {transitions_str} ],\n" \
           f"[ {arcs_str} ],\n" \
           f"[ {labels_str} ],\n" \
           f"[ {im_str} ],\n" \
           f"[ {fm_str} ]"


log = pm4py.read_xes("C:/receipt.xes")
net, im, fm = pm4py.discover_petri_net_inductive(log)
pm4py.view_petri_net(net, im, fm, format="svg")

petri_rep = repr_petri_celonis(net, im, fm)
print(petri_rep)

from pycelonis import get_celonis

celonis_url = "wil-personal.eu-1.celonis.cloud"
api_token = open("api_token.txt").read().strip()

celonis = get_celonis(celonis_url=celonis_url, api_token=api_token)

analysis = celonis.analyses.find("AB_RECEIPT_ANALYSIS")

query = PQL()
query.add(PQLColumn(name="caseid", query="\"receipt_xes\".\"CASE ID\""))
query.add(PQLColumn(name="align_activity", query="ALIGN_ACTIVITY(\"receipt_xes\".\"concept:name\", "+petri_rep+")"))
query.add(PQLColumn(name="align_move", query="ALIGN_MOVE(\"receipt_xes\".\"concept:name\", "+petri_rep+")"))

dataframe = analysis.get_data_frame(query)
dataframe.to_csv("alignment.csv", index=False)

print(dataframe)

print(dataframe["align_move"].value_counts())
