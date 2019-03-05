import replace_entrypoints_in_query_for_sama as rep
import query_decomposition as qd
import sama
import data
import json
import sama_lambda

c = 0


def sama_with_disconnected_queries(sama_query, mappa, khop):
    global c
    qg = qd.QueryGraph(list(mappa.keys()), sama_query)
    L = []
    qg.prettyPrintPaths()
    for i in range(qg.getNumberOfConnectedComponents()):
        # print("Connected component #" + str(i))
        L.extend(
            sama.sama_with_path_decomposed_query(qg.getLambdaClusterKeysByConnectedComponentId(i), khop,
                                                 mappa))
        c = c + 1
        # qg.prettyPrintConnectedComponentPahts(i)
    return L


class SetEncoder(json.JSONEncoder):
    def default(self, obj):
       if isinstance(obj, set):
          return list(obj)
       return json.JSONEncoder.default(self, obj)

## List of results
L = []
main_path = "/home/giacomo/Scrivania/evaluation/sama/"
## Getting all the possible queries to be processed
for key in data.example.keys():
    key = 'F002_Q002Q004Q005'
    print(key)
    ls = rep.replace_entrypoints_in_query_for_sama(data.example[key])
    sama_query = ls[0]
    variable_to_entrypoint_map = ls[1]
    #for sama_query, variable_to_entrypoint_map in :
    lls = sama_with_disconnected_queries(sama_query, variable_to_entrypoint_map, data.example[key]['khop'])
    file = open(main_path+key+"2.json", "w", encoding='utf8')
    file.write(json.dumps(dict(zip(range(len(lls)), lls)),indent=4, cls=SetEncoder, ensure_ascii=False))
    file.close()
    file = open(main_path + key + "2.tsv", "w", encoding='utf8')
    for tree in lls:
        file.write(sama.simple_tree_string(tree))
    file.close()
    exit(10)
    #L.extend(lls)


