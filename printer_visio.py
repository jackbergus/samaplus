import json
import pandas
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())

import data
example = data.example

def dictionary_extend2(d1, d2):
    toReturn = {}
    for x in set.union(set(d1.keys()), set(d2.keys())):
        if x in d1 and x in d2:
            ls = list(d1[x])
            ls.extend(d2[x])
            toReturn[x] = ls
        elif x in d1:
            toReturn[x] = d1[x]
        else:
            toReturn[x] = d2[x]
    return toReturn

def edgeInformation(src, label, dst, group, queryId, answerId, hypothesisId):
    return {'from': src, 'to': dst, 'group': group, 'label': label, 'query': queryId, 'subgraph_no': int(answerId), 'matched_hypothesis': hypothesisId}

def vertexInformation(id, group, label, title, queryId, answerId, hypothesisId):
    return {'id': id, 'label': label, 'group': group, 'title': title, 'query': queryId, 'subgraph_no': int(answerId), 'matched_hypothesis': hypothesisId}

def graphInformation(lambdaVal, queryId, answerId, hypothesisId):
    return {'lambda': int(lambdaVal), 'query': queryId, 'subgraph_no': int(answerId), 'matched_hypothesis': hypothesisId}

VV = []
EE = []
##GG = pandas.DataFrame(columns=graphInformation("", "", "", "").keys())
GG = []

mainPath = "/home/giacomo/Scrivania/evaluation/sama/"
classificationPath = mainPath + "classification.csv"
classification = pandas.read_csv(classificationPath)
for queryId in example.keys():
    queryId = 'F001_Q002Q004Q005'
    queryEdges = example[queryId]['edges']
    entrypoints = example[queryId]['pipeline_entrypoints']
    queryNodes = set()
    queryNodeProperties = []
    for edge in queryEdges:
        queryNodes.add(edge[0])
        queryNodes.add(edge[2])
    with open(mainPath+queryId+".json") as f:
        data = {}
        data = json.load(f)
        countDebugExamples = 0
        for answerId in data.keys():
            answer = data[answerId]
            totalScore = answer['totalScore']
            dictionary = answer['dictionary']
            ## TODO
            if countDebugExamples == 100:
                print("Stopping here for evaluation purposes")
                break
            if int(totalScore) > 3:
                print("Skipping Answer: " + str(answerId) + "/" + str(len(data.keys())))
                continue
            countDebugExamples = countDebugExamples +1
            print("Answer: " + str(answerId) + "/" + str(len(data.keys())))
            df = pysqldf("SELECT distinct matched_hypothesis from classification where query_id='"+queryId+"' and subgraph_id='"+answerId+"'")
            if (len(df) == 0):
                continue
            for index, row in df.iterrows():
                hypothesisId = row['matched_hypothesis']
                if (hypothesisId is None):
                    hypothesisId = "NA"
                V = []
                for queryNode in queryNodes:
                    V.append(vertexInformation(queryNode, 'query',
                                               'Query Entry-Point' if queryNode in entrypoints else 'Query Variable',
                                               queryNode,
                                               queryId, answerId, hypothesisId))
                hypoNodes2 = set()
                dfH = pysqldf("SELECT er, annotated_as from classification where query_id='" + queryId + "' and subgraph_id='" + answerId + "' and matched_hypothesis='" + hypothesisId + "'")
                classQ = pandas.Series(dfH.annotated_as.values, index=dfH.er).to_dict()



                ## Edge recreation for display
                E = list(map(lambda x : edgeInformation(x[0], x[1], x[2], "query", queryId, answerId, hypothesisId), queryEdges))
                alignment_dictionary = {}
                for aligned_paths in answer['tree']:
                    alignment_dictionary = dictionary_extend2(alignment_dictionary, aligned_paths['nodeAlignmentMap'])
                    for edge in aligned_paths['p']:
                        E.append(edgeInformation(edge[0], edge[1], dictionary[edge[2]], "data", queryId, answerId, hypothesisId))
                        hypoNodes2.add(edge[0])
                        hypoNodes2.add(edge[2])
                for alignment_src in alignment_dictionary.keys():
                    for alignment_dst in alignment_dictionary[alignment_src]:
                        E.append(edgeInformation(alignment_src, 'α', dictionary[alignment_dst] if alignment_dst in dictionary else alignment_dst, 'alignment', queryId, answerId, hypothesisId))
                EE.extend(E)

                ## Vertex recreation for display
                V.extend(list(map(lambda x : vertexInformation(dictionary[x] if x in dictionary else x, classQ[x] if x in classQ else "data", dictionary[x] if x in dictionary else x, x, queryId, answerId, hypothesisId), hypoNodes2)))

                VV.extend(V)
                GG.append(graphInformation(totalScore, queryId, answerId, hypothesisId))
    break

print("Vertices")
pandas.DataFrame(VV).to_csv(mainPath+"vertices.csv", sep='\t', encoding='utf-8', index=False)
print("Edges")
pandas.DataFrame(EE).to_csv(mainPath+"edges.csv", sep='\t', encoding='utf-8', index=False)
print("Graph")
pandas.DataFrame(GG).sort_values(by=['lambda', 'query', 'subgraph_no']).to_csv(mainPath+"graphs.csv", sep="\t", encoding='utf-8', index=False)