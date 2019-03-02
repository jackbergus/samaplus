import sama_lambda
import itertools
from query_decomposition import unique
from replace_entrypoints_in_query_for_sama import flatten

def alignmentAtom(queryId, alignmentLambdaScore, dataPath, nodeAlignment):
    return dict(q=queryId, p=dataPath, lambd=alignmentLambdaScore, nodeAlignmentMap=nodeAlignment)

def alignmentAtomFromSPDA(s_pd_a, queryId):
    return alignmentAtom(queryId, s_pd_a[0], s_pd_a[1], s_pd_a[2])

def treeAtom(alignmentAtomList, overallScore, d):
    return dict(tree=alignmentAtomList, totalScore=overallScore, dictionary=d)

def treeSingleton(q_id, p1_lambda_score, p1_data_path, p1_align, dictionary):
    return treeAtom([alignmentAtom(q_id, p1_lambda_score, p1_data_path, p1_align)], p1_lambda_score, dictionary)

def simple_tree_string(tree):
    return ("|".join(unique(filter(lambda z : (z[:1] == "E"), flatten(flatten(map(lambda x : map(lambda y: [y[0], y[2]] , x['p']),  tree["tree"])))))))+"\t"+("|".join(unique(filter(lambda z : (z[:2] == "VM") or (z[:2] == "RM"), flatten(flatten(map(lambda x : map(lambda y: [y[0], y[2]] , x['p']),  tree["tree"])))))))+"\t"+str(tree["totalScore"])+"\n"

def simple_tree_string2(tree):
    return ("\t"+("|".join(unique(flatten(flatten(map(lambda x : map(lambda y: [y[0], y[2]] , x['p']),  tree["tree"])))))))+"\t"+str(tree["totalScore"])+"\n"


def simple_tree_print(tree):
    print("Score = "+str(tree["totalScore"])+" -- "+str(set(filter(lambda z : (z[:2] == "VM") or (z[:2] == "RM"), flatten(flatten(map(lambda x : map(lambda y: [y[0], y[2]] , x['p']),  tree["tree"])))))))

def simple_tree_print_all(tree):
    print("Score = "+str(tree["totalScore"])+" -- "+str(set(flatten(flatten(map(lambda x : map(lambda y: [y[0], y[2]] , x['p']),  tree["tree"]))))))

def return_indexed_graph(lambda_cluster_keys):
    adjacency_list = {}
    degree_map = {k: 0 for k in range(len(lambda_cluster_keys))}
    for i in range(len(lambda_cluster_keys)):
        vertex_i = set(flatten(map(lambda x: (x[0][0], x[0][2]), lambda_cluster_keys[i])))
        for j in range(i):
            common_nodes = set.intersection(vertex_i, flatten(map(lambda x: (x[0][0], x[0][1]), lambda_cluster_keys[j])))
            if (len(common_nodes) > 0):
                adjacency_list[(i, j)] = common_nodes
                degree_map[i] = degree_map[i]+1
                adjacency_list[(j, i)] = common_nodes
                degree_map[j] = degree_map[j]+1
    return (lambda_cluster_keys, adjacency_list, degree_map)

def cluster_iterate(cluster_map, q_id):
        for score, list in cluster_map[q_id].items():
            for data_path_and_align in list:
                yield (score, data_path_and_align[0], data_path_and_align[1], data_path_and_align[2])

emptyset = {}
def getOrDefault_forMapOfSets(map, key):
    if key in map:
        return set(map[key])
    else:
        return emptyset

def psi_with_lambda(q1_q2_commons, p1_alignmentElement, p2_align):
    return (len(list(filter(lambda x: len(set.intersection(getOrDefault_forMapOfSets(p1_alignmentElement['nodeAlignmentMap'], x),
                                                                getOrDefault_forMapOfSets(p2_align, x))) == 0 ,
                                 q1_q2_commons))))

def test_branch_and_bound(q1_q2_commons, p1_alignmentElement, s_dp_a, branch_and_bound_score):
    return psi_with_lambda(q1_q2_commons, p1_alignmentElement, s_dp_a[2]) + branch_and_bound_score + s_dp_a[0]

def BFSIteration(p1_alignmentElement, cluster, Tree, IG_Eq, Visited, edit_threshold):
    test = False
    #if 'VM995305.000032' in p1_alignmentElement['nodeAlignmentMap']['?transactionEvent'] and p1_alignmentElement['lambd'] == 0:
    #    test = True
    #    print("DEBUG1")

    ## Prior to unpacking the data, we have to test if the current node has been visited yet.
    ## If already visited, the current path is not going to be traversed again.
    if (p1_alignmentElement['q'] in Visited):
        return [Tree]

    ## A tree is just an unique element that is going to be extended as a score
    branch_and_bound_score = Tree['totalScore']
    ListFromTree = Tree['tree']
    dictionary = Tree['dictionary']

    ## This step contains the extended tree with the current required steps
    ## TODO: q1 = p1_alignmentElement['q']
    ## TODO: BFSSteps = {q2: [] for (q1,q2) in filter(lambda cp: cp[0] == p1_alignmentElement['q'] and not (cp[1] in Visited), IG_Eq.keys())}
    BFSSteps = []
    for (q1,q2) in IG_Eq.keys():
        if (q1 == p1_alignmentElement['q']) and not (q2 in Visited):
            q1_q2_commons = IG_Eq[(q1,q2)]
            ## TODO: you should use the lambda information from the Tree
            for s_dp_a in cluster_iterate(cluster, q2):
                if test and 'VM995305.000032' in s_dp_a[2]['?transactionEvent']:
                    print("DEBUG2"+" WITH "+str(s_dp_a[0] == 0))
                updatedScore = test_branch_and_bound(q1_q2_commons, p1_alignmentElement, s_dp_a, branch_and_bound_score)
                if updatedScore <= edit_threshold:
                    #if (updatedScore == 0):
                    #    print("DEBUG")
                    cp = list(ListFromTree)
                    cp.append(alignmentAtomFromSPDA(s_dp_a, q2))
                    current = dict(dictionary)
                    current.update(s_dp_a[3])
                    ## TODO: BFSSteps[q2].append(treeAtom(cp, updatedScore, current))  # TODO
                    BFSSteps.append(treeAtom(cp, updatedScore, current))

    ## TODO: tutte le possibili combinazioni delle derivazioni da q1, prendendo tutti i rami in modo bfs
    V = set(Visited)
    V.add(p1_alignmentElement['q'])

    toReturn = []
    for ttt in BFSSteps:
        p2_alignmentElement = ttt['tree'][-1]
        toReturn.extend(BFSIteration(p2_alignmentElement, cluster, ttt, IG_Eq, V, edit_threshold))
    if (len(toReturn) == 0):
        return [Tree]
    return toReturn

## Getting the first node with the highest degree
def pickNode(Vq, degree):
    return sorted(range(len(Vq)), key=lambda x:degree[x])[-1]

def sama_with_path_decomposed_query(lambda_cluster_keys, k_hops, mappa):
    ## The cluster maps each path-id to its cluster.
    ## The cluster is defined as a list of pairs, where the first element is the score of the paths, and the second one
    ## are the paths having the same edit score. Each path is composed by a pair, which is the actual data path and the
    ## lambda alignment
    cluster = {i : sama_lambda.getLambda(lambda_cluster_keys[i], k_hops, mappa) for i in range(len(lambda_cluster_keys))}
    # import json
    # from extended_sama import SetEncoder
    # debug_cluster_file = open("Debug.json", "w", encoding="utf8")
    # debug_cluster_file.write(json.dumps(cluster,indent=4, cls=SetEncoder, ensure_ascii=False))
    # debug_cluster_file.close()
    # exit(1)
    Forest = []
    ## DEBUGGING:
    # for k, v in cluster.items():
    #     print("Clusters for path = "+str(lambda_cluster_keys[k]))
    #     for kp, vp in v.items():
    #         print("\tWith Score="+str(kp))
    #         for element in vp:
    #             print("\t\t"+str(element))
    ##############
    if len(lambda_cluster_keys) > 1:
        ## The indexed graph is a pair (Vq, Eq), in addition to a degree map
        (Vq, Eq, degree) = return_indexed_graph(lambda_cluster_keys)
        q_id = pickNode(Vq, degree)
        Visited = set()
        for p1_lambda_score, p1_data_path, p1_align, dictionary in cluster_iterate(cluster, q_id):
            Tree = treeSingleton(q_id, p1_lambda_score, p1_data_path, p1_align, dictionary)
            Forest.extend(BFSIteration(alignmentAtom(q_id, p1_lambda_score, p1_data_path, p1_align), cluster, Tree, Eq,
                                       Visited, k_hops * 6 * len(lambda_cluster_keys)))

        return Forest
    else:
        for p1_lambda_score, p1_data_path, p1_align, dictionary in cluster_iterate(cluster, 0):
            Forest.append(treeSingleton(0, p1_lambda_score, p1_data_path, p1_align, dictionary))

    return Forest