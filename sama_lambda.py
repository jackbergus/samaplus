from collections import defaultdict
import operator
import psycopg2
from functools import reduce as fold
from replace_entrypoints_in_query_for_sama import flatten
from string import Template
import itertools

def accumulate(l, by, collectFrom, fun):
    it = itertools.groupby(l, operator.itemgetter(by))
    for key, subiter in it:
       yield key, list(fun(item[collectFrom] for item in subiter))

## WHERE  "argumentId" = ANY(VALUES                               ('E995305.00859'),                               ('E994847.00466'),                               ('E994813.00425'),                               ('E994892.00593'),                               ('E994853.00478'),                               ('E994797.00351'),                               ('E996075.00972'),                               ('E995292.00834'),                               ('E994750.00280'),                               ('E996075.00971'),                               ('E994868.00522'),                               ('E994951.00709')) -- id to be replaced with the entrypoint

"""
As per SAMA's specifications, we try to get all the paths terminating into a sink node. In this case we won't index 
the paths, and therefore we're getting all the elements terminating into the path. This query also takes into account
to kill the loops, and therefore we won't have repeated elements.
"""
sql_lambda_query = """
WITH RECURSIVE graph AS (
    SELECT mid AS leaf
          ,ARRAY[ROW("mid", "nistFullLabel", "argumentId", "argumentRawString")] AS path
          ,0 AS depth
    FROM   fact
    WHERE "argumentRawString"<>'' $where
    UNION ALL
    SELECT CASE WHEN o.mid = g.leaf THEN o."argumentId" ELSE o.mid END
          ,path || ROW(o.mid, o."nistFullLabel", o."argumentId", o."argumentRawString") 
          ,depth + 1
    FROM   graph g
    JOIN   fact o ON g.leaf in (o.mid, o."argumentId") 
    AND    NOT (ROW(o."mid", o."nistFullLabel", o."argumentId", o."argumentRawString") = ANY(path))
    AND    depth  <= $topk
    )
select I.path 
from (SELECT distinct path, depth
      FROM   graph
      ORDER BY depth ASC
     ) I
"""

def rec_noneReplace(ls, i, buildUp, acc):
    if (i == len(ls)):
        if (len(buildUp) > 0):
            acc.append(buildUp)
        return acc
    else:
        if ls[i] is None:
            if (len(buildUp) > 0):
                acc.append(buildUp)
            return rec_noneReplace(ls, i + 1, [], acc)
        else:
            buildUp.append(ls[i])
            return rec_noneReplace(ls, i + 1, buildUp, acc)


def noneReplace(ls):
    return rec_noneReplace(list(ls), 0, [], [])

def lambda_align(data_rectified, query_unrectified, query_rectified, entrypoint_map):
    """
    :param data_rectified:      data path to be aligned to the query
    :param query_unrectified:   query in its simplest triple formulation from the undirected representation
    :param entrypoint_map:      Elements that need to be present as perfect matches within the data
    :return:                    It returns a pair ((a,b), c), where
                                * a, is the data path that was be aligned
                                * b, is the set of the node alignments to the query
                                * c, is the score associated from both the vertex and edge edit distance
    """
    ## First step: check how many paths were aligned
    ## This if a first approximation of the alignments that will still provide us the edge edit distance
    edge_label_align = {}
    query_edge_match = 0
    edge_matches = 0
    while (query_edge_match < len(query_unrectified)):
        noMatch = True
        for i in range(len(data_rectified)):
            ## If I have no more alignments, then I'm done
            if (query_edge_match >= len(query_unrectified)):
                noMatch = False
                break
            ## If the labels do match (I'm going to refine the alignment in the next step)
            if (data_rectified[i][1] == query_rectified[query_edge_match][1]):
                # try:
                #     if (len(data_rectified) == 2) and (data_rectified[0][1] == 'Conflict.Attack_Place') and (data_rectified[1][1] == 'Conflict.Attack_Target'):
                #         print("loco")
                # except:
                #     pass
                noMatch = False
                edge_label_align[query_edge_match] = data_rectified[i]
                query_edge_match += 1
                edge_matches += 1
        if noMatch:
            query_edge_match += 1

    edge_plus_distance = len(data_rectified) - edge_matches
    edge_minus_distance = len(query_unrectified) - edge_matches

    ## Evaluate the node alignment using the distinct variables from the query
    node_align = {vertex: set() for vertex in set(flatten(map(lambda x: [x[0], x[2]], query_unrectified)))}
    for query_edge_id in edge_label_align:
        datum_rectified = edge_label_align[query_edge_id]
        variables = query_rectified[query_edge_id]
        node_align[variables[0]].add(datum_rectified[0])
        node_align[variables[2]].add(datum_rectified[2])

    ## Now, evaluating the vertex edit distance.
    ## 1) The vertex key in node_align is not aligned with the path if either its associated value list is empty or the
    ##    entrypoint was not matched with the entrypoint
    vertex_minus_distance = len(list(filter(lambda x: len(x[1]) == 0 or (x[0] in entrypoint_map and len(set.intersection(x[1], entrypoint_map[x[0]])) == 0), node_align.items())))

    ## 2) The added vertices are the ones that are more than the parts that are already aligned.
    vertex_plus_distance = fold(operator.add,
                                map(lambda x: len(x[1])-1, filter(lambda x: len(x[1]) > 1, node_align.items())), 0)
    ##    In addition to that, I must add the vertices that have not been matched with the element from the query
    for subpath in noneReplace(map(lambda x : None if x in edge_label_align.values() else x, data_rectified)):
        vertex_plus_distance += len(set(flatten(subpath)[1:-1]))
    ##    In addition to that, I must consider the nodes not aligned with the entrypoint
    #for ep in entrypoint_map:
    #    if (ep in node_align.keys()):
    #        s = set.difference(set(node_align[ep]), {ep})
    #        if (len(s) > 0):
    #            vertex_plus_distance += len(s)
    #        elif ((len(s) == 0) and (ep in entrypoint_map) and (ep not in entrypoint_map[ep])):
    #            vertex_minus_distance += 1

    finalScore = edge_plus_distance + edge_minus_distance + vertex_minus_distance + vertex_plus_distance
    #if (finalScore == 0):
    #    #print ("SCORE="+str()+" --\t"+str(list(map(lambda x: x[1], data_rectified)))+" E+ = "+str(edge_plus_distance)+ " E- = "+str(edge_minus_distance)+ " V+ = "+str(vertex_plus_distance)+" V- ="+str(vertex_minus_distance))
    #if (finalScore == 1):
    #    print("Score of "+str(finalScore)+" for path "+str(data_rectified)+" for cluster "+str(query_rectified))
    return (data_rectified, node_align, finalScore)

def rowToPath(sqlStringRow):
    return list(map(lambda t: (t[0], t[1].split('.')[0] + '.' + t[1].split('.')[1] + '_' + t[1].split('.')[2], t[2], t[3]),
             map(lambda x: x.strip().lstrip('\"(').lstrip('(').rstrip(')\"').rstrip(')').split(','), sqlStringRow[0][1:-1].split('\",\"')))
         )

def runSQLQuery(query):
    conn = None
    data_path = []
    try:
        conn = psycopg2.connect(dbname="p103")
        cur = conn.cursor()
        cur.execute(query)
        row = cur.fetchone()
        while row is not None:
            t = rowToPath(row)
            data_path.append(dict(data_path = list(map(lambda y : (y[0], y[1], y[2]), t)), dictionary = dict(map(lambda y : (y[2], y[3]), t))))
            row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return data_path

def getLambda(lambda_path, k_hops, mappa):
    """
    :param lambda_path:         Single path (as a list of triples of path-bool-rect_path)
    :param k_hops:              Number of the maximum tollerated expansion from the query graph
    :param top_k:               Number of the top clusters to be returned. If 0, then it is set to 1, because we
                                need to return at least one element
    :param mappa:               Query variable to associated entry point association
    :return:                    It returns a cluster associated to the given lambda_path, thus a list of pairs,
                                where the first element is the score and the second one is the set of all the elemnets
                                in the data-paths having the same score.
    """

    ## Maximum additional length that I need to expand
    initialEdgeWithTriplet = lambda_path[0]
    print("getLambda")
    print(lambda_path)
    where_column = ""
    sinkVariable = initialEdgeWithTriplet[0][0]
    if initialEdgeWithTriplet[0][0] in mappa.keys():
        if initialEdgeWithTriplet[1]:
            where_column = "AND \"mid\" = "+"ANY(VALUES "+(", ".join(map(lambda x : "('"+x+"')", mappa[sinkVariable])))+")"
        else:
            where_column = "AND \"argumentId\" = "+"ANY(VALUES "+(", ".join(map(lambda x : "('"+x+"')", mappa[sinkVariable])))+")"
    s = Template(sql_lambda_query)
    query_unrectified = list(map(lambda x: x[0], lambda_path))
    query_rectified = list(map(lambda x: x[2], lambda_path))
    entrypoint_set = set(mappa.keys())
    cluster = defaultdict(list)

    if (k_hops == 0):
        k_hops = 1
    max_editDistance =  k_hops * 6
    max_path_traversal_from_khops = len(query_rectified) + k_hops

    #print(s.substitute(where = where_column))
    for data_path_with_dictionary in runSQLQuery(s.substitute(where = where_column, topk = max_path_traversal_from_khops)):
        data_path = data_path_with_dictionary["data_path"]
        dictionary = data_path_with_dictionary["dictionary"]
        (data_rectified, node_align, finalScore) = lambda_align(data_path, query_unrectified[::-1], query_rectified[::-1], mappa)
        cluster[finalScore].append([data_rectified, node_align, dictionary])
    return {k: cluster[k] for k in sorted(cluster.keys())[0:max_editDistance]}
