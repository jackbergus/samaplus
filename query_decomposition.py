import networkx as nx
from collections import defaultdict
import sys
sys.setrecursionlimit(10000)



def unique(seq):
  seen = set()
  seen_add = seen.add
  return [x for x in seq if not (x in seen or seen_add(x))]

## Removes form the map the elements having zero entrypoints
def rectify_entrypoints(map):
    toRemove = []
    for key in map["pipeline_entrypoints"].keys():
        if (len(map["pipeline_entrypoints"][key]) == 0):
            toRemove.append(key)
    for key in toRemove:
        map["pipeline_entrypoints"].pop(key)

## Instantiates the query graph in networkx
def triples_to_graph(ls):
    G = nx.MultiDiGraph()
    i = 0
    for triplet in ls:
        ## memo: assigning a weight to return the maxflow
        G.add_edge(triplet[0], triplet[2], label=triplet[1], capacity=1)
        i = i+1
    return G

## Implements a custom dfs visit
def dfs(g, G, start):
    visited, stack = set(), [start]
    dfsCode = []
    N = len(g.nodes)
    while (len(visited) < N):
      if (start in visited):
        stack = list()
        root = list(filter(lambda y : y[0] not in visited,sorted(G.out_degree, key=lambda x: x[1], reverse=True)))
        if (len(root) == 0):
          break
        stack.append(root[0][0])
        start = root[0][0]
      while stack:
          vertex = stack.pop()
          if vertex not in visited:
              visited.add(vertex)
              #visiting the graph
              vertex_out = [(x[0], x[3]["label"], x[1]) for x in list(G.out_edges(nbunch=[vertex], keys=True, data=True))]
              vertex_out.sort(key=lambda x: x[0]+"§"+x[1],reverse=True)
              ls = set()
              for x in vertex_out:
                  dfsCode.append(x)
                  ls.add(x[2])
              set.difference_update(visited)
              stack.extend(ls)
    return unique(dfsCode)

## Associates the highest score to the path having the most candidates as sink nodes times the length of the path, and nodes with the highest degree.
def score_path(ls, map, sinks):
    sumDegree = 0
    maxDegree = 0
    for node in ls:
      sumDegree += map[node]
      maxDegree = max(map[node], maxDegree)
    #print("["+",".join(ls)+"] ==> "+str(len(set.intersection(set(ls), set(sinks)))*(len(ls))+sumDegree*maxDegree))
    return len(set.intersection(set(ls), set(sinks)))*(len(ls))+sumDegree*maxDegree

## Returns the dfs code associated to the directed graph
def dfsCode(G,uG):
  dfsCode = list()
  for c in nx.connected_components(uG):
        g = uG.subgraph(c)
        root = list(filter(lambda x : x[0] in c, sorted(G.out_degree, key=lambda x: x[1], reverse=True)))[0][0]
        dfsCode.extend(dfs(g, G, root))
  unique(dfsCode)
  return dfsCode

## Given that the NetworkX function all_simple_paths is only returning the nodes labels and not the edges, I need
## to reconstruct the edge label information.
def reconstruct_path_from_vertex_list(G, c):
  vls = list(c)
  if (len(vls) > 0):
    prev = vls.pop()
    toReturn = []
    while (len(vls) > 0):
      curr = vls.pop()
      toReturn.append([prev, G.adj[prev][curr][0]['label'], curr])
      prev = curr
    return {"path": toReturn, "nodes":c}
  else:
    return {"path": vls, "nodes":c}

## Given a graph G, a target sink node and
## If no sources are give, we traverse G from all the nodes.
def allPathsForTarget(G, target, sinks, sources=None):
  ls = list()
  if sources is None:
    sources = G.nodes
  for src in sources:
    try:
      for path in nx.all_simple_paths(G, source=src, target=target):
        ls.append(path)
    except:
      pass
  ls.sort(key=lambda x: score_path(x, G.degree, sinks), reverse=True)
  if (len(ls) == 0):
    return {"path": ls, "nodes":ls}
  return reconstruct_path_from_vertex_list(G, ls[0])

def notInverse(triplet, ls):
  tt = tuple(triplet)
  if tuple(triplet) in ls:
    return (tt,True,tt)
  elif tuple([triplet[2], triplet[1], triplet[0]]) in ls:
    return (tt,False, tuple([triplet[2], triplet[1], triplet[0]]))
  return tuple(tt)

def rectified_allPathsForTarget(G, dfs_code, target, sinks, sources=None):
  return [notInverse(triplet, dfs_code) for triplet in allPathsForTarget(G, target, sinks, sources)['path']]

def getDegree(x,G,nodes):
  if x in set.intersection(set(G.nodes),nodes):
    return G.in_degree[x]
  else:
    return 0

## Actual recappend function, defined recursively
def recappendConcat(i, ls, curr, app):
    if (len(ls) == i):
      app.append(curr)
      return app
    else:
      if (ls[i][0] == ls[i - 1][2]):
        curr.append(i)
        return recappendConcat(i + 1, ls, curr, app)
      else:
        app.append(curr)
        return recappendConcat(i + 1, ls, [i], app)


## This function is required because we might break the paths. Therefore, I'm splitting into distinct paths if I broke some connectivity
def recappend(ls):
  if (len(ls) == 0):
    return ls
  if (len(ls) == 1):
    return [[0]]
  else:
    return recappendConcat(1, ls, [0], [])

## Resumes all the remaining paths that have been extracted
def exausively_get_candidate_paths(candidatePaths, extractedEdges, result):
  if (len(candidatePaths) == 0):
    return result
  candidatePaths.sort(key=lambda x : len(x), reverse=True)
  toResume = list()
  for path1 in candidatePaths:
    candidate = list(filter(lambda x: x[2] not in extractedEdges, path1))
    if (len(path1) == len(candidate)):
      for x in candidate:
        extractedEdges.add(x[2])
      result.append(candidate)
    elif (len(candidate) > 0):
      for x in recappend([x[0] for x in candidate]):
        toResume.append([candidate[y] for y in x])
  return exausively_get_candidate_paths(toResume, extractedEdges, result)


## Given a set of sinks, the graph G, the set of the nodes already to visit, a dsfCode providing the right RDF direction, ...
## the function provides the possible lambda paths in the results variable
def generatePathsFromSinks(sinks, G, notVisitedNodes, dfs_code, undirectedG, possibleTargets, extractedEdges, results):
  for target in sinks:
    if (target in notVisitedNodes):
      path1 = rectified_allPathsForTarget(undirectedG, dfs_code, target, sinks, notVisitedNodes)
      if isinstance(path1, list):
        results.append(path1)
        for x in path1:
          extractedEdges.add(x[2])
        traversed = set([item for sublist in path1 for item in sublist[0]])
        notVisitedNodes = notVisitedNodes.difference(traversed)
        if target in traversed:
          traversed.remove(target)
        for node in traversed:
            if node in set.intersection(set(G.nodes),set(undirectedG.nodes)):
              possibleTargets.add(node)
  possibleTargets = unique(list(possibleTargets))
  possibleTargets.sort(key=lambda x: getDegree(x, G, set(undirectedG.nodes)), reverse=True)
  for x in possibleTargets:
      if x in notVisitedNodes:
          notVisitedNodes.remove(x)
  #print(possibleTargets)

  ## Then, I try to get the remaining paths from the remaining nodes from which i might start the visit
  #print(nodes)
  #print("additions")
  candidatePaths = list()
  connectedComponentFullyVisited = False
  while (len(notVisitedNodes)> 0):
    possibleTargets = unique(possibleTargets)
    targetMap = {}
    targetEdgeSet = defaultdict(set)
    ## Then, I iterate over all the possible targets
    for target in possibleTargets:
      targetMap[target] = rectified_allPathsForTarget(undirectedG, dfs_code, target, set(possibleTargets), notVisitedNodes)
      targetEdgeSet[target] = set([x[2] for x in targetMap[target]])

    setToRemove = set()
    for target in possibleTargets:
      for target_p in possibleTargets:
        found = False
        if not (target == target_p):
          if targetEdgeSet[target].issubset(targetEdgeSet[target_p]) and not (targetEdgeSet[target] == targetEdgeSet[target_p]):
            setToRemove.add(target)
            break

    for toRemove in setToRemove:
      targetMap.pop(toRemove)
      targetEdgeSet.pop(toRemove)

    ## The remaining candidate paths over the subgraph for the entrypoints
    otherPaths = list(targetMap.values())
    ## If all the extracted associated paths are empty, this means that I need to visit the other connected components
    if len(list(filter(lambda x : len(x) == 0, otherPaths))) == len(otherPaths):
      connectedComponentFullyVisited = True
      break
    for path1 in otherPaths:
      candidate = list(filter(lambda x: x[2] not in extractedEdges, path1))
      for x in recappend([x[0] for x in candidate]):
        candidatePaths.append([candidate[y] for y in x])
      #print([x[0] for x in candidate])
      traversed = set([item for sublist in path1 for item in sublist[0]])
      notVisitedNodes = notVisitedNodes.difference(traversed)
      for node in traversed:
        if node in set.intersection(set(G.nodes),set(undirectedG.nodes)):
          possibleTargets.append(node)
    #print(notVisitedNodes)
    #print(set(possibleTargets))

  for candidate in exausively_get_candidate_paths(candidatePaths, extractedEdges, []):
    results.append(candidate)
  #print(results)
  if (connectedComponentFullyVisited):
    #print("TODO: visit the remaining part")
    ## I choose the sink with the minimum connectivity
    newSinks = list(notVisitedNodes)
    newSinks.sort(key=lambda x: undirectedG.degree[x])
    #print(newSinks)
    generatePathsFromSinks(newSinks, G, notVisitedNodes, dfs_code, undirectedG, set(possibleTargets), extractedEdges, results)

## This function returns the possible candidate paths for each connected component
def generatePathsFromConnectedComponent(connectedComponent, mapSelectedQuery_keys, G, notVisitedNodes, undirectedG, possibleTargets, extractedEdges, dfs_code):
  ccResult = []
  generatePathsFromSinks(mapSelectedQuery_keys, G, set.intersection(notVisitedNodes, connectedComponent), dfs_code, undirectedG.subgraph(connectedComponent), possibleTargets, extractedEdges, ccResult)
  return ccResult

## Easy prinring for the candidate path generation
def print_lambda_path(candidate):
  print([x[0] for x in candidate])

class QueryGraph():

    def __init__(self, mapSelectedQuery_keys, query):
      G = triples_to_graph(query)
      notVisitedNodes = set(G.nodes)
      self.undirectedG = G.to_undirected()
      self.dfs_code = dfsCode(G, self.undirectedG)
      possibleTargets = set()
      extractedEdges = set()
      connected_components_asListOfNodeSets = list(nx.connected_components(self.undirectedG))
      connected_components_asListOfNodeSets.sort(key=lambda x: len(set.intersection(x, mapSelectedQuery_keys)), reverse=True)
      self.pathsSeparatedByConnectedComponent = [
        generatePathsFromConnectedComponent(x, mapSelectedQuery_keys, G, notVisitedNodes, self.undirectedG, possibleTargets,
                                            extractedEdges, self.dfs_code) for x in
        connected_components_asListOfNodeSets]

    @classmethod
    def fromExamples(self, mapSelectedQuery):
      self.mapSelectedQuery = mapSelectedQuery
      ## Keeping only the non-empty entrypoints from the pipeline
      rectify_entrypoints(self.mapSelectedQuery)
      return self(mapSelectedQuery["pipeline_entrypoints"].keys(), self.mapSelectedQuery["edges"])
      #
      # ## Create a NetworkX graph from the query
      # G = triples_to_graph(self.mapSelectedQuery["edges"])
      # ## Keeping track whether there are remaining nodes to be visited
      # notVisitedNodes = set(G.nodes)
      # ## Getting the graph over which we're extracting the paths.
      # self.undirectedG = G.to_undirected()
      # ## Generating a DFS code associated to the directed graph. This is required because we can traverse the graph
      # ## backwards as it was undirected but then, when I need to join the tables, I need to remember in which direction
      # ## I'm looking at
      # self.dfs_code = dfsCode(G, self.undirectedG)
      #
      # ## Initializing the lambda paths
      # queryPaths = []
      # possibleTargets = set()
      # extractedEdges = set()
      # connected_components_asListOfNodeSets = list(nx.connected_components(self.undirectedG))
      # mapSelectedQuery_keys = mapSelectedQuery["pipeline_entrypoints"].keys()
      # connected_components_asListOfNodeSets.sort(key=lambda x: len(set.intersection(x, mapSelectedQuery_keys)), reverse=True)
      #
      # self.pathsSeparatedByConnectedComponent = [
      #   generatePathsFromConnectedComponent(x, mapSelectedQuery_keys, G, notVisitedNodes, self.undirectedG, possibleTargets,
      #                                       extractedEdges, self.dfs_code) for x in connected_components_asListOfNodeSets]

    ## Prints all the paths separated by connected component
    def prettyPrintPaths(self):
      i = 1
      for connected_component in self.pathsSeparatedByConnectedComponent:
        print("Connected component #" + str(i))
        for x in connected_component:
          print_lambda_path(x)
        i += 1

    ## Print all the paths belonging to a connected component
    def prettyPrintConnectedComponentPahts(self, connectedComponentId):
      for x in self.getLambdaClusterKeysByConnectedComponentId(connectedComponentId):
        print_lambda_path(x)

    ## If the query is split up in many multiple connected components, then I target that I cannot resolve all the
    ## elements together, but that I must produce the lambda results for each connected component at a time
    def getNumberOfConnectedComponents(self):
      return len(self.pathsSeparatedByConnectedComponent)

    ## Returns all the lambdas associated to each connected component
    def getLambdaClusterKeysByConnectedComponentId(self, connectedComponentId):
      return list(filter(lambda x: len(x) > 0, self.pathsSeparatedByConnectedComponent[connectedComponentId]))

if __name__ == "__main__":
  ## Selecting the current query configuration
  from data import example
  for key in example.keys():
    print(key)
    print("".join(list(map(lambda x : "=", range(len(key))))))
    print("".join(list(map(lambda x : "=", range(len(key))))))
    mapSelectedQuery = example[key]
    qg = QueryGraph.fromExamples(mapSelectedQuery)
    for i in range(qg.getNumberOfConnectedComponents()):
      print("Connected component #" + str(i))
      qg.prettyPrintConnectedComponentPahts(i)
    print("".join(list(map(lambda x : "=", range(len(key))))))
    print("".join(list(map(lambda x : "=", range(len(key))))))