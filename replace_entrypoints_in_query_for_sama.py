from query_decomposition import rectify_entrypoints

def flatten(ls):
	return [x for y in ls for x in y]


def innermost(z, candidate_entrypoint_var, candidate_entry_point):
    return list(map(lambda y: candidate_entry_point if y == candidate_entrypoint_var else y,
               z))


def functionReplacement(q, toBeReplaced, candidate_entrypoint_var):
    return list(#map(
        #lambda candidate_entry_point:
            #list(
                map(lambda z: innermost(z, candidate_entrypoint_var, toBeReplaced), q)
            #),
        #toBeReplaced)
    )

def finalQueryWithMap(originalQuery, resolvedEntrypointQuery):
    map = {}
    for a,b in zip(resolvedEntrypointQuery, originalQuery):
        for c,d in zip(a,b):
            if not (c == d):
                map[c] = d
    return (resolvedEntrypointQuery, map)

def replace_entrypoints_in_query_for_sama(maps):
    rectify_entrypoints(maps)
    #epm = maps['pipeline_entrypoints']
    #ls = maps['edges']
    #flatten_edges = flatten(ls)
    #L = [ls]
    #for candidate_entrypoint_var in set.intersection(set(flatten_edges),set(epm.keys())):
    #        toBeReplaced = epm[candidate_entrypoint_var]
    #        L = flatten(map(lambda q: functionReplacement(q,toBeReplaced, candidate_entrypoint_var),
    #                            L)
    #                       )
    epm = maps['pipeline_entrypoints']
    ls = maps['edges']
    return tuple([ls, epm])
    #return list(map(lambda finalQuery : finalQueryWithMap(ls, finalQuery), L))

if __name__ == "__main__":
  ## Selecting the current query configuration
  from data import example
  for key in example.keys():
      print(replace_entrypoints_in_query_for_sama(example[key]))