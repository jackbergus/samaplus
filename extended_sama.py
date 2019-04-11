#
# extended_sama.py
# This file is part of SAMA+
#
# Copyright (C) 2019 - Giacomo Bergami
#
# SAMA+ is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License.
#
# SAMA+ is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with SAMA+. If not, see <http://www.gnu.org/licenses/>.
#
import replace_entrypoints_in_query_for_sama as rep
import query_decomposition as qd
import sama
import data
import json
import io
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
main_path = "/home/giacomo/Scrivania/evaluation/sama2/"
## Getting all the possible queries to be processed
keys = ['F001_Q002', 'F002_Q002Q004Q005']

for key in keys:
    #key = 'F002_Q002Q004Q005'
    print(key)
    ls = rep.replace_entrypoints_in_query_for_sama(data.example[key])
    sama_query = ls[0]
    variable_to_entrypoint_map = ls[1]
    #for sama_query, variable_to_entrypoint_map in :
    lls = sama_with_disconnected_queries(sama_query, variable_to_entrypoint_map, data.example[key]['khop'])
    file = io.open(main_path+key+".json", "w", encoding='utf8')
    file.write(json.dumps(dict(zip(range(len(lls)), lls)),indent=4, cls=SetEncoder, ensure_ascii=False).decode('utf-8'))
    file.close()
    file = io.open(main_path + key + ".tsv", "w", encoding='utf8')
    for tree in lls:
        file.write(sama.simple_tree_string(tree).decode('utf-8'))
    file.close()
    #exit(10)
    #L.extend(lls)


