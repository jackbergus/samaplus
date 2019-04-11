# -*- coding: utf-8 -*-
#
# index_creator_html.py
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
import json
import pandas
from pandasql import sqldf
import io
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

mainPath = "/home/giacomo/Scrivania/evaluation/sama2/"
basePathEndingSlash = "/home/giacomo/Scrivania/evaluation/sama2/htmls/"
classificationPath = mainPath + "classification_full.csv"
classification = pandas.read_csv(classificationPath)


stats = (pysqldf("""
SELECT query_id, subgraph_id, matched_hypothesis, CAST(lambda_score AS INT) as lambda_score,
       SUM(CASE WHEN annotated_as = 'fully-relevant' THEN 1 else 0 END) AS fully_relevant,
       SUM(CASE WHEN annotated_as = 'partially-relevant' THEN 1 else 0 END) AS partially_relevant,
       SUM(CASE WHEN annotated_as = 'NA' THEN 1 else 0 END) AS not_annotated,
       SUM(CASE WHEN annotated_as = 'contradicts' THEN 1 else 0 END) AS contradicts,
       SUM(CASE WHEN annotated_as = 'fully-relevant' THEN 1 else 0 END)*1000+SUM(CASE WHEN annotated_as = 'partially-relevant' THEN 1 else 0 END)-SUM(CASE WHEN annotated_as = 'contradicts' THEN 1 else 0 END)*1000 AS score,
       ("<a href='Hypothesis_"||CAST(lambda_score AS INT)||"_"||query_id||"_"||subgraph_id||"_"||matched_hypothesis||"_.html'>view</a>") as file_name
FROM classification
GROUP BY query_id, subgraph_id, matched_hypothesis, lambda_score
ORDER BY fully_relevant DESC, partially_relevant DESC, contradicts ASC 
"""))

query1_result = pysqldf("""
select matched_hypothesis, lambda_score, score, group_concat(file_name, ';') as files
from stats
where file_name is not null 
group by matched_hypothesis, lambda_score, score
ORDER BY matched_hypothesis ASC, lambda_score ASC, score DESC
""")

fileq1 = io.open("order_by_matched_hypothesis_and_edit_distance.html", "w", encoding='utf8')
fileq1.write('<html>\n<body>\n<table style="width:100%">'.decode('utf-8'))
fileq1.write(("<tr>" + ("<th>matched_hypothesis</th>") + ("<th>edit_distance</th>") + ("<th>hyp_annot_score</th>")  +("<th>plot</th>") + "</tr>\n").decode('utf-8'))
for index, row in query1_result.iterrows():
    for file in row['files'].split(';'):
        fileq1.write(("<tr>"+("<td>"+row['matched_hypothesis']+"</td>")+("<td>"+str(row['lambda_score'])+"</td>")+("<td>"+str(row['score'])+"</td>")+("<td>"+file+"</td>")+"</tr>\n").decode('utf-8'))
fileq1.write('</table>\n</body>\n</html>'.decode('utf-8'))
fileq1.close()

query2_result = pysqldf("""
select query_id, score,  lambda_score, matched_hypothesis, group_concat(file_name, ';') as files
from stats
where file_name is not null 
group by query_id, matched_hypothesis, lambda_score, score
ORDER BY query_id DESC, score DESC, lambda_score ASC, matched_hypothesis ASC
""")

fileq2 = io.open("order_by_query_id_and_annotated_score.html", "w", encoding='utf8')
fileq2.write('<html>\n<body>\n<table style="width:100%">\n'.decode('utf-8'))
fileq2.write(("<tr>" + ("<th>query_id</th>") + ("<th>hyp_annot_score</th>") + ("<th>edit_distance</th>") + ("<th>matched_hypothesis</th>") +("<th>plot</th>") + "</tr>\n").decode('utf-8'))
for index, row in query2_result.iterrows():
    for file in row['files'].split(';'):
        fileq2.write(("<tr>"+("<td>"+(row['query_id'])+"</td>")+("<td>"+str(row['score'])+"</td>")+("<td>"+str(row['lambda_score'])+"</td>")+("<td>"+(row['matched_hypothesis'])+"</td>")+("<td>"+file+"</td>")+"</tr>\n").decode('utf-8'))
fileq2.write('</table>\n</body>\n</html>'.decode('utf-8'))
fileq2.close()