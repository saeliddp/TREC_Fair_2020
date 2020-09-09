# BM25 ranking utility for both reranking and
# retrieval tasks
# BM25 scores are our 'relevance' estimates

import json
import boto3
import nltk
import rank_bm25
import langdetect
import gensim
import googletrans
from googletrans import Translator
from gensim.summarization.bm25 import BM25
from boto3 import resource
from boto3.dynamodb.conditions import Key
from query import get_results_from_doc_list
from preprocess import tokenize
from langdetect import detect 

dynamodb = boto3.resource('dynamodb', aws_access_key_id='', aws_secret_access_key='', region_name='us-west-2')

table = dynamodb.Table('SamplePapers')

translator = Translator()

# Runs bm25 for the reranking task--only looks at documents provided
# by the organizers
def bm25(filename):
    query = []
    with open(filename, 'r', encoding='utf-8') as f:
        for jsonOjb in f:
            data = json.loads(jsonOjb)
            query.append(data)
            
    q_results = []
    for q in query:
        qid = q['qid']
        queryName = q['query']
        queryName = queryName.encode('utf-8')
        queryName = queryName.decode('utf-8')
        queryName = queryName.replace('\\\"', '')
        # queryName = queryName.replace('-', ' ')
        
        ranked = []
        tokens = []
        id_rel_paper = []
        
        translation = translator.translate(queryName)
        queryName = translation.text.lower()
        
        # print(qid)
        # print(queryName)
        
        qRelavance = {}
        for doc in q['documents']:
            qRelavance[doc['doc_id']] = doc['relevance']
        
        docList = get_results_from_doc_list('SamplePapers', q['documents'])
    
        tokenizedQuery = tokenize(queryName)
            
        for doc in docList:
            doc_token = []
            if isinstance(doc.selectedTokens, dict):
                for s in doc.selectedTokens['L']:
                    doc_token.append(s['S'])
            else:
                doc_token = doc.selectedTokens
            tokens.append(doc_token)
            id_rel_paper.append({'doc_id': doc.id, 'relevance': qRelavance[doc.id], 'paper': doc})
       
        if len(tokens) >= 1:
            bm25 = BM25(tokens)
            scores = bm25.get_scores(tokenizedQuery) # returns a list of document scores
    
        i = 0
        for item in id_rel_paper:
            item['bm25_score'] = scores[i]
            i += 1
            
        ranked = sorted(id_rel_paper, key = lambda i: i['bm25_score'], reverse=True) 
            
        result = {}
        #result['q_num'] = '0.' + str(q_num)  # 0 will be replaced with corresponding sequence id
        result['qid'] = qid
        result['ranking'] = ranked
        q_results.append(result)
    
    # print(q_results)
    return q_results
    

# Runs bm25 for retrieval task--looks at entire ~8000 document corpus
# subset and retrieves top 100 results for a query
def bm25_retrieval(filename):
    query = []
    with open(filename, 'r', encoding='utf-8') as f:
        for jsonOjb in f:
            data = json.loads(jsonOjb)
            query.append(data)
    q_results = []
    
    all_doc = []

    response = table.scan(ProjectionExpression="id,authors,selectedTokens,gender_dist,country_dist,#p",ExpressionAttributeNames={'#p': 'partition'})
    data = response['Items']
    
    while 'LastEvaluatedKey' in response:
        response = table.scan(ProjectionExpression="id,authors,selectedTokens,gender_dist,country_dist,#p",ExpressionAttributeNames={'#p': 'partition'},ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    
    #print("Ckpt 0")
    for document in data:
        if document['partition'] > 1:
            pass
        else:
            doc_id = document['id']
            if doc_id not in all_doc:
                all_doc.append({'doc_id': doc_id})
                
    docList = get_results_from_doc_list('SamplePapers', all_doc)
    #print("Ckpt 1")
    
    for q in query:
        qid = q['qid']
        queryName = q['query']
        print(queryName)
        print(qid)
        queryName = queryName.encode('utf-8')
        queryName = queryName.decode('utf-8')
        queryName = queryName.replace('\\\"', '')
        queryName = queryName.replace('+', ' ')
        queryName = queryName.replace('*', '')
        queryName = queryName.replace('(', '')
        queryName = queryName.replace(')', '')
        queryName = queryName.replace('/', ' ')
        # queryName = queryName.replace('-', ' ')
        
        tokens = []
        id_paper = []

        tokenizedQuery = tokenize(queryName)
        
        for doc in docList:
            if doc.selectedTokens is None:
                print("No selected tokens doc_id " + doc.id)
                
            doc_token = []
            if isinstance(doc.selectedTokens, dict):
                for s in doc.selectedTokens['L']:
                    doc_token.append(s['S'])
            else:
                doc_token = doc.selectedTokens
            tokens.append(doc_token)
            id_paper.append({'doc_id': doc.id, 'gender_dist': doc.gender_dist, 'country_dist': doc.country_dist})
       
        if len(tokens) >= 1:
            bm25 = BM25(tokens)
            scores = bm25.get_scores(tokenizedQuery) # returns a list of document scores
    
        i = 0
        for item in id_paper:
            item['bm25_score'] = scores[i]
            i += 1
            
        ranked = sorted(id_paper, key = lambda i: i['bm25_score'], reverse=True) 
        ranked = ranked[:100]
            
        result = {}
        result['qid'] = qid
        result['ranking'] = ranked
        q_results.append(result)

    with open('data/bm25_retrieval.jsonl', 'w', encoding='utf-8') as outfile:
        for r in q_results:
            json.dump(r, outfile, ensure_ascii=False)
            outfile.write('\n')

if __name__ == "__main__":
    bm25_retrieval('data/TREC-Fair-Ranking-eval-sample-no-rel.json')