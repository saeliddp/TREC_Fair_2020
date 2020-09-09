# Utility to get all results from a Query item from the provided JSON
# Uses a combination of client.batch_get_item and table.get_item (for the failed parts of the previous operation)
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.batch_get_item
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.get_item
import boto3
from paper import *
from boto3.dynamodb.conditions import Key

client = boto3.client('dynamodb', aws_access_key_id='', aws_secret_access_key='',region_name='us-west-2')
dynamodb = boto3.resource('dynamodb', aws_access_key_id='', aws_secret_access_key='',region_name='us-west-2')

# returns a list of Paper objects based on documents list in the format
# [{'doc_id': example, ...}, ...]
def get_results_from_doc_list(tablename, documents):
    index = 0
    output_papers = []
    while index < len(documents):
        output_papers.extend(guaranteed_size_get_results(tablename, documents[index: min(len(documents), index + 100)]))
        index += 100
    return output_papers

def guaranteed_size_get_results(tablename, documents):
    table = dynamodb.Table(tablename)
    keys = []
    wanted_docs = set()
    for item in documents:
        keys.append({'id': {'S': item['doc_id']}, 'partition': {'N': "0"}})
        wanted_docs.add(item['doc_id'])
        
    papers = {
        'Keys': keys,
        'ConsistentRead': False,
    }
    
    response = client.batch_get_item(
        RequestItems={
            tablename: papers
        },
        ReturnConsumedCapacity='TOTAL'
    )
    data = response['Responses'][tablename]
    
    while len(response['UnprocessedKeys']) > 0:
        unprocessed_keys = response['UnprocessedKeys'][tablename]
        response = client.batch_get_item(
            RequestItems={
                tablename: unprocessed_keys
            },
            ReturnConsumedCapacity='TOTAL'
        )
        data.extend(response['Responses'][tablename])
    
    output_papers = []
    returned_docs = set()
    for r in data:
        new_paper = Paper(r)
        output_papers.append(new_paper)
        returned_docs.add(new_paper.id)
        
    partitioned_docs = wanted_docs.difference(returned_docs)
    for doc_id in partitioned_docs:
        #print(doc_id)
        q_response = {}
        curr_partition = 1
        partial_paper = None
        while 'Item' in q_response or curr_partition == 1:
            q_response = table.get_item(
                Key={
                    'id': doc_id,
                    'partition': curr_partition
                }
            )
            try:
                if curr_partition == 1:
                    partial_paper = Paper(q_response['Item'])
                elif 'Item' in q_response:
                    if 'inCitations' in q_response['Item']:
                        partial_paper.addInCitations(q_response['Item']['inCitations'])
                    elif 'outCitations' in q_response['Item']:
                        partial_paper.addOutCitations(q_response['Item']['outCitations'])
                curr_partition += 1
            except KeyError:
                # print(doc_id + ' is missing.')
                break
                
        if partial_paper != None:
            output_papers.append(partial_paper)
            
    return output_papers

# returns the gender and country for an author with id author_id
def author_gender_country(author_id):
    table = dynamodb.Table('SampleAuthors')
    g_c = ['none', 'none']
    response = table.get_item(
        Key={
             'corpus_author_id': author_id
        }
    )
    
    if 'Item' in response:
        item = response['Item']
        if 'predicted_gender' in item:
            if item['predicted_gender'] is not None:
                g_c[0] = item['predicted_gender']
        if 'predicted_country' in item:
            if item['predicted_country'] is not None:
                g_c[1] = item['predicted_country']
    
    return g_c

if __name__ == "__main__":
    """
    papers = get_results_from_doc_list('SamplePapers', [{"doc_id": "128240ef2451949d4de75f1f3d33fd73f396ad82", "relevance": 0}, {"doc_id": "209faf97afc090db2f3447a3fea98cc9fccce5ef", "relevance": 1}, {"doc_id": "89f4a8ed61c1af384c895984ece7f07148747fdf", "relevance": 0}])
    print(len(papers))
    for p in papers:
        print(p.id)
        print(p.title)
        #print(len(p.inCitations))
        for a in p.authors:
            print(a)
            a.annotate_gender_country()
            print(a.predicted_country + " " + a.predicted_gender)
        print("\n###\n")"""