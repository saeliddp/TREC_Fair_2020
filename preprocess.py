# Preprocess/tokenize documents for bm25


import json
import re
import string
import nltk
import boto3
import langdetect
import googletrans
from boto3 import resource
from boto3.dynamodb.conditions import Key
from nltk import word_tokenize
#from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
from langdetect import detect 
from googletrans import Translator

selectedFields = ['title', 'paperAbstract', 'entities', 'fieldsOfStudy', 'authors',
                   'venue', 'journalName', 'sources']
stopWords = set(stopwords.words('english')) 
translator = Translator()

# print(stopWords)
# nltk.download('stopwords')
# nltk.download('punkt')

# stemmer = PorterStemmer()

# def normalize(s):
#     '''
#     normalize extra spaces, numbers, and links
#     remove punctuation
#     '''
#     s = re.sub('[ \t]+', ' ', s)
#     s = re.sub('\n+', '\n', s)
#     s = re.sub('\s+', ' ', s)
#     #s = re.sub('\d+', '0', s)
#     s = re.sub('https?://[^\s()]+', 'thisisalink,', s)
#     s = re.sub('[a-z0._]+@[a-z0.-]+[a-z]','thisisanemail',s)
#     # s = s.translate(str.maketrans('', '', string.punctuation))
#     s = re.sub('[^a-zA-Z0-9_\s]', '', s)
#     s = s.lower()
#     return s


def tokenize(t):
    t = t.lower()
    tokens = word_tokenize(t)
    tokensCpy = []
    for t in tokens:
        if t not in stopWords:
            tokensCpy.append(t)
    tokens = tokensCpy
    #tokens = [stemmer.stem(t) for t in tokens]
    return tokens


def get_raw_text_all(doc):
    raw_text = ''
    for key, value in doc.items():
        if isinstance(doc[key], list):
            for i in doc[key]:
                if isinstance(i, dict):
                    if str(i['name']) != '':
                        if len(i['ids']) == 0:
                            raw_text = raw_text + ' ' + str(i['name'])
                        else:
                            raw_text = raw_text + ' ' + str(i['name']) + ' ' + str(i['ids'][0])
                else:
                    if str(i) != '':
                        raw_text = raw_text + ' ' + str(i)
        else:
            if str(value) != '':
                raw_text = raw_text + ' ' + str(value)
    raw_text = raw_text[1:]
    return raw_text


def get_raw_text_selected(doc):
    raw_text = ''
    for key, value in doc.items():
        if key in selectedFields:
            if isinstance(doc[key], list):
                for i in doc[key]:
                    if isinstance(i, dict):
                        if str(i['name']) != '':
                            raw_text = raw_text + ' ' + str(i['name'])  
                    else:
                        if str(i) != '':
                            raw_text = raw_text + ' ' + str(i)
            else:
                if str(value) != '':
                    raw_text = raw_text + ' ' + str(value)
    raw_text = raw_text[1:]
    return raw_text


# def query_table(table_name, filter_key=None, filter_value=None):
#     """
#     Perform a query operation on the table.
#     Can specify filter_key (col name) and its value to be filtered.
#     """
#     table = dynamodb_resource.Table(table_name)

#     if filter_key and filter_value:
#         filtering_exp = Key(filter_key).eq(filter_value)
#         response = table.query(KeyConditionExpression=filtering_exp)
#     else:
#         response = table.query()

#     return response
def main():
    
    # client = boto3.client('dynamodb',
    #   aws_access_key_id='AKIAVHNOJKS6YR75QSMU',
    #   aws_secret_access_key='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',
    #   region_name='us-west-2')
    
    dynamodb = boto3.resource('dynamodb', aws_access_key_id='', aws_secret_access_key='', region_name='us-west-2')
    table = dynamodb.Table('SamplePapers')
    # print(table.item_count)
    response = table.scan()
    data = response['Items']
    
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    languages = {'not detected': 0}
    
    for document in data:
        partition = document['partition']
        if document['partition'] > 1:
            pass
        else:
            doc_id = document['id']
            title = document['title']
            abstract = document['paperAbstract']
            try:
                lang = detect(abstract)
                if lang not in languages:
                    languages[lang] = 1
                else:
                    languages[lang] = languages[lang] + 1
            except:
                lang = ''
                languages['not detected'] = languages['not detected'] + 1
                # print('This title cannot be detected:' + doc_id + title)
                
            raw_text = abstract + title
            
            if lang != 'en':
                translation = translator.translate(raw_text)
                raw_text = translation.text

            # text = normalize(raw_text)
            tokens = tokenize(raw_text)
            table.update_item(
                Key={
                    'id': doc_id,
                    'partition': partition
                },
                UpdateExpression='SET selectedTokens = :val1',
                ExpressionAttributeValues={
                    ':val1': tokens
                }
            )
            table.update_item(
                Key={
                    'id': doc_id,
                    'partition': partition
                },
                UpdateExpression='SET paperLanguage = :val1',
                ExpressionAttributeValues={
                    ':val1': lang
                }
            )
