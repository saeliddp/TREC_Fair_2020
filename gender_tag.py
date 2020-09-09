# Script tags authors in groups of 1000 to avoid the API limit described
# in gender.py

import boto3
import gender
import json
import sys

if __name__ == '__main__':
    dynamodb = boto3.resource('dynamodb', aws_access_key_id='', aws_secret_access_key='', region_name='us-west-2')
    table = dynamodb.Table('SampleAuthors')
    
    if len(sys.argv) < 2:
        print('USAGE: python gender_tag.py [0-31]')
        exit()
    
    thousand = int(sys.argv[1])
        
    print("Starting gender tagging for " + str(thousand) + ".json.")
    
    with open('data/thousands/' + str(thousand) + '.json', 'r') as fr:
        ids = json.load(fr)
    
    count = 0
    unprocessed = []
    for author_id in ids:
        response = table.get_item(
            Key={
                 'corpus_author_id': author_id
            }
        )
        if 'Item' in response:
            item = response['Item']
            if 'name' in item:
                pred_gender = gender.get_gender(item['name'])
                if pred_gender == 429:
                    print("REQUEST LIMIT REACHED... SAVING REMAINING IDS TO " + str(thousand) + "_fail.json")
                    with open("thousands/" + str(thousand) + "_fail.json", "w") as fw:
                        json.dump(ids[ids.index(author_id):], fw)
                    exit()
                    
                if pred_gender == 'none':
                    unprocessed.append(author_id)
                    
                table.update_item(
                    Key={
                        'corpus_author_id': author_id
                    },
                    UpdateExpression='SET predicted_gender = :val1',
                    ExpressionAttributeValues={
                        ':val1': pred_gender
                    }
                )
                count += 1
                print(str(count) + " completed", end='\r')
                
    
    print(str(count) + " completed")
        
    with open('data/thousands/unprocessed.json', 'r') as fr:
        old_unprocessed = json.load(fr)
        
    with open('data/thousands/unprocessed.json', 'w') as fw:
        json.dump(old_unprocessed.extend(unprocessed), fw)