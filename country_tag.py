# Utility to tag authors' countries based on the author id files contained
# in thousands/
# Unsolved bug: sometimes the script pauses for a long time on one author
# ^C skips to the next author, and the script eventually processed all 32k
# authors in the database (predicted country for 60%)

import boto3
import sys
import json
import country
from timeout import timeout
import time

def gather_info(item):
    if 'num_citations' in item:
        return country.get_author_country_email(item['name'], numcitations=int(item['num_citations']))
    else:
        return country.get_author_country_email(item['name'])
        
if __name__ == '__main__':
    # Can list >= 1 files (thousands/filename.json) as arguments
    if len(sys.argv) < 2:
        print("USAGE: python country.py [filename]...")
        exit()

    files = []
    for fn in sys.argv[1:]:
        files.append(fn)

    dynamodb = boto3.resource('dynamodb', aws_access_key_id='', aws_secret_access_key='', region_name='us-west-2')

    table = dynamodb.Table('SampleAuthors')

    count = 0
    country_success = 0
    email_success = 0
    json_lines = []
    
    for fn in files:
        with open(fn, 'r') as fr:
            id_list = json.load(fr)
        
        print("Beginning Processing for " + fn)
        for author_id in id_list:
            response = table.get_item(
                Key={
                     'corpus_author_id': author_id
                }
            )
            if 'Item' in response:
                item = response['Item']
                if 'name' in item:
                    country_email = gather_info(item)
                    
                    print(author_id)
                    print(country_email)

                    if country_email[0] != 'none':
                        country_success += 1
                    if country_email[1] != 'none':
                        email_success += 1
                        
                    table.update_item(
                        Key={
                            'corpus_author_id': author_id
                        },
                        AttributeUpdates={
                            'predicted_country': {
                                'Value': country_email[0],
                                'Action': 'PUT'
                            },
                            'email': {
                                'Value': country_email[1],
                                'Action': 'PUT'
                            },
                            'country_method': {
                                'Value': country_email[2],
                                'Action': 'PUT'
                            }
                        }
                    )
                    
             
            count += 1
            print(str(count) + " completed, " + str(country_success) + " successes\n")

    print("#")
    print("Country Successes: " + str(country_success))
    print("Num Emails Found: " + str(email_success))


