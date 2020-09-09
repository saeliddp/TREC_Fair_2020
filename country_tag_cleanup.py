# Made this because my machine crashed while running country_tag.py
# Safely ignore this file.

import boto3
import sys
import json
import country
from timeout import timeout
import time

#@timeout(20)
def gather_info(item):
    if 'num_citations' in item:
        return country.get_author_country_email(item['name'], numcitations=int(item['num_citations']))
        #author = country.get_author(item['name'], numcitations=int(item['num_citations']))

    else:
        return country.get_author_country_email(item['name'])
        
if __name__ == '__main__':

    if len(sys.argv) < 4:
        print("You messed up")
        exit()
       
    prefix = "data/thousands/"

    files = {}
    files['1.json'] = 550
    files['4.json'] = 600
    files['8.json'] = 340
    files['12.json'] = 35
    files['16.json'] = 380
    files['20.json'] = 130
    files['24.json'] = 160
    files['28.json'] = 620
    files['11.json'] = 650
    files['19.json'] = 0
    files['23.json'] = 115
    files['14.json'] = 100
    files['15.json'] = 0

    dynamodb = boto3.resource('dynamodb', aws_access_key_id='', aws_secret_access_key='', region_name='us-west-2')

    table = dynamodb.Table('SampleAuthors')

    count = 0
    country_success = 0
    email_success = 0
    json_lines = []
    
    #country.set_new_proxy()
    for arg in sys.argv[2:]:
        fn = arg + '.json'
        with open(prefix + fn, 'r') as fr:
            id_list = json.load(fr)
        
        print("Beginning Processing for " + fn)
        for author_id in id_list[files[fn]:]:
            response = table.get_item(
                Key={
                     'corpus_author_id': author_id
                }
            )
            if 'Item' in response:
                item = response['Item']
                if 'name' in item:
                    country_email = gather_info(item)
                    
                    #if author is not None:
                    #   print(author.affiliation)
                        
                    #print(item['name'])
                    print(author_id)
                    print(country_email)
                    #print("#")
                    
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
            #if count % 10 == 0:
                #country.set_new_proxy()
            print(str(count) + " completed, " + str(country_success) + " successes\n")

    print("#")
    print("Country Successes: " + str(country_success))
    print("Num Emails Found: " + str(email_success))


