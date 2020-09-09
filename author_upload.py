# Uploads provided author data to a dynamoDB database
# Source data: data/authors.csv
# Also splits authors into lists of 1000, saves these to 
# data/thousands for author tagging purposes.

import boto3
import csv
import gender
import json

if __name__ == '__main__':
    dynamodb = boto3.resource('dynamodb', aws_access_key_id='', aws_secret_access_key='', region_name='us-west-2')
    table = dynamodb.Table('SampleAuthors')
    
    author_csv = open('data/authors.csv', 'r')
    csv_lines = csv.reader(author_csv, delimiter=',')
    
    failed_authors = []
    with table.batch_writer() as batch:
        attribute_names = csv_lines.__next__()
        #json_lines = []
        count = 0
        id_list = []
        for row in csv_lines:
            json_line = {}
            for i in range(len(attribute_names)):
                json_line[attribute_names[i]] = row[i]
                    
            try:
                batch.put_item(json_line)
                id_list.append(json_line['corpus_author_id'])
                if len(id_list) == 1000:
                    with open('data/thousands/' + str(count) + '.json', 'w') as fw:
                        json.dump(id_list, fw)
                    id_list = []
                    count += 1
            except:
                failed_authors.append(str(json_line))
                
                
        with open('data/thousands/' + str(count) + '.json', 'w') as fw:
                        json.dump(id_list, fw)
    
    
    with open('data/failed_authors.txt', 'w') as fw:
        fw.writelines(failed_authors)
        
    author_csv.close()