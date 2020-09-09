# Groups authors based on h-index and i10 ratings
# These groups were not used for submission

import boto3
import pickle

if __name__ == '__main__':
    dynamodb = boto3.resource('dynamodb', aws_access_key_id='', aws_secret_access_key='', region_name='us-west-2')
    
    table = dynamodb.Table('SampleAuthors')
    
    response = table.scan()
    data = response['Items']
    
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    
    i10_freqs = {}
    
    for item in data:
        """
        h_index = int(item['h_index'])
        if h_index < 5:
            h_group = 0
        elif h_index < 15:
            h_group = 1
        elif h_index < 30:
            h_group = 2
        else:
            h_group = 3
        """
        i10 = int(item['i10'])
        # ranges computed prior
        if i10 <= 1:
            i10_group = 0
        elif i10 <= 4:
            i10_group = 1
        elif i10 <= 9:
            i10_group = 2
        elif i10 <= 19:
            i10_group = 3
        elif i10 <= 38:
            i10_group = 4
        elif i10 <= 87:
            i10_group = 5
        else:
            i10_group = 6
        """   
        if i10 in i10_freqs:
            i10_freqs[i10] += 1
        else:
            i10_freqs[i10] = 1
        """
        table.update_item(
            Key={
                'corpus_author_id': item['corpus_author_id']
            },
            UpdateExpression="set i10_group=:i",
            ExpressionAttributeValues={
                ':i': str(i10_group)
            },
        )
    
    """with open('i10_freqs.pickle', 'wb') as fw:
        pickle.dump(i10_freqs, fw)"""