# USAGE: python make_table.py [TableName]
# This script makes a new table in DynamoDB
# In order to change the characteristics of the table, edit the try block
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table

import boto3
import sys

if __name__ == '__main__':
    if len(sys.argv) != 4 and len(sys.argv) != 6:
        print("USAGE: python make_table.py [TableName] [HASH key name] [HASH key type] OPTIONAL[[RANGE key name] [RANGE key type]]")
        sys.exit(0)
    
    dynamodb = boto3.resource('dynamodb', aws_access_key_id='', aws_secret_access_key='', region_name='us-west-2')
    
    try:
        key_schema = []
        defns = []
        key_schema.append({"AttributeName": sys.argv[2], "KeyType": "HASH"})
        defns.append({"AttributeName": sys.argv[2], "AttributeType": sys.argv[3]})
        if len(sys.argv) > 4:
            key_schema.append({"AttributeName": sys.argv[4], "KeyType": "RANGE"})
            defns.append({"AttributeName": sys.argv[4], "AttributeType": sys.argv[5]})
        resp = client.create_table(
            TableName=sys.argv[1],
            # Declare your Primary Key in the KeySchema argument
            KeySchema=key_schema,
            # Any attributes used in KeySchema or Indexes must be declared in AttributeDefinitions
            AttributeDefinitions=defns,
            # ProvisionedThroughput controls the amount of data you can read or write to DynamoDB per second.
            # You can control read and write capacity independently.
            BillingMode="PAY_PER_REQUEST"
        )
        print(sys.argv[1] + " table created successfully!")
    except Exception as e:
        print("Error creating table: ")
        print(e)