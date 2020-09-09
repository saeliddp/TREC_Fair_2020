# Scans author database to report overall distributions for various groups

import boto3
dynamodb = boto3.resource('dynamodb', aws_access_key_id='', aws_secret_access_key='', region_name='us-west-2')

table = dynamodb.Table('SampleAuthors')

response = table.scan()
data = response['Items']

while 'LastEvaluatedKey' in response:
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    data.extend(response['Items'])

advanced = {
    'united states', 'belgium', 'australia', 'south korea', 'taiwan', 'germany',
    'canada', 'spain', 'switzerland', 'netherlands', 'united kingdom', 'finland',
    'japan', 'italy', 'new zealand', 'portugal', 'european union', 'united kingdom (no new registrations, see also uk)',
    'france', 'hong kong', 'denmark', 'austria', 'singapore', 'luxembourg',
    'norway', 'sweden', 'ireland', 'greece', 'israel', 'slovenia', 'cyprus',
    'macau', 'latvia', 'lithuania', 'puerto rico', 'malta', 'iceland', 'slovakia',
    'estonia', 'czech republic'
    
}

developing = {
    'egypt', 'brazil', 'malaysia', 'thailand', 'peru', 'russia', 'turkey', 'china',
    'argentina', 'pakistan', 'serbia', 'costa rica', 'india', 'sierra leone', 'iran',
    'algeria', 'british indian ocean territory', 'kazakhstan', 'ukraine', 'vietnam',
    'south africa', 'mexico', 'tajikistan', 'philippines', 'syria', 'morocco',
    'indonesia', 'hungary', 'colombia', 'iraq', 'bangladesh', 'chile', 'bulgaria',
    'palestine', 'nigeria', 'united arab emirates', 'brunei', 'ecuador', 'saudi arabia',
    'samoa', 'cuba', 'poland', 'ghana', 'cambodia', 'mongolia', 'north korea', 'nepal',
    'uruguay', 'panama', 'jamaica', 'tuvalu', 'micronesia, federated states of', 'laos',
    'lebanon', 'niue', 'guernsey', 'kyrgyzstan', 'soviet union', 'cameroon', 'georgia',
    'montenegro', 'kenya', 'bolivia', 'senegal', 'cocos (keeling) islands', 'tunisia',
    'armenia', 'tanzania', 'belarus', 'countries', 'yemen', 'uganda', 'dominican republic',
    'el salvador', 'north macedonia', 'anguilla', 'libya', 'ascension island', 'kuwait',
    'oman', 'zimbabwe', 'qatar', 'jordan', 'venezuela', 'bosnia and herzegovina', 'ethiopia',
    'sri lanka', 'sudan', 'ivory coast', 'guatemala', 'romania', 'croatia'
}
num_advanced = 0
num_developing = 0
no_country = 0
male = 0
female = 0
no_gender = 0
for item in data:
    if 'predicted_country' in item:
        if item['predicted_country'] in advanced:
            num_advanced += 1
        elif item['predicted_country'] in developing:
            num_developing += 1
        else:
            no_country += 1
            
    if 'predicted_gender' in item:
        if item['predicted_gender'] == 'male':
            male += 1
        elif item['predicted_gender'] == 'female':
            female += 1
        else:
            no_gender += 1

print('a: ' + str(num_advanced) + ' d: ' + str(num_developing) + ' n: ' + str(no_country))
print('m: ' + str(male) + ' f: ' + str(female) + ' n: ' + str(no_gender))