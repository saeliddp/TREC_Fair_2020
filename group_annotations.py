# Generates group annotations for testing with 2019 evaluation script
# Safely ignore this file

import boto3
from paper import Paper, Author

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
if __name__ == '__main__':
    dynamodb = boto3.resource('dynamodb', aws_access_key_id='', aws_secret_access_key='', region_name='us-west-2')
    
    table = dynamodb.Table('SamplePapers')
    
    response = table.scan(ProjectionExpression="id,authors,#p",ExpressionAttributeNames={'#p': 'partition'})
    data = response['Items']
    
    while 'LastEvaluatedKey' in response:
        response = table.scan(ProjectionExpression="id,authors,#p",ExpressionAttributeNames={'#p': 'partition'},ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    
    csv_lines = []
    num_genders = 0
    num_countries = 0
    num_papers = 0
    for item in data:
        paper = Paper(item)
        if paper.partition <= 1:
            num_papers += 1
            paper.annotate_authors()
            line = paper.id
            #gender_identified = False
            country_identified = False
            if paper.authors is not None:
                for a in paper.authors:
                    #if a.predicted_gender != 'none' and a.predicted_gender is not None:
                     #   line += "," + a.predicted_gender
                     #   gender_identified = True
                    if a.predicted_country != 'none' and a.predicted_country is not None:
                        if a.predicted_country in developing:
                            line += ',' + 'developing'
                        elif a.predicted_country in advanced:
                            line += ',' + 'advanced'
                        country_identified = True
                    
            #if gender_identified:
                #num_genders += 1
            if country_identified:
                num_countries += 1
            csv_lines.append(line + '\n')
    
    #print("Num genders: " + str(num_genders))
    print("Total Papers: " + str(num_papers))
    print("Total Countries: " + str(num_countries))
    #print("Num papers: " + str(len(data)))
    with open('data/country_distribution.csv', 'w') as fw:
        fw.writelines(csv_lines)