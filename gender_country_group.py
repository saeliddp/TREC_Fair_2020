# Calculates and uploads author group distributions for each paper in the
# corpus
# Method used is _distribution() not _score() (initial method)

import random

def gender_score(paper):
    authors = paper.authors
    total_score = 0
    one_found = False
    for a in authors:
        if a.predicted_gender == 'male':
            total_score -= 1
        elif a.predicted_gender == 'female':
            total_score += 1
        if total_score != 0:
            one_found = True
    if len(authors) > 0 and one_found:
        return total_score / len(authors)
    else:
        return 'none'

def gender_distribution(paper):
    authors = paper.authors
    likelihood_male = 0.74389 # calculated over the entire corpus
    freqs = [0, 0] # male, female
    for a in authors:
        if a.predicted_gender == 'male':
            freqs[0] += 1
        elif a.predicted_gender == 'female':
            freqs[1] += 1
        else: # 'guess' the author's gender based on overall probability in the corpus
            if random.random() <= likelihood_male:
                freqs[0] += 1
            else:
                freqs[1] += 1
    return freqs

        
# source: https://www.imf.org/~/media/Files/Publications/WEO/2019/October/English/TableA.ashx
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

def country_score(paper):
    authors = paper.authors
    score = 0
    one_found = False
    for a in authors:
        if a.predicted_country in advanced:
            score -= 1
        elif a.predicted_country in developing:
            score += 1
        elif a.predicted_country != 'none':
            print('country not found: ' + a.predicted_country)
        if score != 0:
            one_found = True
    if len(authors) > 0 and one_found:
        return score / len(authors)
    else:
        return 'none'
        
def country_distribution(paper):
    authors = paper.authors
    likelihood_advanced = 0.79330 # calculated over the entire corpus
    freqs = [0, 0] # advanced, developing
    for a in authors:
        if a.predicted_country in advanced:
            freqs[0] += 1
        elif a.predicted_country in developing:
            freqs[1] += 1
        else: # 'guess' the author's country based on overall probability in the corpus
            if random.random() <= likelihood_advanced:
                freqs[0] += 1
            else:
                freqs[1] += 1
    return freqs

if __name__ == '__main__':
    import boto3
    from paper import Paper, Author

    dynamodb = boto3.resource('dynamodb', aws_access_key_id='', aws_secret_access_key='', region_name='us-west-2')
    
    table = dynamodb.Table('SamplePapers')
    
    response = table.scan(ProjectionExpression="id,authors,#p",ExpressionAttributeNames={'#p': 'partition'})
    data = response['Items']
    
    while 'LastEvaluatedKey' in response:
        response = table.scan(ProjectionExpression="id,authors,#p",ExpressionAttributeNames={'#p': 'partition'},ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    
    for item in data:
        paper = Paper(item)
        if paper.partition <= 1:
            paper.annotate_authors()
            #for a in paper.authors:
                #print(a.predicted_country + " " + a.predicted_gender)
            #g_score = gender_score(paper)
            #c_score = country_score(paper)
            g_dist = gender_distribution(paper)
            c_dist = country_distribution(paper)
            #print(c_dist)
            #print(g_dist)
            table.update_item(
                Key={
                    'id': paper.id,
                    'partition': paper.partition
                },
                UpdateExpression="set gender_dist=:g, country_dist=:c",
                ExpressionAttributeValues={
                    ':g': str(g_dist),
                    ':c': str(c_dist)
                },
            )
            