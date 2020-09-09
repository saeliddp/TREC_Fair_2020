# Methods to predict gender based on author name
# Currently uses the genderize.io API, which allows for
# 1000 free requests per ip addr per day
# TODO: use a proxy once the limit is reached

import requests

def get_gender(name, country_id="US"):
    try:
        name = name.split()[0]
    except:
        return 'none'
        
    response = requests.get("https://api.genderize.io?name=" + name + "&country_id=" + country_id)
    if response.status_code != 200:
        print(response.status_code)
        if response.status_code == 429:
            return 429
        else:
            return 'none'
    else:
        return response.json()['gender']

def get_gender_batch(names, country_id="US"):
    request_base = "https://api.genderize.io/?"
    genders = {}
    for name in names:
        name = name.split()[0].lower()
        genders[name] = ''
        request_base += "name[]=" + name + "&"
    response = requests.get(request_base[:-1])
    
    if response.status_code != 200:
        print(response.status_code)
    else:
        for piece in response.json():
            genders[piece['name']] = piece['gender']
    
    return genders