# Collection of methods to predict an author's country based on their name
# and citation count.

from scholarly import scholarly
from bs4 import BeautifulSoup
from timeout import timeout
from fp.fp import FreeProxy
import boto3
import sys
import json
import requests
import re
import pickle

google_cloud_api_key = ""

# dictionary loading
def d_load(filepath):
    with open(filepath, 'rb') as fr:
        output = pickle.load(fr)
    return output

# maps attributes to countries
abbr_country = d_load('country_data/abbr-country.pickle')
city_country = d_load('country_data/city-country.pickle')
univ_country = d_load('country_data/univ-country.pickle')

def set_new_proxy():
    while True:
        proxy = FreeProxy(rand=True, timeout=3).get()
        proxy_works = scholarly.use_proxy(http=proxy, https=proxy)
        if proxy_works:
            break
    print("Working proxy:", proxy)
    return proxy    

#set_new_proxy()

def get_author_country_email(authorname, numcitations=-1):
    country = 'none'
    email = 'none'
    method = 'none'
    author = get_author(authorname, numcitations)
    if author is not None:
        # first, try to use email
        if author.email != '' and author.email is not None:
            print("Searching for email extension...")
            email = author.email
            extension = email.split('.')[-1].lower()
            if extension in abbr_country:
                country = abbr_country[extension]
            elif extension == 'edu' or extension == 'gov':
                country = abbr_country['us']
            
            if country != 'none':
                method = 'email'
        
        # if email gets us nothing, try to use affiliation
        # this is a slow operation--projected to execute rarely
        if country == 'none' and author.affiliation != '' and author.affiliation is not None:
            print("Parsing affiliation...")
            aff = author.affiliation.lower()
            for univ in univ_country:
                if univ in aff:
                    country = abbr_country[univ_country[univ]]
                    break
            
            if country == 'none':
                for city in city_country:
                    if city in aff:
                        country = abbr_country[city_country[city]]
                        break
            
            if country != 'none':
                method = 'affiliation'
                
        if country == 'none':
            country = scrape_country(authorname + ', ' + author.affiliation)
            if country != 'none':
                method = 'google_scrape_scholarly_data'
            
        # last effort--google places api
        if country == 'none' and author.affiliation != '' and author.affiliation is not None:
            geo_text = extract_geo_from_text(author.affiliation)
            if geo_text is None:
                country = places_api_country(author.affiliation)
            else:
                country = places_api_country(geo_text)
            
            if country != 'none':
                method = 'places_api'
    else:
        country = scrape_country(authorname + ' homepage')
        if country != 'none':
            method = 'google_scrape_name_only'
    return [country, email, method]

@timeout(30)
def author_next_util(search_query):
    try:
        return next(search_query)
    except StopIteration:
        raise StopIteration()
    except Exception as e:
        print(e)
        # TODO: Change to a logical exception for request limits
        raise ZeroDivisionError() # arbitrary exception type
    
def get_author(authorname, numcitations=-1):
    print("Searching Scholarly...")
    best_guess = None
    try:
        search_query = scholarly.search_author(authorname)
                    
        if numcitations == -1: # we have no citation data for this author
            while True:
                try:
                    best_guess = author_next_util(search_query)
                    break
                except StopIteration: # end of author list reached (list has no members)
                    break
                except ZeroDivisionError: # request limit for ip address reached
                    print("Limit Reached... setting new proxy")
                    set_new_proxy()
                    search_query = scholarly.search_author(authorname)
                except: # timeout
                    break
        else:
            while True:
                try:	
          
                    author = author_next_util(search_query)
                    #print(author.affiliation)
                    #print(extract_geo_from_text(author.affiliation))
                    if best_guess is None:
                        best_guess = author
                    else:
                        try: # in case .citedby not contained
                            if abs(numcitations - author.citedby) < abs(numcitations - best_guess.citedby):
                                best_guess = author
                        except:
                            pass
                except StopIteration: # end of author list reached
                    break
                except ZeroDivisionError: # request limit
                    print("Limit Reached... setting new proxy")
                    set_new_proxy()
                    search_query = scholarly.search_author(authorname)
                except: # timeout
                    break

                    
    except:
        return None

    return best_guess 

def places_api_country(textquery):
    print("Google Places API..." + textquery)
    try:
        response = requests.get("https://maps.googleapis.com/maps/api/place/findplacefromtext/json?key=" + google_cloud_api_key + "&input=" + textquery + "&inputtype=textquery&fields=formatted_address")
        
        with open('google_api_requests.txt', 'r') as fr:
            num_requests = fr.read()
        with open('google_api_requests.txt', 'w') as fw:
            fw.write(str(int(num_requests) + 1))
        
        candidates = response.json()['candidates']
        address = candidates[0]['formatted_address'] # will always go with first hit
        return address.split(", ")[-1].lower()
    except:
        return 'none'

def extract_geo_from_text(text):
    place_keywords = {'college', 'university', 'universidad', 'escuela', 'school', 'institute', 'academy', 'clinic', 'hospital', 'polytechnic', 'foundation', 'institution', 'research', 'lab', 'laboratory', 'incorporated', 'inc.', 'inc', 'analytics', 'llc', 'corporation', 'spotify', 'facebook', 'google', 'amazon', 'microsoft', 'apple'}
    tokens = re.split(r';+|,+', text.lower())
    tokens.reverse()
    
    for t in tokens:
        wordset = set(t.split())
        #print(wordset.difference(place_keywords))
        if len(wordset) > len(wordset.difference(place_keywords)):
            return t
    
    return None

def first_google_result(query):
    query = query.replace(' ', '+')
    URL = f"https://google.com/search?q={query}"
    USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:65.0) Gecko/20100101 Firefox/65.0"
    headers = {"user-agent" : USER_AGENT}
    response = requests.get(URL, headers=headers)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        for g in soup.find_all('div', class_='r'):
            anchors = g.find_all('a')
            if anchors:
                return anchors[0]['href']
    else:
        print(response.status_code)
    return 'none'
    
def scrape_country(query):
    print("Scraping Google...")
    url = first_google_result(query)
    if url != 'none':
        extension = url[url.index('://') + 3:].split('/')[0].split('.')[-1]
        if extension in ['edu', 'gov']:
            return abbr_country['us']
        elif extension in abbr_country:
            return abbr_country[extension]
            
    return 'none'
