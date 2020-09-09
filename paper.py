# Python wrapper for papers
import json
def extract_list(list_of_dicts, type_of_key):
        output = []
        for item in list_of_dicts:
            output.append(item[type_of_key])
        return output
        
class Paper(object):
    # not necessarily a batch_response, but some json response
    # from a query... json must describe a paper
    def __init__(self, batch_response):
        try:
            self.paperAbstract = batch_response['paperAbstract']['S']
        except:
            try:
                self.paperAbstract = batch_response['paperAbstract']
            except:
                self.paperAbstract = None
        
        try:
            self.doiUrl = batch_response['doiUrl']['S']
        except:
            try:
                self.doiUrl = batch_response['doiUrl']
            except:
                self.doiUrl = None
        
        try:
            self.sources = extract_list(batch_response['sources']['L'], 'S')
        except:
            try:
                self.sources = batch_response['sources']
            except:
                self.sources = None
            
        try:
            self.s2PdfUrl = batch_response['s2PdfUrl']['S']
        except:
            try:
                self.s2PdfUrl = batch_response['s2PdfUrl']
            except:
                self.s2PdfUrl = None
            
        try:
            self.venue = batch_response['venue']['S']
        except:
            try:
                self.venue = batch_response['venue']
            except:
                self.venue = None
            
        try:
            self.journalPages = batch_response['journalPages']['S']
        except:
            try:
                self.journalPages = batch_response['journalPages']
            except:
                self.journalPages = None
            
        try:
            self.s2Url = batch_response['s2Url']['S']
        except:
            try:
                self.s2Url = batch_response['s2Url']
            except:
                self.s2Url = None
        
        try:
            self.magId = batch_response['magId']['S']
        except:
            try:
                self.magId = batch_response['magId']
            except:
                self.magId = None
        
        try:
            self.pdfUrls = extract_list(batch_response['pdfUrls']['L'], 'S')
        except:
            try:
                self.pdfUrls = batch_response['pdfUrls']
            except:
                self.pdfUrls = None
            
        try:
            self.fieldsOfStudy = extract_list(batch_response['fieldsOfStudy']['L'], 'S')
        except:
            try:
                self.fieldsOfStudy = batch_response['fieldsOfStudy']
            except:
                self.fieldsOfStudy = None
            
        try:
            self.journalName = batch_response['journalName']['S']
        except:
            try:
                self.journalName = batch_response['journalName']
            except:
                self.journalName = None
        
        try:
            self.outCitations = extract_list(batch_response['outCitations']['L'], 'S')
        except:
            try:
                self.outCitations = batch_response['outCitations']
            except:
                self.outCitations = None
        
        try:
            self.year = batch_response['year']['N']
        except:
            try:
                self.year = batch_response['year']
            except:
                self.year = None
        
        try:
            self.inCitations = extract_list(batch_response['inCitations']['L'], 'S')
        except:
            try:
                self.inCitations = batch_response['inCitations']
            except:
                self.inCitations = None
        
        try:
            self.journalVolume = batch_response['journalVolume']['S']
        except:
            try:
                self.journalVolume = batch_response['journalVolume']
            except:
                self.journalVolume = None
        
        try:
            self.entities = extract_list(batch_response['entities']['L'], 'S')
        except:
            try:
                self.entities = batch_response['entities']
            except:
                self.entities = None
        
        try:
            self.pmid = batch_response['pmid']['S']
        except:
            try:
                self.pmid = batch_response['pmid']
            except:
                self.pmid = None
        
        try:
            self.id = batch_response['id']['S']
        except:
            try:
                self.id = batch_response['id']
            except:
                self.id = None
                
        try:
            self.paperLanguage = batch_response['paperLanguage']['S']
        except:
            try:
                self.paperLanguage = batch_response['paperLanguage']
            except:
                self.paperLanguage = None
        
        try:
            self.partition = batch_response['partition']['N']
        except:
            try:
                self.partition = batch_response['partition']
            except:
                self.parttition = None
            
        try:
            self.doi = batch_response['doi']['S']
        except:
            try:
                self.doi = batch_response['doi']
            except:
                self.doi = None
        
        try:
            self.title = batch_response['title']['S']
        except:
            try:
                self.title = batch_response['title']
            except:
                self.title = None
        
        try:
            self.authors = []
            author_list = extract_list(batch_response['authors']['L'], 'M')
            for a in author_list:
                try:
                    self.authors.append(Author(a))
                except:
                    pass
        except:
            self.authors = []
            try:
                for a in batch_response['authors']:
                    try:
                        self.authors.append(Author(a))
                    except:
                        pass
            except:
                self.authors = None
        
        try:
            self.selectedTokens = extract_list(batch_response['selectedTokens'], 'S')
        except:
            try:
                self.selectedTokens = batch_response['selectedTokens']
            except:
                self.selectedTokens = None
    
        try:
            self.gender_score = float(batch_response['gender_score']['S'])
        except:
            try:
                self.gender_score = float(batch_response['gender_score'])
            except:
                self.gender_score = None
        
        try:
            self.country_score = float(batch_response['country_score']['S'])
        except:
            try:
                self.country_score = float(batch_response['country_score'])
            except:
                self.country_score = None
        
        try:
            self.gender_dist = json.loads(batch_response['gender_dist']['S'])
        except:
            try:
                self.gender_dist = json.loads(batch_response['gender_dist'])
            except:
                self.gender_dist = None
        
        try:
            self.country_dist = json.loads(batch_response['country_dist']['S'])
        except:
            try:
                self.country_dist = json.loads(batch_response['country_dist'])
            except:
                self.country_dist = None

    def addInCitations(self, citation_list):
        if self.inCitations is None:
            self.inCitations = citation_list
        else:
            self.inCitations.extend(citation_list)
            
    def addOutCitations(self, citation_list):
        if self.outCitations is None:
            self.outCitations = citation_list
        else:
            self.outCitations.extend(citation_list)
    
    def annotate_authors(self):
        if self.authors is not None:
            for a in self.authors:
                a.annotate_gender_country()

import query
class Author(object):
    def __init__(self, author_map):
        self.predicted_gender = 'none'
        self.predicted_country = 'none'
        try:
            self.name = author_map['name']['S']
            self.ids = extract_list(author_map['ids']['L'], 'S')
        except:
            try:
                self.name = author_map['name']
                self.ids = author_map['ids']
            except:
                self.name = None
                self.ids = None
    
    def __str__(self):
        return self.name + ": " + str(self.ids)
    
    def annotate_gender_country(self):
        if len(self.ids) > 0:
            g_c = query.author_gender_country(self.ids[0])
            self.predicted_gender = g_c[0]
            self.predicted_country = g_c[1]