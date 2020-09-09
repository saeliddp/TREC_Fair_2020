### Developed on AWS Cloud9
### Requirements
Python 3.6, scipy, scholarly, boto3, nltk, gensim, googletrans,
bs4, fp (FreeProxy)

### InfoSeeking Lab: TREC Fair Ranking Task 2020
Developers: Coco Li, Daniel Saelid  
Directors: Dr. Chirag Shah, Dr. Ruoyuan Gao  
Contributors: Bin Han, Dr. Yunhe Feng  

### Primary Scripts
rerank.py - reranking algorithms  
rerank_pipeline.py - produces submission files  
retrieval_pipeline.py - produces submission files  
bm25.py - gives docs bm25 scores based on queries  

### Author Tagging Scripts
country.py - methods for predicting country  
country_tag.py - performs tagging + upload  
gender.py - method for predicting gender  
gender_tag.py - performs tagging + upload  
h_i_grouping.py - tags authors based on h-index and i10  
gender_country_group.py - uploads group distributions for authors  

### DynamoDB Querying Scripts
query.py - handles batch querying  
paper.py - json paper data -> python object  
scan.py - read through entire database to calculate statistics  

### DynamoDB Upload Scripts
author_upload.py - upload author data  
make_table.py - create tables  
subcorpus_upload.py - upload paper data  

### Assorted Utilities
preprocess.py - annotate papers for bm25  
country_tag_cleanup.py - tag remaining authors for country  
timeout.py - timeout method wrapper  
validate-run-rerank.py - ensure submission format is valid  
validate-run-retrieve.py - ensure submission format is valid  
wget-drive.sh - download files from google drive  

### 2019_eval/ Directory
Contains utilities to run the 2019 evaluation script for reranking on output files  

### country_data/ Directory
Contains files to connect author data to countries  
Provided by Bin Han  

### *_submissions/ Directories
Contains compressed submissions to TREC  

### data/ Directory
Contains any data to be uploaded to DynamoDB or auxiliary data (csv, json, txt, etc)  
data/destinationPath contains the license agreement for Ai2 Semantic Scholar  