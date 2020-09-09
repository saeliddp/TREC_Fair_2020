# Uses batch_writer to upload all papers from the provided corpus to the specified table
# data/records is the subcorpus provided by TREC organizers
# It is not present for size concerns

import boto3
import json
import math, time

dynamodb = boto3.resource('dynamodb', aws_access_key_id='', aws_secret_access_key='', region_name='us-west-2')

def upload(tablename, lines):
    table = dynamodb.Table(tablename)
    failed_papers = []
    partition_failures = []
    with table.batch_writer() as batch:
        for line in lines:
            paper = json.loads(line)
            segments = partition(paper)
            
            if len(segments) == 0:
                    partition_failures.append(line)
                    
            for segment in segments:
                try:
                    batch.put_item(Item=segment)
                except:
                    failed_papers.append(line)
    
    final_failed_papers = []
    print("# of failed papers: " + str(len(failed_papers)))
    if len(failed_papers) > 0:
        print("Waiting 10 seconds")
        time.sleep(10)
        for line in failed_papers:
            paper = json.loads(line)
            try:
                response=table.put_item(
                    Item=paper
                )
            except:
                final_failed_papers.append(line)
    
    with open('data/' + tablename + '_partition_failures.txt', 'w') as fw:
        fw.writelines(partition_failures)
        
    with open('data/' + tablename + '_failed_papers.txt', 'w') as fw:
        fw.writelines(final_failed_papers)

# Size estimate for use in partitioning
def approx_json_size(somejson):
    return len((str(somejson).encode('utf-8')))
    
# 0 = first partition, no other partitions
# 1 = first partition, others exist
# generally, assumes partitions are a result of incitations or outcitations
def partition(paper):
    segments = []
    paper_id = paper['id']
    total_size = approx_json_size(paper)
    if total_size < 390000: # 390kb
        paper['partition'] = 0
        segments.append(paper)
    else:
        target_key = None
        for key in paper:
            if total_size - approx_json_size(paper[key]) <= 390000:
                target_key = key
                break
        if target_key is None:
            print("PARTITION FAILURE: SIZABLE KEY NOT FOUND IN DOC_ID " + paper_id)
        elif type(paper[target_key]) is not list:
            print("PARTITION FAILIURE: NOT A LIST, DOC_ID " + paper_id)
        else:
            key_data = paper.pop(target_key)
            paper['partition'] = 1
            segments.append(paper)
            num_partitions = math.ceil(approx_json_size(key_data) / 390000)
            list_increment = math.floor(len(key_data) / num_partitions)
            ind = 0
            part = 2
            while ind + list_increment <= len(key_data):
                new_seg = {}
                new_seg['id'] = paper_id
                new_seg['partition'] = part
                part += 1
                new_seg[target_key] = key_data[ind: ind + list_increment]
                segments.append(new_seg)
                ind = ind + list_increment
            
            if ind < len(key_data):
                new_seg = {}
                new_seg['id'] = paper_id
                new_seg['partition'] = part
                new_seg[target_key] = key_data[ind: ]
                segments.append(new_seg)
    return segments

if __name__ == '__main__':
    pass
    """with open('data/records', 'r') as fr:
        lines = fr.readlines()
    upload('SamplePapers', lines)"""