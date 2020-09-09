# Runs reranking algorithm on the bm25 results for retrieval
# Does not run bm25 each run -- should be the model for rerank as well

import bm25
import rerank
import json
import time
import sys

class FakePaper(object):
    def __init__(self, doc_dict):
        self.id = doc_dict['doc_id']
        self.gender_dist = doc_dict['gender_dist']
        self.country_dist = doc_dict['country_dist']
        self.authors = []
        for i in range(sum(self.gender_dist)):
            self.authors.append(0)
            
if __name__ == '__main__':
    if len(sys.argv) != 6:
        print("USAGE")
        exit()
    queries = sys.argv[4]
    eval_seq = sys.argv[5]
    
    r_weight = int(sys.argv[1]) / 100
    g_weight = int(sys.argv[2]) / 100
    c_weight = int(sys.argv[3]) / 100
    
    print("prepping bm25 results...")
    start = time.time()
    
    with open('data/bm25_retrieval.jsonl', 'r') as fr:
        res_lines = fr.readlines()
        
    results = []
    for r in res_lines:
        results.append(json.loads(r))
    
    for r in results:
        for doc_dict in r['ranking']:
            doc_dict['paper'] = FakePaper(doc_dict)
            
    print("done")
    
    """
    with open('data/query_rankings.csv', 'r') as fr:
        csv_lines = fr.readlines()
    """
    total_docs = 0
    
    print("reranking...")
    curr_csv_ind = 0
    """
    if curr_csv_ind == len(csv_lines):
        csv_lines.append('raw_bm25,relevance,gender_score,\n')
    else:
        csv_lines[curr_csv_ind] = csv_lines[curr_csv_ind][:-1]
        csv_lines[curr_csv_ind] += 'disp_impact_balanced_per_query,relevance,gender_score,\n' 
    """
    curr_csv_ind += 1
    
    missing_docs = []
    for query_result in results:
        total_docs += len(query_result['ranking'])
        #rerank.annotate(query_result)
        #erank.random_shuffle(query_result)
        #rerank.disp_impact_balanced(query_result, rel_weight=r_weight, gender_weight=g_weight, country_weight=c_weight, per_query=True)
        rerank.disp_impact_KL(query_result, rel_weight=r_weight, gender_weight=g_weight, country_weight=c_weight, per_query=True)
        
        #for r in query_result['ranking']:
            #print(str(r['paper'].gender_dist) + ',' + str(r['paper'].country_dist))
        """
            if curr_csv_ind == len(csv_lines):
                csv_lines.append(',' + str(r['bm25_score']) + ',' + str(r['paper'].gender_score) + ',\n')
            else:
                csv_lines[curr_csv_ind] = csv_lines[curr_csv_ind][:-1]
                csv_lines[curr_csv_ind] += (',' + str(r['bm25_score']) + ',' + str(r['paper'].gender_score) + ',\n')
            curr_csv_ind += 1
        """
        rerank.de_annotate(query_result)
        #break

    print("done")
    """
    with open('data/query_rankings.csv', 'w') as fw:
        fw.writelines(csv_lines)
    """
    elapsed = time.time() - start
    print("Time Elapsed: " + str(elapsed))
    print("# of Queries Ranked: 200")
    print("# of Documents Ranked: " + str(total_docs))
    
    qid_map = {}
    for query in results:
        qid_map[query['qid']] = query
    
    with open(eval_seq, 'r') as eval_file:
        eval_lines = eval_file.readlines()
    prefix = 'retrieval_submissions/'
    # University of Washington, KL-Divergence retrieve, relevance, gender, country
    with open(prefix + 'UW_Kt_r' + str(int(r_weight * 100)) + 'g' + str(int(g_weight * 100)) + 'c' + str(int(c_weight * 100)) + '.jsonl', 'w', encoding='utf-8') as outfile:
        for line in eval_lines:
            seq = line.split(',')[0]
            qid = int(line.split(',')[1].strip())
            qid_map[qid]['q_num'] = seq
            json.dump(qid_map[qid], outfile, ensure_ascii=False)
            outfile.write('\n')
    
    