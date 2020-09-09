# Reranking utilities to prioritize fairness or relevance, or some combination
# of the two

import random
from scipy import special
# example input: {"q_num": "0.2", "qid": 89123, "ranking": ["d6b48754da936d0689a4fd1e2077b4c4610cb9cc", "d1cfb3e06f83bc01118f87c42ef0b411f7e67a50", "9f17759fbc55555d72d306dbb3f0a33d111524d7", "773b48e7b40d9a964b46caafc6a0a2efd2f70f73", "62b19468ecf35e1f730f2470eed8790cf05210e1", "22b8733be3e6c170253782a54c99b7a1efcb701f", "0c9fb65d4e56bbfc9519b49721f300284cfbbf71", "0acdf697d9b91d8bf0d3acdd3d7461afc800a6c0", "094e84c894490313f3e7792c43d816f5088262bd", "067543b6317bb0ab94bb9b90b9cb6f523d44d2f6"]}

# maintain order, clear all data except doc_id
def de_annotate(data):
    ranking_list = data['ranking']
    just_doc_ids = []
    for item in ranking_list:
        just_doc_ids.append(item['doc_id'])
    data['ranking'] = just_doc_ids

def random_shuffle(data):
    random.shuffle(data['ranking'])

# Takes relevance value (bm25 score) max score for the list, and min score
# for the list and normalizes relevance value to lie between 0 and 1, with 0
# being the most relevant
def normalize_relevance(value, max_r, min_r=0):
    min_r = max(min_r, 0)
    max_r = max(max_r, 0)
    if value == -1 or max_r - min_r == 0:
        return 1
    else:
        return (max_r - value) / (max_r - min_r)
        
# Returns the 'cost' (higher = less desirable) of adding a document with author
# group distributions defined in doc_list to the current ranked list of documents
def cost_KL(relevance, freq_lists, ideal_proportions_lists, weight_list, doc_list):
    cost = weight_list[0] * relevance #relevance is normalized between 0 and 1
    proportion_lists = []
    new_freq_lists = []
    for i in range(len(freq_lists)):
        new_freq_lists.append(freq_lists[i].copy())
        for j in range(len(new_freq_lists[i])):
            new_freq_lists[i][j] += doc_list[i][j]
            
    for fl in new_freq_lists:
        pl = []
        for freq in fl:
            if sum(fl) > 0:
                pl.append(freq / sum(fl))
            else:
                pl.append(freq)
        proportion_lists.append(pl)
    #print("Doc List: " + str(doc_list))
    #print("New Freq Lists: " + str(new_freq_lists))
    #print("Prop lists: " + str(proportion_lists))
    for i, pl in enumerate(proportion_lists):
        #print("rel_entr for " + str(pl) + str(sum(special.rel_entr(pl, ideal_proportions_lists[i]))))
        #print("weight: " + str(weight_list[i + 1]))
        cost += sum(special.rel_entr(pl, ideal_proportions_lists[i])) * weight_list[i + 1] # +1 since relevance is weight[0]
    #print("Cost: " + str(cost) + "\n") 
    return cost

# Reranks document list in 'data' to minimize 'cost' at each position in the new
# doc_list. Weight parameters describe how much each parameter affects the cost
# evaluation. per_query determines whether ideal proportions are determined on a
# per-query basis or a database-wide basis.
def disp_impact_KL(data, rel_weight=0.34, gender_weight=0.33, country_weight=0.33, per_query=True):
    ranking_list = data['ranking']
    new_rl = []
    freqs = [[0,0], [0,0]] # gender, country
    if per_query:
        q_freq_gender = [0,0]
        q_freq_country = [0,0]
        q_num_authors = 0
        for doc in ranking_list:
            for i, val in enumerate(q_freq_gender):
                q_freq_gender[i] += doc['paper'].gender_dist[i]
            for i, val in enumerate(q_freq_country):
                q_freq_country[i] += doc['paper'].country_dist[i]
            q_num_authors += len(doc['paper'].authors)
        
        
        ideal_gender_proportions = [q_freq_gender[0] / q_num_authors, q_freq_gender[1] / q_num_authors]
        ideal_country_proportions = [q_freq_country[0] / q_num_authors, q_freq_country[1] / q_num_authors]
        ideals = [ideal_gender_proportions, ideal_country_proportions] # overall
        if ideal_gender_proportions[0] in [1, 0]:
            print("ONE G")
        if ideal_country_proportions[0] in [1, 0]:
            print("ONE C")
    else:
        prop_male = 0.74389
        prop_advanced = 0.79330
        ideals = [[prop_male, 1 - prop_male], [prop_advanced, 1 - prop_advanced]]

    weights = [rel_weight, gender_weight, country_weight]
    #print(weights)
    
    max_rel = ranking_list[0]['bm25_score']
    for doc in ranking_list:
        if doc['bm25_score'] == -1:
            doc['bm25_score'] = 0
    min_rel = ranking_list[-1]['bm25_score']
    
    while len(ranking_list) > 0:
        # search
        min_cost_ind = 0
        first_freqs = [ranking_list[0]['paper'].gender_dist, ranking_list[0]['paper'].country_dist]
        min_cost = cost_KL(normalize_relevance(ranking_list[0]['bm25_score'], max_rel, min_rel), freqs, ideals, weights, first_freqs)
        for i, doc in enumerate(ranking_list[1:]):
            #print("min_cost: " + str(min_cost) + " min_cost_ind: " + str(min_cost_ind) + " min_cost_country: " + str(ranking_list[min_cost_ind]['paper'].country_dist))

            doc_freqs = [doc['paper'].gender_dist, doc['paper'].country_dist]
            doc_cost = cost_KL(normalize_relevance(doc['bm25_score'], max_rel, min_rel), freqs, ideals, weights, doc_freqs)
            if doc_cost < min_cost:
                min_cost = doc_cost
                min_cost_ind = i + 1
            
        next_doc = ranking_list.pop(min_cost_ind)
        
        for i, val in enumerate(freqs[0]):
            freqs[0][i] += next_doc['paper'].gender_dist[i]
        for i, val in enumerate(freqs[1]):
            freqs[1][i] += next_doc['paper'].country_dist[i]
        #print("Proportions: " + str(freqs[0][0] / sum(freqs[0])))
        #print("Doc Freqs: " + str(next_doc['paper'].gender_dist) + str(next_doc['paper'].country_dist))
        #print("Freqs: " + str(freqs))
        new_rl.append(next_doc)
        #break

    data['ranking'] = new_rl

""" Various previous implementations of reranking

def split_none_gender(data):
    ranking_list = data['ranking']
    valid = []
    none = []
    for item in ranking_list:
        if item['paper'].gender_score is None:
            none.append(item)
        else:
            valid.append(item)
    return [valid, none]
    
def split_none_country(data):
    ranking_list = data['ranking']
    valid = []
    none = []
    for item in ranking_list:
        if item['paper'].country_score is None:
            none.append(item)
        else:
            valid.append(item)
    return [valid, none]


def pure_gender(data):
    comps = split_none_gender(data)
    male_biased = []
    female_biased = []
    none = comps[1]
    for item in sorted(comps[0], key=lambda i: abs(i['paper'].gender_score)):
        if item['paper'].gender_score < 0:
            male_biased.append(item)
        else:
            female_biased.append(item)
    
    merged = []
    curr_gender_sum = 0
    while len(male_biased) > 0 and len(female_biased) > 0:
        if curr_gender_sum < 0:
            merged.append(female_biased.pop(0))
        else:
            merged.append(male_biased.pop(0))
            
        curr_gender_sum += merged[-1]['paper'].gender_score
    
    if len(male_biased) > 0:
        merged.extend(male_biased)
    if len(female_biased) > 0:
        merged.extend(female_biased)
    if len(none) > 0:
        merged.extend(none)
    data['ranking'] = merged
        
def pure_country(data):
    comps = split_none_country(data)
    male_biased = []
    female_biased = []
    none = comps[1]
    for item in sorted(comps[0], key=lambda i: abs(i['paper'].country_score)):
        if item['paper'].country_score < 0:
            male_biased.append(item)
        else:
            female_biased.append(item)
    
    merged = []
    curr_gender_sum = 0
    while len(male_biased) > 0 and len(female_biased) > 0:
        if curr_gender_sum < 0:
            merged.append(female_biased.pop(0))
        else:
            merged.append(male_biased.pop(0))
            
        curr_gender_sum += merged[-1]['paper'].country_score
    
    if len(male_biased) > 0:
        merged.extend(male_biased)
    if len(female_biased) > 0:
        merged.extend(female_biased)
    if len(none) > 0:
        merged.extend(none)
    data['ranking'] = merged

def normalize_gc(value, target_score):
    diff = value - target_score
    highest_poss = 1 + abs(target_score)
    return abs(diff) / highest_poss
    
def sp_score(item, rel_weight, gender_weight, country_weight, max_r, min_r=0):
    # everything will have relevance
    rel_score = normalize_relevance(item['bm25_score'], max_r, min_r)
    output = 1
    if item['paper'].gender_score is None and item['paper'].country_score is None:
        output = rel_score
    elif item['paper'].gender_score is None:
        country_weight += gender_weight
        output = (abs(item['paper'].country_score) * country_weight + rel_score * rel_weight)
    elif item['paper'].country_score is None:
        gender_weight += country_weight
        output = (abs(item['paper'].gender_score) * gender_weight + rel_score * rel_weight)
    else:
        output = (abs(item['paper'].gender_score) * gender_weight + abs(item['paper'].country_score) * country_weight + rel_score * rel_weight)
    
    if output > 1:
        print(output)
    elif output < 0:
        print(output)
    return output

def di_score_null_zero(item, rel_weight, gender_weight, country_weight, max_r, min_r=0):
    #15106 advanced, 3936 developing, 12933 unidentified
    #18810 male, 6235 female, 6930 unidentified
    target_gender_score = -0.5021
    target_country_score = -0.5866
    # everything will have relevance
    rel_score = normalize_relevance(item['bm25_score'], max_r, min_r)
    if item['paper'].gender_score is None:
        item['paper'].gender_score = 0
    if item['paper'].country_score is None:
        item['paper'].country_score = 0
    
    return (normalize_gc(item['paper'].gender_score, target_gender_score) * gender_weight + normalize_gc(item['paper'].country_score, target_country_score) * country_weight + rel_score * rel_weight)
    
def di_score(item, rel_weight, gender_weight, country_weight, max_r, min_r=0):
    #15106 advanced, 3936 developing, 12933 unidentified
    #18810 male, 6235 female, 6930 unidentified
    target_gender_score = -0.5021
    target_country_score = -0.5866
    # everything will have relevance
    rel_score = normalize_relevance(item['bm25_score'], max_r, min_r)
    output = 1
    if item['paper'].gender_score is None and item['paper'].country_score is None:
        output = rel_score
    elif item['paper'].gender_score is None:
        country_weight += gender_weight
        output = normalize_gc(item['paper'].country_score, target_country_score) * country_weight + rel_score * rel_weight
    elif item['paper'].country_score is None:
        gender_weight += country_weight
        output = normalize_gc(item['paper'].gender_score, target_gender_score) * gender_weight + rel_score * rel_weight
    else:
        output = normalize_gc(item['paper'].gender_score, target_gender_score) * gender_weight + normalize_gc(item['paper'].country_score, target_country_score) * country_weight + rel_score * rel_weight
    
    if output > 1:
        print(output)
    elif output < 0:
        print(output)
    return output
    
def statistical_parity(data, rel_weight=0.34, gender_weight=0.33, country_weight=0.33):
    ranking_list = data['ranking']
    #for i in ranking_list:
    #    print(i)
    
    max_rel = ranking_list[0]['bm25_score'] # assuming bm25 has been applied
    min_ind = len(ranking_list) - 1
    min_rel = ranking_list[min_ind]['bm25_score']
    while min_ind > 0 and min_rel < 0:
        min_ind -= 1
        min_rel = ranking_list[min_ind]['bm25_score']
    
    ranking_list.sort(key=lambda i: sp_score(i, rel_weight, gender_weight, country_weight, max_rel, min_rel))
    
    #for i in ranking_list:
    #    print(i)
    data['ranking'] = ranking_list

def disparate_impact(data, rel_weight=0.34, gender_weight=0.33, country_weight=0.33):
    ranking_list = data['ranking']
    #for i in ranking_list:
    #    print(i)
    
    max_rel = ranking_list[0]['bm25_score'] # assuming bm25 has been applied
    min_ind = len(ranking_list) - 1
    min_rel = ranking_list[min_ind]['bm25_score']
    while min_ind > 0 and min_rel < 0:
        min_ind -= 1
        min_rel = ranking_list[min_ind]['bm25_score']
    
    ranking_list.sort(key=lambda i: di_score(i, rel_weight, gender_weight, country_weight, max_rel, min_rel))
    #ranking_list.sort(key=lambda i: di_score_null_zero(i, rel_weight, gender_weight, country_weight, max_rel, min_rel))
    
    #for i in ranking_list:
    #    print(i)
    data['ranking'] = ranking_list

def cost(sum_list, ideal_list, weight_list, doc_list, none_value):
    cost = 0
    for i in range(len(sum_list)):
        if doc_list[i] is None:
            doc_list[i] = none_value
        cost += abs(ideal_list[i] - sum_list[i] - doc_list[i]) * weight_list[i]
    return cost

def disp_impact_balanced(data, rel_weight=0.34, gender_weight=0.33, country_weight=0.33, per_query=True):
    # we want none_value to be the farthest away from ideal.
    # since ideal will predominantly be below 0 for both gender and country scores,
    # we set none_value to 1
    
    none_value = 1
    ranking_list = data['ranking']
    new_rl = []
    sums = [0, 0, 0] # relevance, gender, country
    if per_query:
        q_sum_gender = 0
        q_sum_country = 0
        q_num_authors = 0
        for doc in ranking_list:
            if doc['paper'].gender_score is not None:
                q_sum_gender += doc['paper'].gender_score * len(doc['paper'].authors)
            else:
                q_sum_gender += none_value * len(doc['paper'].authors)

            if doc['paper'].country_score is not None:
                q_sum_country += doc['paper'].country_score * len(doc['paper'].authors)
            else:
                q_sum_country += none_value * len(doc['paper'].authors)
            q_num_authors += len(doc['paper'].authors)
        
        ideal_q_gender = q_sum_gender / q_num_authors
        ideal_q_country = q_sum_country / q_num_authors
        ideals = [0, ideal_q_gender, ideal_q_country] # overall
        #print(ideals)
        ideal_sums = [0, ideal_q_gender, ideal_q_country]
    else:
        ideals = [0, -0.5021, -0.5866]
        ideal_sums = [0, -0.5021, -0.5866]
        
    weights = [rel_weight, gender_weight, country_weight]
    #print(weights)
    
    max_rel = ranking_list[0]['bm25_score']
    for doc in ranking_list:
        if doc['bm25_score'] == -1:
            doc['bm25_score'] = 0
    min_rel = ranking_list[-1]['bm25_score']
    
    while len(ranking_list) > 0:
        # search
        min_cost_ind = 0
        min_cost = cost(sums, ideal_sums, weights, [normalize_relevance(ranking_list[0]['bm25_score'], max_rel, min_rel), ranking_list[0]['paper'].gender_score, ranking_list[0]['paper'].country_score], none_value)
        for i, doc in enumerate(ranking_list[1:]):
            #print("min_cost: " + str(min_cost) + " min_cost_ind: " + str(min_cost_ind) + " min_cost_gend: " + str(ranking_list[min_cost_ind]['paper'].gender_score))

            doc_list = [normalize_relevance(doc['bm25_score'], max_rel, min_rel), doc['paper'].gender_score, doc['paper'].country_score]
            doc_cost = cost(sums, ideal_sums, weights, doc_list, none_value)
            if doc_cost < min_cost:
                min_cost = doc_cost
                min_cost_ind = i + 1
            
        next_doc = ranking_list.pop(min_cost_ind)
        
        sums[0] += normalize_relevance(next_doc['bm25_score'], max_rel, min_rel)
        if next_doc['paper'].gender_score is not None:
            sums[1] += next_doc['paper'].gender_score
        if next_doc['paper'].country_score is not None:
            sums[2] += next_doc['paper'].country_score
    
        #print(str(sums) + ' ideal:' + str(ideal_sums))
        for i in range(len(ideal_sums)):
            ideal_sums[i] += ideals[i]
            
        new_rl.append(next_doc)

    data['ranking'] = new_rl
    
    
def promote_binary_relevance(ranking_list, to_index=-1):
    if to_index == -1:
        to_index = len(ranking_list)
    
    sublist = ranking_list[:to_index]
    zeroes = []
    ones = []
    for item in sublist:
        if 'relevance' in item and item['relevance'] == 1:
            ones.append(item)
        else:
            zeroes.append(item)
            
    ones.extend(zeroes)
    ones.extend(ranking_list[to_index:])
    
    return ones
"""