import os
import sys
from pyspark import SparkContext, SparkConf
import json
import itertools
import math
import time

def read_csv_line(line):
    line = line.split(',')
    return (line[0].strip(), line[1].strip())

def reading_baskets(input_file_path, case_number):
    rdd = sc.textFile(input_file_path)
    header = rdd.first()
    
    if(case_number == 1):
        rdd = rdd.filter(lambda x: x != header).map(read_csv_line).groupByKey().map(lambda x : (x[0], set(x[1])))
    else:
        rdd = rdd.filter(lambda x: x != header).map(read_csv_line).map(lambda x: (x[1], x[0])).groupByKey().map(lambda x : (x[0], set(x[1])))
    return rdd

# def gen_candidates(baskets, frequent_items, k):
#     candidates = []
#     if(k == 1):
#         for _, b in baskets:
#             for c in itertools.combinations(b, k):
#                 candidates.append(frozenset(c))
                
#         return list(set(candidates))
    
#     for _, b in baskets:
#         for c in itertools.combinations(b, k):
#             sub_combinations = set(frozenset(x) for x in itertools.combinations(c, k-1))
#             if(sub_combinations.issubset(set(frequent_items))):
#                 candidates.append(frozenset(c))
                
#     return list(set(candidates))

def gen_candidates(baskets, frequent_items, k):
    candidates = set()
    if(k == 1):
        for _, b in baskets:
            for c in itertools.combinations(b, k):
                c = tuple(sorted(c))
                candidates.add(c)
                
        return candidates
    
#     for _, b in baskets:
#         for c in itertools.combinations(b, k):
    for s1, s2 in itertools.combinations(frequent_items, 2):
        c = set(s1).union(set(s2))
        if(len(c) == k):
            c = tuple(sorted(c))
            candidates.add(c)
            
    return candidates


# def gen_candidates(baskets, frequent_items, k):
#     candidates = []
#     if(k == 1):
#         for _, b in baskets:
#             for c in itertools.combinations(b, k):
#                 c = tuple(sorted(c))
#                 candidates.append(c)
                
#         return list(set(candidates))
    
# #     for _, b in baskets:
# #         for c in itertools.combinations(b, k):
#     for s1, s2 in itertools.combinations(frequent_items, 2):
#         c = set(s1).union(set(s2))
#         if(len(c) == k):
#             c = tuple(sorted(c))
#             candidates.append(c)
            
#     return list(set(candidates))
    
    
def calculate_support(baskets, candidates):
    counter = {}
#     k = len(candidate)
#     candidate = set(candidate)
    for _, b in baskets:
        for c in candidates:
            if(set(c).issubset(b)):
                if(c in counter):
                    counter[c] += 1
                else:
                    counter[c] = 1
#         b = set(b)
#         combinations = set(frozenset(x) for x in itertools.combinations(b, k))
#         if(candidate.issubset(b)):
#             count += 1
    return counter

# def calculate_support(baskets, k):
#     counter = {}
#     for _, b in baskets:
# #         b = list(b)
#         combinations = [frozenset(x) for x in itertools.combinations(b, k)]
#         for c in combinations:
#             if(c in counter):
#                 counter[c] += 1
#             else:
#                 counter[c] = 1
#     return counter


def apriori(baskets, sup):
    k = 1
    candidates = gen_candidates(baskets, [], 1)
    output = []
    while(len(candidates) != 0):
        frequent_items = []
        s1 = time.time()
        sup_count = calculate_support(baskets, candidates)
        for candidate, count in sup_count.items():
            if(count >= sup):
                frequent_items.append(candidate)
        s1 = time.time()-s1
#         output['candidate'][k] = candidates
        output += frequent_items
        k += 1
        s2 = time.time()
        candidates = gen_candidates(baskets, frequent_items, k)
        s2 = time.time()-s2
#         print(k, s1, s2)
        
    return list(set(output))



def apriori_son(baskets_iterator, sup, total_len):
    baskets = list(baskets_iterator)
    partition_sup = math.ceil(sup * len(baskets) / total_len)
#     yield (partition_sup, len_partition, total_len)
    result = apriori(baskets, partition_sup)
    for x in result:
        yield x

    
def phase2_count(basket_iterator, candidates):
    baskets = [x for x in basket_iterator]
    sup_count = calculate_support(baskets, candidates)
    for candidate, count in sup_count.items():
        yield (candidate, count)
        
        
def prepare_output(candidates, frequent):
    output_candidate = {}
    for item in candidates:
        item = tuple(sorted(tuple(item)))
        if(len(item) in output_candidate):
            output_candidate[len(item)].append(item)
        else:
            output_candidate[len(item)] = [item]
    
    output_frequent = {}
    for item, _ in frequent:
        item = tuple(sorted(tuple(item)))
        if(len(item) in output_frequent):
            output_frequent[len(item)].append(item)
        else:
            output_frequent[len(item)] = [item]
            
    for k, v in output_candidate.items():
        output_candidate[k] = sorted(v)
        
    for k, v in output_frequent.items():
        output_frequent[k] = sorted(v)
        
        
    for k, v in output_candidate.items():
        if(k == 1):
            output_candidate[k] = ",".join(["('{}')".format(x[0]) for x in v])
            continue
        output_candidate[k] = ",".join([str(x) for x in v])
        
        
    for k, v in output_frequent.items():
        if(k == 1):
            output_frequent[k] = ",".join(["('{}')".format(x[0]) for x in v])
            continue
        output_frequent[k] = ",".join([str(x) for x in v])
    
            
    return output_candidate, output_frequent


def save_output(output_candidate, output_frequent, output_path):
    with open(output_path, 'wt') as file:
        file.write("Candidates:\n")
        for k in sorted(output_candidate.keys()):
            file.write(output_candidate[k]+'\n')
            file.write('\n')
        file.write("Frequent Itemsets:\n")
        for k in sorted(output_frequent.keys()):
            file.write(output_frequent[k]+'\n')
            file.write('\n')
                        

if __name__ == '__main__':
    appName = 'assignment2'
    master = 'local[*]'
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    
    st = time.time()
    
    case_number = int(sys.argv[1].strip())
    support = int(sys.argv[2].strip())
    input_file = sys.argv[3].strip()
    output_file = sys.argv[4].strip()
    
    baskets_rdd = reading_baskets(input_file, case_number)
#     baskets_rdd.persist()
    total_len = baskets_rdd.count()
    phase1 = baskets_rdd.mapPartitions(lambda x: apriori_son(x, support, total_len))
    candidates = list(set(phase1.collect()))
    
    phase2 = baskets_rdd.mapPartitions(lambda x: phase2_count(x, candidates)).reduceByKey(lambda a, b: a+b).filter(lambda x: x[1] >= support)
    frequent = phase2.collect()
    
    
    o_c, o_f = prepare_output(candidates, frequent)
    save_output(o_c, o_f, output_file)
    
    print("Duration: {:.5f}".format(time.time()-st))