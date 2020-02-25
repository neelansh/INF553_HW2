import os
import sys
from pyspark import SparkContext, SparkConf
import json
import itertools
import math
import time
import csv
from task1 import *


def extract_csv(file_names):
    for f in file_names:
        with open(f, 'rt') as file:
            reader = csv.reader(file)
            for i, line in enumerate(reader):
                if(i == 0):
                    continue
                yield line
                
def reading_baskets(input_file_path, case_number, filter_thresh):
    rdd = sc.textFile(input_file_path)
    header = rdd.first()
    
    if(case_number == 1):
        rdd = rdd.filter(lambda x: x != header).map(read_csv_line).groupByKey().filter(lambda x: len(x[1]) > filter_thresh).map(lambda x : (x[0], set(x[1])))
    else:
        rdd = rdd.filter(lambda x: x != header).map(read_csv_line).map(lambda x: (x[1], x[0])).groupByKey().filter(lambda x: len(x[1]) > filter_thresh).map(lambda x : (x[0], set(x[1])))
    return rdd


if __name__ == '__main__':
    appName = 'assignment2'
    master = 'local[*]'
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    
    st = time.time()
    
    # parameters
    case_number = 1
    filter_threshold = int(sys.argv[1].strip())
    support = int(sys.argv[2].strip())
    input_file = sys.argv[3].strip()
    output_file = sys.argv[4].strip()
    
    # pre processing
    rdd = sc.parallelize([input_file]).mapPartitions(extract_csv)
    header = rdd.first()

    rdd = rdd.map(lambda x: '{}-{},{}'.format(x[0], x[1], int(x[5]))).collect()

    with open("preprocessed_data.csv", "wt") as f:
        f.write("DATE-CUSTOMER_ID,PRODUCT_ID\n")
        for i, l in enumerate(rdd):    
            f.write(l+"\n")
    
    
    baskets_rdd = reading_baskets("preprocessed_data.csv", case_number, filter_threshold)
    baskets_rdd = baskets_rdd.repartition(16)
    total_len = baskets_rdd.count()
#     print(total_len, baskets_rdd.getNumPartitions())
    phase1 = baskets_rdd.mapPartitions(lambda x: apriori_son(x, support, total_len))
    candidates = list(set(phase1.collect()))
    phase2 = baskets_rdd.mapPartitions(lambda x: phase2_count(x, candidates)).reduceByKey(lambda a, b: a+b).filter(lambda x: x[1] >= support)
    frequent = phase2.collect()
    
    
    o_c, o_f = prepare_output(candidates, frequent)
    save_output(o_c, o_f, output_file)
    
    print("Duration: {:.5f}".format(time.time()-st))