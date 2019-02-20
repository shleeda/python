#ex) python compare_input.py chs chs_2013 chu_col_list_ref 6 6
#python compare_input.py [hive db이름] [hive 테이블 이름] [메타데이터 테이블이름] [edl_idx] [etl_idx]

import sys
import pandas as pd
import hive_con as hi
import mysql_con as my

def match_col():

    hive_not = []
    meta_not = []
    nrow=[]

    for i in range(0,len(hive)):
        for j in range(0,len(meta)):
            if hive['col_name'][i]==meta['ecl_eng_name'][j]:
                #print(i,j)
                #print(hive['col_name'][i],meta['ecl_eng_name'][j])
                hive_not.append(i)
                meta_not.append(j)
                nrow.append("1")

    hive_re = hive
    meta_re = meta

    for i in hive_not:
        hive_re = hive_re.drop(i,0)

    for j in meta_not:
        meta_re = meta_re.drop(j,0)

    print("hive not match ----------------\n",hive_re)
    print("meta not match ----------------\n",meta_re)

    return nrow

if __name__ == '__main__':
    hive = hi.hive_col(sys.argv[1],sys.argv[2])
    meta = my.metadata_col(sys.argv[3],sys.argv[4],sys.argv[5])
    nrow = match_col()
    print("hive ncol : ",len(hive))
    print("meta data ncol : ",len(meta))
    print("match nrow : ",len(nrow))