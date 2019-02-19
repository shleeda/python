import sys
import os
import pandas as pd
import re
sys.path.append("/usr/lib64/python3.5/site-packages")
sys.path.append("/usr/lib/python3.5/site-packages")


def hive_query(command):

    col = []
    insert = "ssh hadoop@nn1 \'hive -e \"" + command + ";\"\'"
    result = os.popen(insert).read()

    a = result.split("\n")

    for line in range(0,len(a)):
        col.append(line)
    col.pop()

    hive_re = pd.DataFrame(columns=('year', 'statements', 'patients', 'total_expense'))

    for i in col[1:]:
        a=i.split()
        year = a[0]
        statements = a[1]
        patients = a[2]
        total_expense = a[3]
        hive_re = hive_re.append({'year' : year, 'statements' : statements, 'patients' : patients, 'total_expense' : total_expense}, ignore_index=True)

    return hive_re


def hive_query_koges_epi(command):

    col = []
    insert = "ssh hadoop@nn1 \'hive -e \"" + command + ";\"\'"
    result = os.popen(insert).read()

    a = result.split("\n")

    for line in range(0,len(a)):
        col.append(line)
    col.pop()

    hive_re = pd.DataFrame(columns=('total', 'male', 'female'))

    for i in result[1:]:
        a=i.split()
        total = a[0]
        male = a[1]
        female = a[2]
        hive_re = hive_re.append({'total' : total, 'male' : male, 'female' : female}, ignore_index=True)

    return hive_re

def koges_snp_name(command):

    col = []
    col_name = []

    insert = "ssh hadoop@nn1 \'hive -e \"" + command + ";\"\'"

    result = os.popen(insert).read()

    a = result.split("\n")

    for line in range(0,len(a)):
        col.append(line)
    col.pop()

    p = re.compile('^rs+')

    for i in col[1:]:
        a=i.split()
        name = a[0]
        m = p.match(name)
        if m :
            col_name.append(name)
        else :
            pass

    return col_name


def koges_snp_result(col_name,db, table):

    col = []

    for i in range(0,len(col_name),2):

        command_snp = "select sum(if("+col_name[i]+"!=\'\"\'0\'\"\',1,0)) as total, sum(if("+col_name[i]+"!=\'\"\'0\'\"\' and a00_sex=1,1,0)) as male, sum(if("+col_name[i]+"!=\'\"\'0\'\"\' and a00_sex=2,1,0)) as female from "+db+"."+table
        insert = "ssh hadoop@nn1 \'hive -e \"" + command_snp + ";\"\'"
        #print(insert)
        result = os.popen(insert).read()
        a = result.split("\n")
        for line in range(0,len(a)):
            col.append(line)
        col.pop()

    #print(result)

    hive_re = pd.DataFrame(columns=('total', 'male', 'female'))

    for i in col[2*i+1:]:
        a=i.split()
        total = a[0]
        male = a[1]
        female = a[2]
        hive_re = hive_re.append({'total' : total, 'male' : male, 'female' : female}, ignore_index=True)

    return hive_re
