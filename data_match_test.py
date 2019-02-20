import paramiko
import MySQLdb
import pandas as pd

def hive_col(db_name,table_name):

    col = []
    col_name = []

    ssh = paramiko.SSHClient()

    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh.connect('*.*.*.*', username='****', password='****')

    insert = "ssh hadoop@nn1 'hive -e \"desc " + db_name +"."+  table_name +";\"'"
    stdin, stdout, stderr = ssh.exec_command(insert)
    print(insert)

    for line in stdout:
        col.append(line)

    for i in (col[1:]):
        a=i.split()
        name=a[0]
        col_name.append(name)
        hive_column = pd.DataFrame(col_name, columns=["col_name"])

    return hive_column
    ssh.close()

def metadata_col(table_name, edl_idx,etl_idx):

    db = MySQLdb.connect("*.*.*.*","****","****","****" )
    db.set_character_set('utf8')
    cur = db.cursor()

    print("select ecl_eng_name from "+table_name+" where edl_idx="+edl_idx+" and etl_idx="+etl_idx)
    meta_col = pd.read_sql("select ecl_eng_name from "+table_name+" where edl_idx ="+edl_idx+" and etl_idx ="+etl_idx, db)

    return meta_col

    db.close()

def match_col():
    global nrow
    nrow=[]
    for i in range(0,len(hive)):
        for j in range(0,len(meta)):
            if hive['col_name'][i]==meta['ecl_eng_name'][j]:
                print(hive['col_name'][i],meta['ecl_eng_name'][j])
                nrow.append("1")

if __name__ == '__main__':
    hive = hive_col("chs","chs_2009")
    meta = metadata_col("chu_col_list","6","2")
    print("\n")
    match_col()
    print(len(hive), len(meta), len(nrow))


