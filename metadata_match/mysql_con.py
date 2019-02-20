import MySQLdb
import pandas as pd

def metadata_col(table_name, edl_idx,etl_idx):

    db = MySQLdb.connect("*.*.*.*","****","****","****")
    db.set_character_set('utf8')

    print("Query : ", "select ecl_eng_name from "+table_name+" where edl_idx="+edl_idx+" and etl_idx="+etl_idx)
    meta_col = pd.read_sql("select ecl_eng_name from "+table_name+" where edl_idx ="+edl_idx+" and etl_idx ="+etl_idx, db)

    db.close()

    return meta_col

def auto_table():

    db = MySQLdb.connect("*.*.*.*","****","****","****")
    db.set_character_set('utf8')

    auto_table = pd.read_sql("select * from auto_match", db)

    db.close()

    return auto_table
