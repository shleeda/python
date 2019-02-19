import sys
sys.path.append("/usr/lib64/python3.5/site-packages")
sys.path.append("/usr/lib/python3.5/site-packages")
import pymysql
import pandas as pd

def result_insert(uid,df,table):
    db = pymysql.connect(host='192.168.0.102', user='lod_ui', password='lod!@',db='health_care_ui')
    cur = db.cursor()

    year_df = pd.read_sql("select yearName from tr_dataset_year where dataSetUID ="+uid+";",db)

    for i in range(0,len(df)):
        dataSetUID=str(uid)
        year=str(year_df['yearName'][i])
        statements=str(df['statements'][i])
        patients=str(df['patients'][i])
        total_expense=str(df['total_expense'][i])


        cur.execute("insert into "+ table +" (dataSetUID, year, statements, patients, total_expense) values ('%s','%s','%s','%s','%s');" %(dataSetUID, year, statements, patients, total_expense))
        db.commit() # 데이터가 실제로 기록됨

    #cur.execute("update tr_dataset_list set precessState = \'1\' where dataSetUID = "+uid)


    db.close()

def result_insert_koges(uid,df,table,snp,type_number):
    db =  pymysql.connect(host='192.168.0.102', user='lod_ui', password='lod!@',db='health_care_ui')
    cur = db.cursor()

    if type_number == "1":

        for i in range(0,len(df)):
            dataSetUID=str(uid)
            type = str(type_number)
            rs=str(snp)
            total=str(df['total'][i])
            male=str(df['male'][i])
            female=str(df['female'][i])

            cur.execute("insert into "+ table +" (dataSetUID, type, total, male, female) values ('%s','%s','%s','%s','%s');" %(dataSetUID, type, total, male, female))
            db.commit() # 데이터가 실제로 기록됨

    elif type_number == "2":

        for i in range(0,len(df)):
            dataSetUID=str(uid)
            type = str(type_number)
            rs=str(snp[i])
            total=str(df['total'][i])
            male=str(df['male'][i])
            female=str(df['female'][i])

            cur.execute("insert into "+ table +" (dataSetUID, type, rs, total, male, female) values ('%s','%s','%s','%s','%s','%s');" %(dataSetUID,type,rs,total, male, female))
            db.commit() # 데이터가 실제로 기록됨

    else :
        pass


        #cur.execute("update tr_dataset_list set precessState = \'1\' where dataSetUID = "+uid)


        db.close()


