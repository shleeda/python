import pymssql

db = pymssql.connect(host = '*.*.*.*' , user ='****' , password = '****' , database = '****')
cur = db.cursor()

def csv_import(filename):
    f=open(filename,"r",encoding='UTF8')
    lines=f.readlines() # 파일의 모든 라인을 읽어서 리스트로 리턴
    for i in lines:
        separate=i.split(",") # 구분자 설정
        c0=str(separate[0])
        c1=str(separate[1])
        c2=str(separate[2])
        c3=str(separate[3])
        c4=str(separate[4])
        c5=str(separate[5])
        c6=str(separate[6])
        c7=str(separate[7])
        c8=str(separate[8])
        c9=str(separate[9])
        c10=str(separate[10])
        c11=str(separate[11])
        c12=str(separate[12])

        print("('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" %(c0,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12))
        cur.execute("insert into medicine_master (c0,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12) values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" %(c0,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12))
        db.commit() # 데이터가 실제로 기록됨
        f.close()

    print("GOOD!! END!!!!")

csv_import("file location")

db.close()