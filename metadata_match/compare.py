import hive_con as hi
import mysql_con as my
import xlwt

def match_col():

    hive_not = []
    meta_not = []
    nrow=[]

    for i in range(0,len(hive)):
        for j in range(0,len(meta)):
            if hive['col_name'][i]==meta['ecl_eng_name'][j]:
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

    auto = my.auto_table()

    wbk = xlwt.Workbook() # 엑셀파일 생성
    new_sheet = wbk.add_sheet('sheet1') # 엑셀파일의 시트 생성

    for i in range(0,len(auto)):
        print(auto['edl_eng_name'][i],auto['etl_ref'][i],auto['edl_idx'][i],auto['etl_idx'][i])

        A = str(auto['edl_eng_name'][i])
        B = str(auto['etl_ref'][i])
        C = str(auto['edl_idx'][i])
        D = str(auto['etl_idx'][i])

        hive = hi.hive_col(A,B)
        meta = my.metadata_col("chu_col_list_ref",C,D)
        nrow = match_col()

        print("------------------------------------------------")
        print("hive ncol : ",len(hive))
        print("meta data ncol : ",len(meta))
        print("match nrow : ",len(nrow))
        print("------------------------------------------------")

        '''
        new_sheet.write(i,0,len(hive)) #1열에 하이브
        new_sheet.write(i,1,len(meta)) #2열에 메타데이터
        new_sheet.write(i,2,len(nrow)) #3열에 매칭
        wbk.save("D:\\match.xls") #엑셀 파일로 저장..
        '''


