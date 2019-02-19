 #-*-coding:UTF-8-*-
import xlrd
import MySQLdb

db = MySQLdb.connect("*.*.*.*","****","****","****" )
db.set_character_set('utf8')
cur = db.cursor()

def xlsx_import(filename):

    book = xlrd.open_workbook(filename)
    sheet = book.sheet_by_name("****")
    columns = str(sheet.ncols)
    rows = str(sheet.nrows)

    for r in range(1, sheet.nrows):
        eol_idx = str(sheet.cell(r,0).value)
        edl_idx = str(sheet.cell(r,1).value)
        esl_idx = str(sheet.cell(r,2).value)
        eol_value = str(sheet.cell(r,3).value)
        eol_text = str(sheet.cell(r,4).value)
        eol_order_by = str(sheet.cell(r,5).value)

        eol_value= round(float(eol_value))

        print("DATA: ('%s','%s','%s','%s','%s','%s')" %(eol_idx, edl_idx, esl_idx, eol_value, eol_text, eol_order_by))
        cur.execute("insert into chu_filter_option (eol_idx, edl_idx, esl_idx, eol_value, eol_text, eol_order_by) values (%s,%s,%s,'%s','%s',%s);" %(eol_idx, edl_idx, esl_idx, eol_value, eol_text, eol_order_by))
        db.commit() # 데이터가 실제로 기록됨

    print (columns + "columns and " + rows + "rows complete")
    print ("good end!")

xlsx_import("file location")

db.close()