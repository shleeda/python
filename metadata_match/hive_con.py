import paramiko
import pandas as pd

def hive_col(db_name,table_name):

    col = []
    col_name = []

    ssh = paramiko.SSHClient()

    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh.connect('*.*.*.*', username='****', password='****')

    insert = "ssh hadoop@nn1 'hive -e \"desc " + db_name +"."+  table_name +";\"'"
    stdin, stdout, stderr = ssh.exec_command(insert)
    print("command : ", insert)

    for line in stdout:
        col.append(line)

    for i in (col[1:]):
        a=i.split()
        name=a[0]
        col_name.append(name)
        hive_column = pd.DataFrame(col_name, columns=["col_name"])

    ssh.close()

    return hive_column




