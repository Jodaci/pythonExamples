import cx_Oracle
import datetime as dt
import pandas as pd

# connection string in the format
# <username>/<password>@<dbHostAddress>:<dbPort>/<dbServiceName>
connStr = 'system/pass@localhost:1521/xepdb1'

# initialize the connection object
conn = None
try:
    # create a connection object
    conn = cx_Oracle.connect(connStr)

    # get a cursor object from the connection
    cur = conn.cursor()

    # create a sample dataframe
    # dataDf = pd.DataFrame(columns=["ST_NAME","DOB","STUDENTID"],
    #                       data=[['xyz', dt.datetime(2021, 1, 1), 7654],
    #                             ['abc', dt.datetime(2020, 10, 12), 9724]])
    
    # read dataframe from excel
    dataDf = pd.read_excel("cx_oracle_insert_data.xlsx")
    # print(dataDf.columns)
    
    # reorder the columns as per the requirement
    dataDf = dataDf[["ST_NAME","DOB","STUDENTID"]]

    # prepare data insertion rows from dataframe
    dataInsertionTuples = [tuple(x) for x in dataDf.values]
    print(dataInsertionTuples)

    # create sql for deletion of existing rows to avoid insert conflicts
    sqlTxt = 'DELETE from "test1".students where\
                (st_name=:1 and dob=:2)\
                or (studentid=:3)'
    # execute the sql to perform deletion
    cur.executemany(sqlTxt, [x for x in dataInsertionTuples])

    rowCount = cur.rowcount
    print("number of existing rows deleted =", rowCount)

    # create sql for data insertion
    sqlTxt = 'INSERT INTO "test1".students\
                (st_name, dob, studentid)\
                VALUES (:1, :2, :3)'
    # execute the sql to perform data extraction
    cur.executemany(sqlTxt, dataInsertionTuples)

    rowCount = cur.rowcount
    print("number of inserted rows =", rowCount)

    # commit the changes
    conn.commit()
except Exception as err:
    print('Error while inserting rows into db')
    print(err)
finally:
    if(conn):
        # close the cursor object to avoid memory leaks
        cur.close()

        # close the connection object also
        conn.close()
print("data insert example execution complete!")
