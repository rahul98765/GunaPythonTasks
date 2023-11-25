# import all libraires
from flask import Flask, render_template, request
import psycopg2
import pandas as pd
from datetime import datetime
import os, sys, codecs
#from flask_sqlalchemy import SQLAlchemy

# Initialize flask and create PostgreSQL database
app = Flask(__name__)

app.config['MAX_CONTENT_LENGTH']=1024*1024*1024

def getfilesize(filename):
   with open(filename,"rb") as fr:
       fr.seek(0,2)
       size=fr.tell()
       return fr.tell()

def splitfile(filename, splitsize, cur, conn):
   if not os.path.isfile(filename):
        return

   filesize=getfilesize(filename)
   with open(filename,"rb") as fr:
    counter=1
    orginalfilename = filename.split(".")
    readlimit = 500000
    n_splits = filesize//splitsize
    flag =1
    for i in range(n_splits+1):
        chunks_count = int(splitsize)//int(readlimit)
        data_5kb = fr.read(readlimit) 
        with open(orginalfilename[0]+"_{id}.".format(id=str(counter))+orginalfilename[1],"ab") as fw:
            fw.seek(0) 
            fw.truncate()
            while data_5kb:                
                fw.write(data_5kb)
                #outputdata = data_5kb.decode(encoding='latin-1')
                #outputdata = str()
                try:
                    if flag == 1:
                        cur.execute( "INSERT INTO test_od (filename, data) VALUES (%s, %s)", (filename, data_5kb))
                        #print("Inserted")
                        conn.commit() 
                        flag = 2
                    else:
                        #print("Update")
                        #query = "update test_od set data = CONCAT(data,CAST (" + data_5kb + " as text )) where filenmae = " + filename
                        query = "update test_od set data = CONCAT(data," + str(data_5kb) + " ) where filenmae = " + filename
                        #print(query)
                        cur.execute(query)
                        #cur.execute( "update test_od set data = CONCAT(data,convert(text," + data_5kb + " )) where filenmae = " + filename)
                        conn.commit()
                except (Exception, psycopg2.Error) as error:
                    print ("Error while inserting data to Database", error)
                    quit()
                if chunks_count:
                    chunks_count-=1
                    data_5kb = fr.read(readlimit)
                else: break   
        os.remove(orginalfilename[0]+"_{id}.".format(id=str(counter))+orginalfilename[1])   
        counter+=1 

def connecion():
    # Connect to the database 
    try:
        conn = psycopg2.connect(database="Task", user="postgres", 
                        password="root", host="localhost", port="5432", keepalives=0, keepalives_idle=30,
keepalives_interval=10,
keepalives_count=5)
        return conn
    except (Exception, psycopg2.Error) as error:
        print("Database connection Filed.", error)
        quit()        
  
# Create index function for upload and return files
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        try:

            #todayDate = date.today()
            
            #print(dateTimeFormat)
            
            file = request.files['file']
            
            #print(file.filename)
            conn = connecion()
            cur = conn.cursor()
        
            filedata = file.read()
            print(len(filedata))
            # Create table if not exsits
            cur.execute( "CREATE TABLE IF NOT EXISTS test_od (id serial PRIMARY KEY, filename varchar(100), data Bytea)") 
            
            splitfile(file.filename, 1024*1024, cur, conn)
            #cur.execute( "INSERT INTO test_od (filename, data) VALUES (%s, %s)", (file.filename, filedata))

            #cur.execute( "COPY test_od from 'gsoy_sample_csv.csv'")

            # dynamic table for each file data
            today = datetime.now()
            # dd_mm_YY
            dateTimeFormat = today.strftime("%Y_%m_%d_%H_%M_%S")
            dynmcTablename = "master_study_list"
            dynmcTablename = dynmcTablename + "_" + dateTimeFormat
            cur.execute( "CREATE TABLE " + dynmcTablename + " (id serial PRIMARY KEY, filename varchar(100), data Bytea)") 
            # Insert data into the table 
            #cur.execute( "INSERT INTO " + dynmcTablename + " (filename, data) VALUES (%s, %s)", (file.filename, compressed_data))
            cur.execute( "INSERT INTO " + dynmcTablename + " select * from test_od where filename=" + file.filename)
            # commit the changes 
            conn.commit() 
            # close the cursor and connection 
            cur.close() 
            conn.close() 
            return f'Uploaded file : {file.filename}'
        except (Exception, psycopg2.Error) as error:
            print ("Error while inserting data to Database", error)
            quit()
    return render_template('uploadlargefile.html')


if __name__ == '__main__':
    app.run(debug=True)

