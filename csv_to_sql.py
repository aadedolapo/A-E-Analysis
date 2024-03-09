import pandas as pd
import numpy as np

import psycopg2 as psy
# Load in csv data
csv_file = pd.read_csv('AAEDatasetCleaned.csv')

# load pgadmin4 credentials
with open('credentials.txt', 'r') as f:
    file = f.read()
exec(file)

cursor = None
conn = None

try:
    with psy.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS aae")

            script = """CREATE TABLE aae (
                            id      int PRIMARY KEY,
                            Place           varchar(10),
                            Provider_Name varchar(15), 
                            Age_Group varchar(10), 
                            GP_Practice_Name varchar(15),
                            Financial_Year varchar(10) ,
                            Financial_Month int,
                            Time_In_AandE varchar(10),
                            Arrival_Mode varchar(60),
                            Chief_Complaint varchar(50),
                            Discharge_Destination varchar(70),
                            Attendance_Count int)
                        """
            cursor.execute(script)

            for index, row in csv_file.iterrows():
                insert_data = """ INSERT INTO aae (id,Place,Provider_Name,Age_Group,
                                GP_Practice_Name,Financial_Year,Financial_Month,
                                Time_In_AandE,Arrival_Mode,Chief_Complaint,
                                Discharge_Destination,Attendance_Count)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                            """
                values = tuple(row)

                cursor.execute(insert_data, values)

            conn.commit()
except Exception as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
