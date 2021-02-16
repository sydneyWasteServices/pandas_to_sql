from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float, Date, Time, SmallInteger, DateTime, Text
import sqlalchemy
import pandas as pd
import pyodbc
import pymssql
# meta = MetaData()

engine = create_engine('mssql+pymssql://SW-SRV1\\ATSSQLEXPRESS/OLTP_SOURCE_COPY_TEST')

df = pd.read_csv("../../../dataVault/booking/27.1.2021_2.2.2021.csv", dtype={"Schd Time Start" : str, "PO" : str})

df = df.fillna('NULL')

# Float(asdecimal=True)
# precision=3 
# 
# It means they definitly have 00.000  has three decimal point even it is zero 
try :
    df.to_sql(name="staging_table_test", con=engine, dtype={
        'Job No': Float(precision=5),
        'Date': Date(), 
        'Schd Time Start': Time(),
        'Schd Time End': Time(),
        'Latitude': Float(precision=7, asdecimal=True),
        'Longitude': Float(precision=7, asdecimal=True),
        'Customer number': Float(precision=4),
        'Customer Name': String(length=800),
        'Site Name': String(length=800),
        'Address 1': String(length=1200),
        'Address 2': String(length=1200),
        'City': String(length=500),
        'State': String(length=30),
        'PostCode': Integer(),
        'Zone': String(length=500),
        'Phone': String(length=600),
        'Qty Scheduled': SmallInteger(),
        'Qty Serviced': SmallInteger(),
        'Serv Type': String(length=600),
        'Container Type': String(length=20),
        'Bin Volume': Float(precision=5),
        'Status': String(length=5),
        'Truck number': String(length=50),
        'Route number': String(length=50),
        'Generate ID': String(length=500),
        'Initial Entry Date': DateTime(),
        'Weight': Float(precision=5),
        'Prorated Weight': Float(precision=5),
        'Booking Reference 1': String(length=200),
        'Booking Reference 2': String(length=200),
        'Alternate Ref No 1': String(length=200),
        'Alternate Ref No 2': String(length=200),
        'Alternate Service Ref 1': String(length=200),
        'Alternate Service Ref 2': String(length=200),
        'Notes': Text(length=8000),
        'Directions': Text(length=8000),
        'CheckLists': String(length=300),
        'Waste Type': String(length=350),
        'Tip Site': String(length=450), 
        'Price': Integer(),
        'PO' : String(length=200)
    })

except:
    print('error')



#  pymssql.OperationalError as e:
#     print("Mssql error "+e)
