import sqlalchemy as db
from sqlalchemy import create_engine, Column, Text, Integer, ForeignKey, Float, Boolean, DateTime, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import func
from sqlalchemy import Table, MetaData
import datetime
import pandas as pd
from datetime import timedelta
import numpy as np
from insert import session, engine 
import calendar
import random
from random import choice
from string import ascii_uppercase
from create import Agents, Sellers, Houses, Buyers, Offices, AgentOffices, Transactions, engine, connection, metadata    

Agents = db.Table('Agents', metadata, autoload = True, autoload_with = engine)
Sellers = db.Table('Sellers', metadata, autoload = True, autoload_with = engine)
Buyers = db.Table('Buyers', metadata, autoload = True, autoload_with = engine)
Offices = db.Table('Offices', metadata, autoload = True, autoload_with = engine)
AgentOffice = db.Table('AgentOffices', metadata, autoload = True, autoload_with = engine)
Houses = db.Table('Houses', metadata, autoload = True, autoload_with = engine)
Transactions = db.Table('Transactions', metadata, autoload = True, autoload_with = engine)

#begin = datetime.datetime(year, month, 1)
#end = datetime.datetime(year, month, calendar.monthrange(year, month)[1], 23, 59, 59)


# QUESTION 1: Find the top 5 offices with the most sales for that month.
def query_1(number = 5, year = datetime.datetime.utcnow().year, month = datetime.datetime.utcnow().month):
    
    begin = datetime.datetime(year, month, 1)
    end = datetime.datetime(year, month, calendar.monthrange(year, month)[1], 23, 59, 59)
    #print(begin, end)
    query = db.select([Offices.columns.officeId.label('Office ID'), Offices.columns.name.label('Office Name'), Offices.columns.zipCode.label('ZipCode'), Offices.columns.email.label('Contact Info: Email'),Offices.columns.phone.label('Phone'),
                       db.func.sum(Transactions.columns.salesPrice).label('Total Sales'), 
                       db.func.count(Transactions.columns.salesPrice).label('Sales Count')])\
                        .group_by(Houses.columns.zipCode)\
                        .where(db.and_(Transactions.columns.soldDate >= begin, end >= Transactions.columns.soldDate))\
                        .order_by(db.desc(db.func.sum(Transactions.columns.salesPrice)))
                        

    query = query.select_from((Transactions.join(Houses, Transactions.columns.houseId == Houses.columns.houseId)).join(Offices, Offices.columns.zipCode == Houses.columns.zipCode))
    results = connection.execute(query).fetchall()[:number]
    print("/---------------------------------------------------------------------------------------/")
    print("The top {2} Offices that achoeved the highest sales in month: {0}, {1}".format(month, year,number))
    print("/---------------------------------------------------------------------------------------/")
    if results != []:
        df = pd.DataFrame(results)
        df.columns = results[0].keys()
        print(df)
    else: 
        print("No sales in month: {}".format(month))
print("QUESTION 1: Find the top 5 offices with the most sales for that month") 
query_1()
print("/-------------------------------------------------------------------------------------------/")

# QUESTION 2: Find the top 5 estate agents who have sold the most (include their contact details and their sales details so that it is easy contact them and congratulate them).
def query_2(count = 5):
    query = db.select([Transactions.columns.agentId.label('Agent ID'),Agents.columns.firstName.label('Name'), Agents.columns.email.label('Contact Info: Email'), Agents.columns.phone.label('Phone'),
                       db.func.sum(Transactions.columns.salesPrice).label('Total Sales'), 
                       db.func.count(Transactions.columns.salesPrice).label('Number of Sales')])\
                        .group_by(Transactions.columns.agentId).order_by(db.desc(db.func.sum(Transactions.columns.salesPrice)))

    query = query.select_from(Agents.join(Transactions, 
                                         Agents.columns.agentId == Transactions.columns.agentId))
    results = connection.execute(query).fetchall()[:count]
    df = pd.DataFrame(results)
    df.columns = results[0].keys()
    print("/---------------------------------------------------------------------------------------/")
    print("The top {0} agents with the highest sales number:".format(count))
    print("/---------------------------------------------------------------------------------------/")
    print(df)

print("\n\nQUESTION 2: Find the top 5 estate agents who have sold the most (include their contact details and their sales details so that it is easy contact them and congratulate them)")
query_2()
print("/---------------------------------------------------------------------------------------/")

# commision function
def agent_commission(price):
    price = int(price)
    if price < 100000:
        return 0.1*price
    elif price < 200000:
        return 0.075*price
    elif price < 500000:
        return 0.06*price
    elif price < 1000000:
        return 0.05*price
    else: 
        return 0.04*price

# QUESTION 3: Calculate the commission that each estate agent must receive and store the results in a separate table.

def query_3():
    
    query = db.select([Transactions.columns.agentId.label('Agent ID'), Agents.columns.email.label('Email'), Transactions.columns.salesPrice])

    query = query.select_from(Agents.join(Transactions, 
                                         Agents.columns.agentId == Transactions.columns.agentId))
    results = connection.execute(query).fetchall()
    #print(results)
    df = pd.DataFrame(results)
    df.columns = results[0].keys()
    df['salesPrice'] = df['salesPrice'].apply(agent_commission)
    dd = df.groupby(['Email']).sum()
    com_table = dd.sort_values(["salesPrice"], ascending=False)
    com_table.rename(columns = {'salesPrice': 'Commision'}, inplace=True)
    print("/---------------------------------------------------------------------------------------/")
    print(com_table)  

print("\n\nQuestion 3: Calculate the commission that each estate agent must receive and store the results in a separate table.")
query_3()
print("/---------------------------------------------------------------------------------------/")

#QUESTION 4: For all houses that were sold that month, calculate the average number of days that the house was on the market.
def query_4(year = datetime.datetime.utcnow().year, month = datetime.datetime.utcnow().month):  
    start = datetime.datetime(year, month, 1)
    end = datetime.datetime(year, month, calendar.monthrange(year, month)[1], 23, 59, 59)
    query = db.select([Transactions.columns.houseId.label('House ID'), Houses.columns.listDate.label('List Date'), Transactions.columns.soldDate.label('Sold Date'), Houses.columns.zipCode.label('Zip Code')])\
                        .where(db.and_(Transactions.columns.soldDate >= start, end >= Transactions.columns.soldDate))

    query = query.select_from(Transactions.join(Houses, 
                                         Transactions.columns.houseId == Houses.columns.houseId))
    results = connection.execute(query).fetchall()
    if results is not None:
        df = pd.DataFrame(results)
        df.columns = results[0].keys()
        df["On Market Days"] = abs(df["Sold Date"] - df["List Date"])
        print("/---------------------------------------------------------------------------------------/")
        print("The average number of days a house was listed on the market in Month {0}, Year {1} was: {2} Days".format(month, year, np.mean(df["On Market Days"]).days + 1))
        print("/---------------------------------------------------------------------------------------/")
        print(pd.DataFrame(df[["House ID", "Zip Code", "On Market Days"]]))
        print()
    else:
        print("No Hosues sold in Month: {0}, Year {1}".format(month,year))
        
print("\n\nQUESTION 4: For all houses that were sold that month, calculate the average number of days that the house was on the market")
query_4()
print("/---------------------------------------------------------------------------------------/")

# QUESTION 5: For all houses that were sold that month, calculate the average selling price
def query_5(year = datetime.datetime.utcnow().year, month = datetime.datetime.utcnow().month):
    
    start = datetime.datetime(year, month, 1)
    end = datetime.datetime(year, month, calendar.monthrange(year, month)[1], 23, 59, 59)
    
    query = db.select([Transactions.columns.agentId, Houses.columns.zipCode, Transactions.columns.salesPrice])\
            .where(db.and_(Transactions.columns.soldDate >= start, end >= Transactions.columns.soldDate))

    query = query.select_from(Transactions.join(Houses, 
                                         Transactions.columns.houseId == Houses.columns.houseId))
    results = connection.execute(query).fetchall()
    if results is not None:
        df = pd.DataFrame(results)
        df.columns = results[0].keys()
        
        print("/---------------------------------------------------------------------------------------/")
        print("The Average Sales Price in Month {0} is: ${1}".format(month, int(np.mean(df["salesPrice"]))))
        print("/---------------------------------------------------------------------------------------/")
        df.rename(columns = {'salesPrice': 'Sales Price', 'agentId': 'Agent ID', 'zipCode': 'Zip Code'}, inplace=True)
        print("\n", df)
    else: 
        print("No Sales in month: {}.format(month)")
    
print('QUESTION 5: For all houses that were sold that month, calculate the average selling price')
query_5()
print("/---------------------------------------------------------------------------------------/")

# QUESTION 6: Find the zip codes with the top 5 average sales prices
def query_6(count = 5):
    
    query = db.select([Offices.columns.name.label('Office Name'), Offices.columns.zipCode.label('Zip Code'), 
                       db.func.avg(Transactions.columns.salesPrice).label('Average Sales Price')])\
                        .group_by(Houses.columns.zipCode).order_by(db.desc(db.func.sum(Transactions.columns.salesPrice)))

    query = query.select_from((Transactions.join(Houses, Transactions.columns.houseId == Houses.columns.houseId)).join(Offices, Offices.columns.zipCode == Houses.columns.zipCode))
    results = connection.execute(query).fetchall()[:count]
    if results != []:
        df = pd.DataFrame(results)
        df.columns = results[0].keys()
        print("/---------------------------------------------------------------------------------------/")
        print("The top {0} best selling areas' Zip Codes are as follows:".format(count))
        print("/---------------------------------------------------------------------------------------/")
        print(df)
    else:
        print("No Houses sold in the month {0}".format(month))
        
print("\n\nQUESTION 6: Find the zip codes with the top 5 average sales prices")
query_6()
print("/---------------------------------------------------------------------------------------/")