import sqlalchemy as db
from sqlalchemy import create_engine, Column, Text, Integer, ForeignKey, Float, Boolean, DateTime, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import func, select
from sqlalchemy import Table, MetaData
import datetime
import pandas as pd
from datetime import timedelta
import numpy as np
import calendar
import random
from random import choice
from string import ascii_uppercase
from create import Agents, Sellers, Houses, Buyers, Offices, AgentOffices, Transactions, engine

Session = sessionmaker(bind=engine)
session = Session() 
metadata = MetaData()


def main():
    
    #Random Names for the data
    fname = ('John','Andy','Joe', 'Angelina', 'Jack','Emma','Jennifer','Sophie', 'Kirsty', 'Ahmed', 'Tiffany')
    lname = ('Johnson','Smith','Williams', 'Good', 'Sparrow','Watson','Anniston','Hall', 'Ghandhi', 'Song', 'Stewart')

    #Agents: agentId, firstName, lastName, email
    def add_agent(fname, lname):
        fname = " ".join(random.choice(fname) for _ in range(1))
        lname = " ".join(random.choice(lname)for _ in range(1))
        email = fname + '.' + lname + '@agent.com'
        phone = int('1'+''.join(choice("0123456789") for i in range(10)))
        session.add(Agents(firstName = fname, lastName = lname, email = email, phone = phone))

    #inserts data into the table Agents
    for i in range(500):
        add_agent(fname, lname)
    session.commit()

    '''stmt = select('*').select_from(Agents)
    result = session.execute(stmt).fetchall()
    print(result)'''

    #Sellers: sellerId, firstName, lastName, email
    def add_sellers(fname, lname):
        fname = " ".join(random.choice(fname) for _ in range(1))
        lname = " ".join(random.choice(lname)for _ in range(1))
        email = fname + '.' + lname + '@seller.com'
        phone = int('1'+''.join(choice("0123456789") for i in range(10)))

        session.add(Sellers(firstName = fname, lastName = lname, email = email, phone = phone))

    for i in range(1000):
        add_sellers(fname, lname)
    session.commit()

    #Buyers: buyerId, firstName, lastName, email
    def add_buyers(fname, lname):
        fname = " ".join(random.choice(fname) for _ in range(1))
        lname = " ".join(random.choice(lname)for _ in range(1))
        email = fname + '.' + lname + '@buyer.com'
        phone = int('1'+''.join(choice("0123456789") for i in range(10)))
        session.add(Buyers(firstName = fname, lastName = lname, email = email, phone = phone))

    for i in range(1000):
        add_buyers(fname, lname)
    session.commit()

    #Offices: officeId, name, email, phone, zipCode
    def add_offices(fname, lname):
        name = " ".join(random.choice(fname) for _ in range(1)) + " " + " ".join(random.choice(lname) for _ in range(1)) + " " + 'Properties'
        email = name.replace(" ", "") + '@office.com'
        phone = int('1'+''.join(choice("0123456789") for i in range(10)))
        zipcode = random.randint(10000, 99000)
        session.add(Offices(name = name, email = email, phone = phone, zipCode = zipcode))
        #print(name, phone, zipcode, email)

    for i in range(1210):
        add_offices(fname, lname)
    session.commit()

    #AgentOffices: agentId, officeId, agent, office
    def add_agentOffice(AID):
        pop = set(range(1,21))
        rel = random.randint(1,10)
        samples = random.sample(pop, rel)
        for _ in samples:
            session.add(AgentOffices(agentId = AID, officeId = _))

    for AID in range(500):
        add_agentOffice(AID)
    session.commit()

    #Houses: houseId, bedroom, bathroom, price, zipCode, agentId, sellerId, sold, listDate, agent, seller
    def add_house():
        bednum = random.randint(1,10)
        bathnum = random.randint(1,5)
        price = random.randint(100000, 1000000)
        zipcode = random.randint(10000, 99000)
        AID = random.randint(1,10)
        SID = random.randint(1,20)
        date = datetime.datetime(random.randint(2018,2020), random.randint(1,12), random.randint(1,28))
        session.add(Houses(bedroom = bednum, bathroom = bathnum, price = price, zipCode = zipcode, agentId = AID, sellerId = SID, listDate = date, sold = 0))

    for i in range (2000):
        add_house()
    session.commit()

    #Transactions: houseId, sellerId, buyerId, agentId, salesPrice, housing, agent, seller
    def add_transaction(buyerId, houseId, AID, SP):
        buyer = session.query(Buyers).filter(Buyers.buyerId == buyerId).first()
        house = session.query(Houses).filter(Houses.houseId == houseId).first()
        if buyer == None:
            print('Invald Buyer ID')
        elif house == None:
            print('Invalid House ID')
        elif house.sold == 1:
            print('House has been Sold!')
        else:
            house.sold = 1

            transaction = Transactions(houseId = house.houseId, 
                                        buyerId = buyer.buyerId, 
                                        sellerId = house.sellerId, 
                                        agentId = AID, 
                                        salesPrice = SP)

            session.add(transaction)
            session.commit()

    pop = set(range(1,1100))
    sam = random.sample(pop, 500)
    for i in sam:
        add_transaction(random.randint(1,100), i, random.randint(1,50), random.randint(100000,1000000))
        
if __name__ == '__main__':
    main()            