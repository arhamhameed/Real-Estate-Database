
# Imports
import sqlalchemy as db
from sqlalchemy import create_engine, Column, Text, Integer, ForeignKey, Float, Boolean, DateTime, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import func
from sqlalchemy import Table, MetaData
import datetime
import pandas as pd
from datetime import datetime
import numpy as np


# Initialize DataBase
engine = create_engine('sqlite:///database.db')
engine.connect()
connection = engine.connect()
metadata = MetaData()
Base = declarative_base()


class Agents(Base):
    __tablename__ = 'Agents'
    agentId = Column(Integer, primary_key = True)
    firstName = Column(VARCHAR(20), nullable = False)
    lastName = Column(VARCHAR(20), nullable = False)
    email = Column(VARCHAR(50), nullable = False)
    phone = Column(VARCHAR(20), nullable = False)


class Sellers(Base):
    __tablename__ = 'Sellers'
    sellerId = Column(Integer, primary_key = True)
    firstName = Column(Text, nullable = False)
    lastName = Column(Text, nullable = False)
    email = Column(VARCHAR(50), nullable = False)
    phone = Column(VARCHAR(20), nullable = False)


class Buyers(Base):
    __tablename__ = 'Buyers'
    buyerId = Column(Integer, primary_key = True)
    firstName = Column(Text, nullable = False)
    lastName = Column(Text, nullable = False)
    email = Column(VARCHAR(50), nullable = False)
    phone = Column(VARCHAR(20), nullable = False)


class Offices(Base):
    __tablename__ = 'Offices'
    officeId = Column(Integer, primary_key = True)
    name = Column(Text, nullable = False)
    email = Column(VARCHAR(50), nullable = False)
    phone = Column(VARCHAR(20), nullable = False)
    zipCode = Column(Integer, nullable = False)

class AgentOffices(Base):
    __tablename__ = 'AgentOffices'
    agentId = Column(Integer, ForeignKey('Agents.agentId'), primary_key = True)
    officeId = Column(Integer, ForeignKey('Offices.officeId'), primary_key = True)
    agent = relationship(Agents)
    office = relationship(Offices)


class Houses(Base):
    __tablename__ = 'Houses'
    houseId = Column(Integer, primary_key = True)
    bedroom = Column(Integer, nullable = False)
    bathroom = Column(Integer, nullable = False)
    price = Column(Integer, nullable = False)
    zipCode = Column(VARCHAR, nullable = False)
    agentId = Column(Integer, ForeignKey('Agents.agentId'))
    sellerId = Column(Integer, ForeignKey('Sellers.sellerId'))
    sold = Column(Integer, nullable = False, default = 0)
    listDate = Column(DateTime, nullable = False, default = datetime.utcnow())
    agent = relationship(Agents)
    seller = relationship(Sellers)

class Transactions(Base):
    __tablename__ = "Transactions"
    houseId = Column(Integer, ForeignKey('Houses.houseId'), primary_key = True)
    sellerId = Column(Integer,ForeignKey('Sellers.sellerId'))
    buyerId = Column(Integer,ForeignKey('Buyers.buyerId'))
    agentId = Column(Integer, ForeignKey('Agents.agentId'))
    salesPrice = Column(Integer, nullable = False)
    soldDate = Column(DateTime, nullable = False, default = datetime.utcnow())
    housing = relationship(Houses)
    agent = relationship(Agents)
    seller = relationship(Sellers)




Base.metadata.create_all(engine)