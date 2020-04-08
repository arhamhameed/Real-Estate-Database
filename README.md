# Data Base Assignment 

## CS162 DB Application - Assignmnet 2

### Running the Python Code

In order to run the python files, please follow the instructions and strictly take care of the order of file execution. 

#### Go to the path where the folder is located. Then execute the following code in the given order:

Install all the necessary dependancies using the command:

    pip3 install -r requirements.txt

Create the database for the queries using the command

    python3 create.py 

Insert dummy data into the database using the command

    python3 insert.py

Query the questions using the command

    python3 query.py
   
The reason why order is import is because we first have to create the database then insert data in it and then query it. 

# Normalization, Transaction, and Indices

Throughout the brainstorming part of the assignment I focused on data normalization, how I can reduce the redundancies, and follow the guidelines of 3NF form. 

I tried to ensure that there are no transitive functional dependancies. When constructing many to many relationships, I created a table AgentOffices, that stores information regarding relationship between two tables: Offices and Agents. 

With respect to transactions, since I used SQLAlchemy I set autocommit to False and whenever the data is updated or inserted, it is not logged until we do not commit it. Therefore, the data is inserted and updated in the database without any interruptions. 

