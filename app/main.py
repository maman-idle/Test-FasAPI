from logging import exception
from typing import Optional
from fastapi import FastAPI, status, HTTPException
from pydantic import BaseModel
import random, time
import psycopg2
from psycopg2.extras import RealDictCursor #Enable easier access columns

#Creating schema for the API (validate the input) using pydantic
class Customer(BaseModel):
    name: str
    age: int
    occupation: str = "Wizard"   #Create default value
    address: Optional[str]  #Create optional value

#hardcoded data
my_customers = [
    {"id":1,"name":"Alice","age":13,"occupation":"Student"},
    {"id":2,"name":"Bob","age":19,"occupation":"Farmer"},
    {"id":3,"name":"Claire", "age":29,"address":"Illinois"}
]

#Connect to PostgreSQL DB
while True:
    try:
        conn = psycopg2.connect(host='localhost', database='fastapi', user='postgres', 
        password='mhfu3rd', cursor_factory=RealDictCursor)
        cursor = conn.cursor() #Allow python code to execute PostgeSQL command in DB session
        print("Connected")
        break

    except Exception as error:
        print(error)
        time.sleep(30) #Retry the connection for every 30s


#initiate the FastApi
app = FastAPI()

#URL ordering matter! Get the more specific one on top of the more generic.

#Get many
@app.get("/customers")
def get_customers():

    #Query request
    cursor.execute(""" SELECT * FROM customers """)
    users = cursor.fetchall()
    return {'users':users}


#Create
@app.post("/customer", status_code=status.HTTP_201_CREATED) #adding status code for specific path operation
def create_customer(customer:Customer):

    #Use returning to fetch the data
    cursor.execute(""" insert into customers(name, age, occupation, address) values(%s,%s,%s,%s) returning *""",
    (customer.name,customer.age, customer.occupation, customer.address))

    new_customer = cursor.fetchone() #get the input for this insertion
    conn.commit() #commit your change to the database

    return {"customer":new_customer}


#Get one
@app.get("/customers/{id}")
def get_customer(id:int):
    try:
        cursor.execute(""" select * from customers where id = %s""", str(id) )
    except:
        #Giving correct status code for unavailable item
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not available")

    customer = cursor.fetchone()
    if customer:
        return {"customer":customer}


#Edit
@app.put("/customers/{id}")
def update_customer(id:int, customer:Customer):
    try:
        cursor.execute(""" select * from customers where id = %s""", str(id) )
    except:
        #Giving correct status code for unavailable item
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not available")

    cursor.execute("""update customers set name=%s, age=%s, occupation=%s, address=%s where id=%s
    returning *""", (customer.name,
    customer.age, customer.occupation, customer.address, str(id)))

    edited_customer = cursor.fetchone()
    conn.commit()
    return {"customer":edited_customer}


#Delete
@app.delete("/customers/{id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_customer(id:int):
    try:
        cursor.execute(""" select * from customers where id = %s""", str(id) )
    except:
        #Giving correct status code for unavailable item
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not available")

    cursor.execute(""" delete from customers where id=%s """, str(id) )  
    conn.commit()   