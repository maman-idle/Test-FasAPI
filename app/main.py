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
    occupation: str = "Wise man"   #Create default value
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

@app.get("/customers")
def get_customers():
    return{"customer":my_customers}


#adding status code for specific path operation
@app.post("/customer", status_code=status.HTTP_201_CREATED)
def create_customer(customer:Customer):
    test_customer = customer.dict() #Convert to dict to allow assignment
    test_customer["id"] = random.randrange(4, 1000)
    my_customers.append(test_customer)
    return {"customer":test_customer}


@app.get("/customers/{id}")
def get_customer(id:int):
    for customer in my_customers:
        if customer["id"] == id:
            return{"customer":customer}
    #Giving correct status code for unavailable item
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not available")