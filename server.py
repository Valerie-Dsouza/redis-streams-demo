import redis
red = redis.Redis(host="localhost",port=6379,db=1)

stream = "customer"
records= int(input("Enter no of records to insert : "))
n=0
while (n<records):
    customer_id=input("Enter customer id  ")
    customer_name=input("Enter customer name  ")
    red.xadd(stream,{"id ": customer_id,"name ": customer_name})
    n += 1
