!python -m pip install "pymongo[srv]"
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
!pip install mysql-connector-python
import mysql.connector
from mysql.connector import Error

import re
from datetime import datetime, date, time

# all the mails recieved starts with "From "
# ^ - helps you search a line starting with From
# include  space at the end in order to differentiate it from anothe instance "From:"
pattern = r"^From "

# mailid regex pattern also ensures if it is a valid mail id
# alternatively we can search for a word including "@"
# just using \w will exclude the special characters in the regex, we need full expression
# mailPattern = r"\w*@\w+"
mailPattern = r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"

# date pattern
datePattern = r"(\w{3} *\d{1,2} \d{2}:\d{2}:\d{2} \d{4})"

# create a list to store the parsed data in the form dictionaries to feed into Mango DB
data=[]

with open ("mbox.txt",'r') as file:
 for line in file:
  match = re.match(pattern,line)
  if(match):
      #print(line)
      mailIds = re.findall(mailPattern, line)
      dateMatch = re.findall(datePattern,line)
      parsed_date = datetime.strptime(dateMatch[0], "%b %d %H:%M:%S %Y")
      #print(mailIds) #print(dateMatch) #print(type(parsed_date))
      # check if one mailid and date is found in a line
      # convert them in the form json data and append to the list
      if(len(mailIds)==1 and len(dateMatch) == 1):
        logData={"Email":mailIds[0], "Date": parsed_date}
        data.append(logData)
        
# establish the connection to the mongoDB and test the connection

uri = "mongodb+srv://gauthamsundaram120593:Sundar_90@cluster0.4s2ir.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

client = MongoClient("mongodb+srv://gauthamsundaram120593:Sundar_90@cluster0.4s2ir.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

# create a new database Project 2
db = client.Project2

# create a new collection named user_history in the database
records = db.user_history

# insert the exatracted data into the user_history collection
records.insert_many(data)

# create some find querie if the data is inserted properly
x = records.find()
#x = records.find({"Email":"stephen.marquard@uct.ac.za"})
for i in x:
  print(i)

# establish the connection to the SQL database hosted un tiDB and test the connection

try:
    connection = mysql.connector.connect(
    host = "gateway01.eu-central-1.prod.aws.tidbcloud.com",
    port = 4000,
    user = "2r9Eig778rCimFs.root",
    password = "zxG9GH43o1cFDYow"
    )
    if connection.is_connected():
        print("Connection successful!")
        # Ping the server
        connection.ping(reconnect=True, attempts=3, delay=5)
        print("Ping successful!")
        mycursor = connection.cursor()
except Error as e:
    print(f"Error: {e}")


# create a new database named Project-2
mycursor.execute("create database Project2")
mycursor.execute("show databases")
print(list(mycursor))

# create a new table named user_history to store the mail and data information
mycursor.execute("""
        CREATE TABLE IF NOT EXISTS Project2.user_history (
            Email VARCHAR(255),
            Date DATETIME
        );
    """)

#current_datetime = datetime.now()
#mycursor.execute(insert into Project2.user_history values (%s,%s),"gau_512@gmail.com, current_datetime")"
#mycursor.execute(" DELETE FROM Project2.user_history where email='sgau@gmail.com' ")
#connection.commit()
data = records.find()
query = "insert into Project2.user_history values (%s,%s)"
# use list comprhension to convert the data from dictionaries format to Tuple so that it can be added to the SQL database
values = [(x["Email"],x["Date"]) for x in data]
mycursor.executemany(query,values)
connection.commit()


mycursor.execute("select* from Project2.user_history")
out = mycursor.fetchall()
from tabulate import tabulate
print(tabulate(out, headers=[i[0] for i in mycursor.description],showindex="always",tablefmt='psql'))

#List all unique email addresses
mycursor.execute("select distinct Email from Project2.user_history")
out = mycursor.fetchall()
from tabulate import tabulate
print(tabulate(out, headers=[i[0] for i in mycursor.description],showindex="always",tablefmt='psql'))

#Count the number of emails received per day.
mycursor.execute("select DATE(Date), count(Email) from Project2.user_history group by DATE(Date) order by count(Email) desc ")
out = mycursor.fetchall()
from tabulate import tabulate
print(tabulate(out, headers=[i[0] for i in mycursor.description],showindex="always",tablefmt='psql'))

# Find the first and last email date for each email address
mycursor.execute("select Email, min(Date) As First_Date, max(Date) As Last_Date from Project2.user_history group by Email")
out = mycursor.fetchall()
from tabulate import tabulate
print(tabulate(out, headers=[i[0] for i in mycursor.description],showindex="always",tablefmt='psql'))

#Count the total number of emails from each domain (e.g., gmail.com, yahoo.com)
#mycursor.execute("select Distinct Email from Project2.user_history where Email REGEXP '@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}' ")
mycursor.execute("select SUBSTRING_INDEX(Email, '@', -1) As Domain, count(SUBSTRING_INDEX(Email, '@', -1)) AS Count from Project2.user_history Group by SUBSTRING_INDEX(Email, '@', -1) order by count(SUBSTRING_INDEX(Email, '@', -1))")
out = mycursor.fetchall()
from tabulate import tabulate
print(tabulate(out, headers=[i[0] for i in mycursor.description],showindex="always",tablefmt='psql'))


#The number of unique users that logged in on a specific date
mycursor.execute("select DATE(Date) As Date, count(DISTINCT Email) As UniqueEmail from Project2.user_history group by DATE(Date) order by count(DISTINCT Email) desc ")
out = mycursor.fetchall()
from tabulate import tabulate
print(tabulate(out, headers=[i[0] for i in mycursor.description],showindex="always",tablefmt='psql'))


