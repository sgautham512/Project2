# Project2

Problem Statement : 
The task is to fetch data from a server log file, extract all email addresses along with their corresponding dates, and upload this data into a user history database. The goal is to ensure the extracted data is clean, accurate, and accessible for further analysis and historical tracking.
Tasks:

Task 1: Extract Email Addresses and Dates
Read the log file and extract all occurrences of email addresses.
Capture the date associated with each email address.

Task 2: Data Transformation
Ensure the extracted data is structured in a format suitable for database insertion.
Format the date into a standard format (e.g., YYYY-MM-DD HH:MM:SS).
Task 3: Save Data to MongoDB
Save the processed data into a MongoDB collection named user_history.
Task 4: Database Connection and Data Upload
Fetch the data from the MongoDB collection.
Insert the data into a relational database (e.g., SQLite or any SQL database) using Python.
The table name should be user_history.
Include primary key constraints and any other relevant constraints.




Task 5: Run Queries on the Database
Write and execute SQL  queries to analyze the data, :
List all unique email addresses.
Count the number of emails received per day.
Find the first and last email date for each email address.
Count the total number of emails from each domain (e.g., gmail.com, yahoo.com).
create 10 questions and answer using SQL queries. 
