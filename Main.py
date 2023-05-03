import SQLServer2 as sql
import csv
import pyodbc

server =  "localhost\SQLExpress"
database = "FinalExamDataBase"

db = sql.SQLServer2(server, database)



db.connect()

# create table 1
db.execute_query('CREATE TABLE Book (ISBN VARCHAR(13) PRIMARY KEY, Title VARCHAR(255),Author VARCHAR(255),Publisher VARCHAR(255), PublishedDate INT, Category VARCHAR(255), Subcategory VARCHAR(255), Location VARCHAR(255), Shelf VARCHAR(255),Copies INT)')


filename = "Final_LibraryBookData.csv"
data = []

with open(filename, "r") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        data.append(row)

# insert data into table
table_name = "Book"
columns = [    "ISBN","Title","Author","Publisher","PublishedDate","Category","Subcategory","Location","Shelf","Copies","BorrowedBy"]



db.send_data(table_name, [tuple(row.values()) for row in data], columns=columns)
db.disconnect()
