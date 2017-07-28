common_python
=====
python library.

Util　　
-----
misc functions

CSV  
-----
read CSV file into sqlite/sqlalchemy object.  
write CSV file from sqlite/sqlalchemy object.  

### required libraries
+ sqlalchemy

### class CsvSqlite
convert CSV<-->Sqlite

#### convert CSV --> Sqlite

Create sqlite table from csv header,asumming that first line is header.  
All columns are text columns.
Create index to the columns which names are '*_id'.

### class CsvSqla
convert CSV<-->SqlAlchemy
