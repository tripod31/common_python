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
convert CSV <--> Sqlite

#### constructor option
header:  
When True(Default),it asume that first line is header.  
When False,names of columns are 'col0','col1'... 

#### convert CSV --> Sqlite

All columns are text columns.
Create index to the columns which names are '*_id'.

### class CsvSqla
convert CSV<-->SqlAlchemy
