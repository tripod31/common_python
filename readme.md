common_python
=====
python library.

Util　　
-----
misc functions

CSV  
-----
read CSV file into sqlite/sqlalchemy object
### required libraries
+ sqlalchemy

### class CsvSqlite
convert CSV<-->Sqlite

#### convert CSV --> Sqlite

Create sqlite table from csv header,asumming that first line is header.
Add auto-incriment "_id_" column as first column,as primary key.this is because we need primary key to update data by SQL.
All the other columns are text columns.
Create index to the columns which names are '*_id'.

### class CsvSqla
convert CSV<-->SqlAlchemy
