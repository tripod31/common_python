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

### class CsvSqla
convert CSV<-->SqlAlchemy

### class CsvSqlite
convert CSV<-->Sqlite

#### converting CSV-->Sqlite
All columns are converted to TEXT Columns.
When Column has name like "*_id",index is added to it.
