import DbInterface

dbfile = "trythis.db"

db = DbInterface.DbInterface(dbfile)

db.ExecuteNonQuerySql("DROP TABLE IF EXISTS Cars")
db.ExecuteNonQuerySql("CREATE TABLE Cars(Id INT, Name TEXT, Price INT)")
db.ExecuteNonQuerySql("INSERT INTO Cars VALUES(1,'Audi',52642)")
db.ExecuteNonQuerySql("INSERT INTO Cars VALUES(2,'Mercedes',57127)")
db.ExecuteNonQuerySql("INSERT INTO Cars VALUES(3,'Skoda',9000)")
db.ExecuteNonQuerySql("INSERT INTO Cars VALUES(4,'Volvo',29000)")
db.ExecuteNonQuerySql("INSERT INTO Cars VALUES(5,'Bentley',350000)")
db.ExecuteNonQuerySql("INSERT INTO Cars VALUES(6,'Citroen',21000)")

row = db.ExecuteSqlQueryReturningSingleRow("SELECT Name, Price FROM Cars WHERE Id=1")
print "row = %s" % str(row)

rows = db.ExecuteSqlQueryReturningMultipleRows("SELECT * FROM Cars")
print "rows = %s" % str(rows)

value = db.ExecuteSqlQueryReturningSingleInt("SELECT Price FROM Cars WHERE Id=2")
print "value = %d" % value

values = db.ExecuteSqlQueryForStrings("SELECT Name FROM Cars")
print "values = %s" % str(values)

value = db.ExecuteSqlQueryForSingleString("SELECT Name FROM Cars WHERE Id=2")
print "value = %s" % value

rows = db.GetDatasetForSqlQuery("SELECT * FROM Cars")
print "rows = %s" % str(rows)

generator = db.GetDataReaderForSqlQuery("SELECT * FROM Cars")
for i in generator:
	print i


commandList = [
		"INSERT INTO Cars VALUES(7,'Volvo',29000)",
		"INSERT INTO Cars3 VALUES(8,'Bentley',350000)",
		"INSERT INTO Cars VALUES(9,'Citroen',21000)"
		]

try:
	db.ExecuteMultipleSqlStatementsWithRollback(commandList)
except:
	pass
	
rows = db.ExecuteSqlQueryReturningMultipleRows("SELECT * FROM Cars")
print "rows = %s" % str(rows)

commandList = [
		"INSERT INTO Cars VALUES(7,'Volvo',29000)",
		"INSERT INTO Cars VALUES(8,'Bentley',350000)",
		"INSERT INTO Cars VALUES(9,'Citroen',21000)"
		]

db.ExecuteMultipleSqlStatementsWithRollback(commandList)
rows = db.ExecuteSqlQueryReturningMultipleRows("SELECT * FROM Cars")
print "rows = %s" % str(rows)
