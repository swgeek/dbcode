import CoreDb
import DbLogger

#dbpath = "C:\\depotListing\\listingDb.sqlite"
dbpath = "/Users/v724660/fmapp/listingDb.sqlite"

db = CoreDb.CoreDb(dbpath)
logger = DbLogger.dbLogger()

command = "SELECT * FROM sqlite_master WHERE type='table'";
tableList = db.ExecuteSqlQueryReturningMultipleRows(command)
for row in tableList:
    logger.log(row)

logger.log("dropping largest files")

command = "DROP TABLE '%s'" % "largestFiles"
db.ExecuteNonQuerySql(command)

command = "SELECT * FROM sqlite_master WHERE type='table'";
tableList = db.ExecuteSqlQueryReturningMultipleRows(command)
for row in tableList:
    logger.log(row)
