import CoreDb
import DbLogger
import DbSchema

def getFilesNotInNewFilesTable(db):
	command = "select filehash, status from %s " % DbSchema.oldFilesTable + \
		"where filehash not in (select filehash from %s);" % DbSchema.newFilesTable
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	return results

db = CoreDb.CoreDb("C:\\depotListing\\listingDb.sqlite")
logger = DbLogger.dbLogger()

logger.log(getFilesNotInNewFilesTable(db))