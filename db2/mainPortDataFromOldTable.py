import CoreDb
import DbLogger
import DbSchema

def getFilesNotInNewFilesTable(db):
	command = "select filehash, filesize, status from %s " % DbSchema.oldFilesTable + \
		"where filehash not in (select filehash from %s);" % DbSchema.newFilesTable
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	return results


# files not in depot are not in the new table, transfer them over
# set location to 0 to indicate not in depot
def copyNotFoundFilesIntoNewFilesTable(db):
	command = "INSERT INTO %s (filehash, filesize, primaryLocation) " % DbSchema.newFilesTable + \
		"SELECT filehash, filesize, 0 FROM %s " % DbSchema.oldFilesTable + \
		"where filehash not in (select filehash from %s);" % DbSchema.newFilesTable
	db.ExecuteNonQuerySql(command)

db = CoreDb.CoreDb("C:\\depotListing\\listingDb.sqlite")
logger = DbLogger.dbLogger()

copyNotFoundFilesIntoNewFilesTable(db)

filelist = getFilesNotInNewFilesTable(db)
for entry in filelist:
	logger.log(entry)