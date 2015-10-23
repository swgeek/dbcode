#
# find directory hashes that do not have a corresponding path and try to recover path
# from older db backups

import DbLogger
import CoreDb
import DbSchema
import miscQueries


def checkBackupDb(dirlist, backupDbPath, db, logger):
	backupDb = CoreDb.CoreDb(backupDbPath)

	logger.log("checking %s" % backupDbPath)
	for dirhash in dirlist:
		dirpath = miscQueries.getDirectoryPath(backupDb, dirhash)
		if dirpath:
			logger.log("found %s: " % dirhash)
			logger.log(dirpath)
			miscQueries.insertDirHash(db, dirhash, dirpath)


logger = DbLogger.dbLogger()
dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/db/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)

# find dirhash with no direcotry path
command = "select dirPathHash from %s where dirPathHash not in (select dirPathHash from %s);" % (DbSchema.OriginalDirectoryForFileTable, DbSchema.OriginalDirectoriesTable) 
results = db.ExecuteSqlQueryReturningMultipleRows(command)
logger.log(len(results))
resultsSet = set(results)
logger.log(len(resultsSet))

# now see if can find dir listings from old tables
dirlist = [x[0] for x in resultsSet]

backupDbPath = "E:\\dbBackup\\20151009\\listingDb.sqlite"
checkBackupDb(dirlist, backupDbPath, db, logger)

