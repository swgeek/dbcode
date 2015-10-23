#
# find files that do not have an entry in dirListingforFile and mark them as orphans.

import DbLogger
import CoreDb
import DbSchema
import miscQueries


def sanityCheckOrphans(db, filelist, logger):
	for filehash in filelist:
		results = miscQueries.getOriginalDirectoriesForFile(db, filehash)
		if len(results):
			logger.log("ERROR, have listing for supposed orphan %s" % filehash)
			exit(1)



def checkBackupDb(filelist, backupDbPath, db, logger):
	backupDb = CoreDb.CoreDb(backupDbPath)

	logger.log("checking %s" % backupDbPath)
	for filehash in filelist:
		results = miscQueries.getOriginalDirectoriesForFile(backupDb, filehash)
		if results:
			logger.log("found %s: " % filehash)
			for value in results:
				logger.log(value)
				filename = value[1]
				dirhash = value[2]
				command = "insertOriginalDir(db, %s, %s, %s)" % (filehash, filename, dirhash)
				logger.log(command)
				miscQueries.insertOriginalDir(db, filehash, filename, dirhash)
				miscQueries.setFileStatus(db, filehash, None)


logger = DbLogger.dbLogger()
dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/db/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)

# find files with no file listing, set status to orphan
#command = "update %s set status = 'orphan' where filehash not in (select filehash from %s);" % (DbSchema.newFilesTable, DbSchema.OriginalDirectoryForFileTable) 
#db.ExecuteNonQuerySql(command)

# now see if can find file listings from old tables

command = "select filehash from %s where status = 'orphan';" % DbSchema.newFilesTable
results = db.ExecuteSqlQueryReturningMultipleRows(command)
logger.log(len(results))
filelist = [x[0] for x in results]

#sanityCheckOrphans(db, filelist, logger)

backupDbPath = "E:\\dbBackup\\old_think_908\\previou\\listingDb.sqlite"
checkBackupDb(filelist, backupDbPath, db, logger)

# last option: get from temp listing
for filehash in filelist:
	command = "select * from %s where filehash = '%s';" % (DbSchema.TempDirectoryForFileTable, filehash)
	oldFileInfo = db.ExecuteSqlQueryReturningSingleRow(command)
	if oldFileInfo:
		logger.log("filehash: %s" % filehash)
		logger.log("oldinfo: %s" % str(oldFileInfo))
		filename = oldFileInfo[1]
		dirhash = oldFileInfo[2]
		miscQueries.insertOriginalDir(db, filehash, filename, dirhash)
		miscQueries.setFileStatus(db, filehash, None)


