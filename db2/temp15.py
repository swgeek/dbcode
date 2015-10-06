# remove files and entries for "toDeleteCompletely" files
# EXactly the same as "toRemove, i.e. temp10", so combine

import CoreDb
import DbLogger
import miscQueries
import DbSchema
import FileUtils


def deleteFileFromDisk(filehash, logger):
	depotRoot1 = "I:\\objectstore1"
	depotRoot2 = "F:\\objectstore2"
	deleted = FileUtils.DeleteFileFromDepot(depotRoot1, filehash)
	if deleted:
		logger.log("deleted %s from %s" % (filehash, depotRoot1))

	deleted = FileUtils.DeleteFileFromDepot(depotRoot2, filehash)
	if deleted:
		logger.log("deleted %s from %s" % (filehash, depotRoot2))


logger = DbLogger.dbLogger()

dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/fmapp/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)

list = miscQueries.getFilesWithStatus(db, "toDeleteCompletely")
logger.log("got %d entries " % len(list))

if list is None:
    logger.log("list is None")
    exit(1)

if not list:
    logger.log("list is empty")
    exit(1)

# now dir entries usually already removed, use this to check quickly instead of wasting db call
origDirsForFileDict, origFilesForDirDict = miscQueries.cacheOrigDirsForFileTable(db)


for row in list:
	logger.log("deleting %s" % str(row))

	filehash, dummy, dummy2, dummy3 = row
	logger.log(filehash)

	# physically delete from disk
	deleteFileFromDisk(filehash, logger)

	# delete completely from newFilesTable
	command = "delete from %s where filehash = '%s';" % (DbSchema.newFilesTable, filehash)
	db.ExecuteNonQuerySql(command)

	# get list of entries in originalDirectoriesForFile
	dirlist = origDirsForFileDict.get(filehash)
	if not dirlist:
		logger.log("empty dirs list")
		continue


	logger.log("dirs:")
	for entry in dirlist:
		logger.log(entry)

	logger.log("deleting dir entries")
	# delete from originalDirectoriesForFile
	command = "delete from %s where filehash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, filehash)
	db.ExecuteNonQuerySql(command)

	dirlist = miscQueries.getOriginalDirectoriesForFile(db, filehash)
	logger.log("dirs after delete:")
	for entry in dirlist:
		logger.log(entry)

