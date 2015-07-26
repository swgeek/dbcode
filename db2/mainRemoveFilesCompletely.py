
# removes files that with status "toRemoveCompletely"
# will check for any references to file (e.g. dirs)first, 
# will not remove if refs exist

import time
import os

import CoreDb
import DbLogger
import miscQueries
import DbSchema


def getCounts(db, logger):
	count = miscQueries.numberOfRows(db, DbSchema.newFilesTable)
	logger.log("newFilesTable has %d entries" % count)

	# get size on disk of depots if possible


def getFilesToRemove(db):
	command = "select filehash from %s where status = \"toRemoveCompletely\"; " \
		% (DbSchema.newFilesTable)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	# make this into a set
	fileSet = set([x[0] for x in result])
	return fileSet


def getFilesWithDirLocations(db):
	command = "select distinct(filehash) from %s; " % (DbSchema.OriginalDirectoryForFileTable)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	fileSet = set([x[0] for x in result])
	return fileSet


def getFilesWithTempDirLocations(db):
	command = "select distinct(filehash) from %s; " % (DbSchema.TempDirectoryForFileTable)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	fileSet = set([x[0] for x in result])
	return fileSet


# only does disk stuff, assumes db data handled elsewhere
def DeleteFileFromDepot(depotRootPath, filehash):
		subdir = filehash[0:2]
		filepath = os.path.join(depotRootPath, subdir, filehash)

		if not os.path.isfile(filepath):
			return False

		os.remove(filepath)
		return True


def removeFilesFromDisk(db, filesToRemove):
	# should get these from db, but hardcoding for now
	depotRoot = "I:\\objectstore1"
	depotRoot2 = "H:\\objectstore2"
	depotPaths = {1:depotRoot, 2:depotRoot2}

	for filehash in filesToRemove:
		# should get location from db, but for now just try deleting
		# from both depots
		DeleteFileFromDepot(depotRoot, filehash)
		DeleteFileFromDepot(depotRoot2, filehash)


def removeFilesFromFilesTable(db, filesToRemove):
	for filehash in filesToRemove:
		command = "delete from %s where filehash = '%s';" % (DbSchema.FileListingTable, filehash)
		db.ExecuteNonQuerySql(command)

		command = "delete from %s where filehash = '%s';" % (DbSchema.newFilesTable, filehash)
		db.ExecuteNonQuerySql(command)




db = CoreDb.CoreDb("/Users/v724660/fmapp/listingDb.sqlite")
logger = DbLogger.dbLogger()

startTime = time.time()

logger.log("start time: %s => time 0" % str(time.time()))
getCounts(db, logger)
logger.log("time: %s" % str(time.time() - startTime))

logger.log("getting list of files to remove")
filesToRemove = getFilesToRemove(db)
logger.log("%d files to remove" % len(filesToRemove))
logger.log("time: %s" % str(time.time() - startTime))

logger.log("getting getFilesWithDirLocations")
filesWithDirs = getFilesWithDirLocations(db)
logger.log("filesWithDirs has %d entries" % len(filesWithDirs))
logger.log("time: %s" % str(time.time() - startTime))

# do not delete any files with dir locations
filesToRemove = filesToRemove - filesWithDirs
logger.log("now %d files to remove" % len(filesToRemove))
logger.log("time: %s" % str(time.time() - startTime))

logger.log("getting getFilesWithTempDirLocations")
filesWithDirs = getFilesWithTempDirLocations(db)
logger.log("filesWithTempDirs has %d entries" % len(filesWithDirs))
logger.log("time: %s" % str(time.time() - startTime))

# do not delete any files with dir locations
filesToRemove = filesToRemove - filesWithDirs
logger.log("now %d files to remove" % len(filesToRemove))
logger.log("time: %s" % str(time.time() - startTime))

# mark unused as do not need any more
filesWithDirs = None

logger.log("removing files from disk")
removeFilesFromDisk(db, filesToRemove)

logger.log("time: %s" % str(time.time() - startTime))

logger.log("removing files from db tables")
#removeFilesFromFilesTable(db, filesToRemove)

logger.log("time: %s" % str(time.time() - startTime))

getCounts(db, logger)
logger.log("time: %s" % str(time.time() - startTime))

