
# from mainGetLargestFileEtc, not sure if need to refactor, so may throw this away
# gets largest file and deletes
# comment out delete for first run, uncomment if want to delete

import miscQueries
import CoreDb
import DbLogger
import time
import DbSchema
import FileUtils

def getOldFileInfo(db, filehash):
	command = "select * from %s where filehash = '%s';" % (DbSchema.oldFilesTable, filehash)
	result = db.ExecuteSqlQueryReturningSingleRow(command)
	return result

def getOriginalDirPathForDirHash(db, dirhash):
	command = "select dirPath from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoriesTable, dirhash)
	result = db.ExecuteSqlQueryForSingleString(command)
	return result


def getOriginalDirectoriesForFile(db, filehash):
	command = "select * from %s where filehash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, filehash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	return results


def logInfoForLargestFile(db, logger):
	largestRow = miscQueries.getLargestFile(db)
	logger.log("file info: %s" % str(largestRow))

	filehash = largestRow[0]

	oldFileInfo = getOldFileInfo(db, filehash)
	logger.log("old file info: %s" % str(oldFileInfo))

	dirs = getOriginalDirectoriesForFile(db, filehash)
	logger.log("original dirs:")

	for row in dirs:
		logger.log("\trow: %s" % (str(row)))
		dirhash = row[2]
		dirpath = getOriginalDirPathForDirHash(db, dirhash)
		logger.log("\tpath:%s" % dirpath)
		# following format only as delete dir utility expects it. Maybe change
		#logger.log("(\"%s\",\"%s\",0)," % (dirhash, dirpath))
		logger.log("")
		continue

	return filehash

def checkIfFileOnDisk(filehash):
	pass

# assumes removed from disk
def removeFileInfoFromDb(filehash):
	pass


def deleteFileFromDisk(filehash, logger):
	depotRoot1 = "I:\\objectstore1"
	depotRoot2 = "H:\\objectstore2"
	deleted = FileUtils.DeleteFileFromDepot(depotRoot1, filehash)
	if deleted:
		logger.log("deleted %s from %s" % (filehash, depotRoot1))

	deleted = FileUtils.DeleteFileFromDepot(depotRoot2, filehash)
	if deleted:
		logger.log("deleted %s from %s" % (filehash, depotRoot2))



def getFilesFromDir(db, dirhash):
	command = "select filehash, filename from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, dirhash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	return results


def deleteFilesInDir(db, dirhash, logger):
	filelist = getFilesFromDir(db, dirhash)
	for filehash, filename in filelist:
		logger.log("deleting %s from disk" % filehash)
		logger.log("filename: %s" % filename)

		oldFileInfo = getOldFileInfo(db, filehash)
		logger.log("old file info: %s" % str(oldFileInfo))

		dirs = getOriginalDirectoriesForFile(db, filehash)
		logger.log("original dirs:")

		for row in dirs:
			logger.log("\trow: %s" % (str(row)))
			dirhash = row[2]
			dirpath = getOriginalDirPathForDirHash(db, dirhash)
			logger.log("\tpath:%s" % dirpath)
			logger.log("")
		
		deleteFileFromDisk(filehash, logger)
		miscQueries.setFileStatus(db, filehash, "deleted")


def extractFile(db, destinationDir, filehash, newFilename, logger):
	depotRoot1 = "I:\\objectstore1"
	depotRoot2 = "H:\\objectstore2"
	if not FileUtils.CopyFileFromDepot(db, depotRoot1, destinationDir, filehash, newFilename):
		if not FileUtils.CopyFileFromDepot(db, depotRoot2, destinationDir, filehash, newFilename):
			logger.log("ERROR, file not found")
			exit(1)


def extractFileOrSetNotFound(db, destinationDir, filehash, newFilename, logger):
	depotRoot1 = "I:\\objectstore1"
	depotRoot2 = "H:\\objectstore2"
	if not FileUtils.CopyFileFromDepot(db, depotRoot1, destinationDir, filehash, newFilename):
		if not FileUtils.CopyFileFromDepot(db, depotRoot2, destinationDir, filehash, newFilename):
			logger.log("file not found, setting status")
			miscQueries.setFileStatus(db, filehash, "notFound")

def extractFileOrSetDeleted(db, destinationDir, filehash, newFilename, logger):
	depotRoot1 = "I:\\objectstore1"
	depotRoot2 = "H:\\objectstore2"
	if not FileUtils.CopyFileFromDepot(db, depotRoot1, destinationDir, filehash, newFilename):
		if not FileUtils.CopyFileFromDepot(db, depotRoot2, destinationDir, filehash, newFilename):
			logger.log("file not found, setting status")
			miscQueries.setFileStatus(db, filehash, "deleted")


logger = DbLogger.dbLogger()

drive1 = "I:"
drive2 = "H:"

spaceForDepot1 = FileUtils.windowsSpecificGetFreeSpace(drive1)
logger.log("drive for %s has: %d free" % (drive1, spaceForDepot1) )

spaceForDepot2 = FileUtils.getFreeSpaceOnDepotDrive(drive2)
logger.log("drive for %s has: %d free" % (drive2, spaceForDepot2) )

dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/fmapp/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)

#deleteFilesInDir(db, "A16165790411D1745147EF941D60B8BBD4A2AA1D", logger)

filehash = logInfoForLargestFile(db, logger)

#extractFileOrSetDeleted(db, "H:\\extract", filehash, filehash, logger)
#extractFileOrSetNotFound(db, "H:\\extract", filehash, filehash, logger)
#extractFile(db, "H:\\extract", filehash, filehash, logger)
#deleteFileFromDisk(filehash, logger)
#miscQueries.setFileStatus(db, filehash, "deleted")

#filehash = "E3ED49B8581DE969A2FF2FC0B5DBB07B6FEBBB7C"
#miscQueries.setFileStatus(db, filehash, "toRemoveCompletely")
#miscQueries.setFileStatus(db, filehash, "keepForNow")
#miscQueries.setFileStatus(db, filehash, "notFound")
#
