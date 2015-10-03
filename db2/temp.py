
# from mainGetLargestFileEtc, not sure if need to refactor, so may throw this away
# gets largest file and deletes
# comment out delete for first run, uncomment if want to delete

import os.path
import os

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

def getNewFileInfo(db, filehash):
	command = "select * from %s where filehash = '%s';" % (DbSchema.newFilesTable, filehash)
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


def logInfoForFile(db, filehash, logger):
	newFileInfo = getNewFileInfo(db, filehash)
	logger.log("new file info: %s" % str(newFileInfo))
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


def logInfoForLargestFile(db, logger):
	largestRow = miscQueries.getLargestFile(db)
	logger.log("file info: %s" % str(largestRow))

	filehash = largestRow[0]

	logInfoForFile(db, filehash, logger)

	return filehash

def checkIfFileOnDisk(filehash):
	pass

# assumes removed from disk
def removeFileInfoFromDb(filehash):
	pass


def deleteFileFromDisk(filehash, logger):
	depotRoot1 = "I:\\objectstore1"
	depotRoot2 = "F:\\objectstore2"
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


def deleteDirEntries(db, dirhash, logger):
	logger.log("deleting dir %s" % dirhash)
	# delete from originalDirectoriesForFile
	command = "delete from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, dirhash)
	db.ExecuteNonQuerySql(command)
	command = "delete from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoriesTable, dirhash)
	db.ExecuteNonQuerySql(command)



def setToRemoveStatusForFilesInDirUsingCache(db, dirhash, logger, origDirsForFileDict, origFilesForDirDict):

	filelist = origFilesForDirDict.get(dirhash)
	if not filelist:
		deleteDirEntries(db, dirhash, logger)
		logger.log("no files found for this dir")
		return

	for filehash, filename in filelist:
		logger.log("filename: %s" % filename)

		dirs = origDirsForFileDict[filehash]

		if len(dirs) > 1:
			logger.log("multiple locations, not deleting file")
			origDirsForFileDict[filehash].remove(dirhash)
			continue

		for newdirhash in dirs:
			if dirhash != newdirhash:
				logger.log("dirhash does not match, not deleting")
				continue

		logger.log("set toremove status for %s" % filehash)
		miscQueries.setFileStatus(db, filehash, "toRemove")
		origDirsForFileDict[filehash] = None

	deleteDirEntries(db, dirhash, logger)


def deleteFilesInDirUsingCache(db, dirhash, logger, origDirsForFileDict = None, origFilesForDirDict = None):

	if not origDirsForFileDict:
		logger.log("not supported as need to check code, exiting")
		exit(1)

	if not origFilesForDirDict:
		logger.log("not supported as need to check code, exiting")
		exit(1)
		
	filelist = origFilesForDirDict.get(dirhash)
	if not filelist:
		deleteDirEntries(db, dirhash, logger)
		logger.log("no files found for this dir")
		return

	for filehash, filename in filelist:
		logger.log("filename: %s" % filename)

		dirs = origDirsForFileDict[filehash]

		if len(dirs) > 1:
			logger.log("multiple locations, not deleting file")
			origDirsForFileDict[filehash].remove(dirhash)
			continue

		for newdirhash in dirs:
			if dirhash != newdirhash:
				logger.log("dirhash does not match, not deleting")
				continue

		logger.log("deleting file %s" % filehash)
		deleteFileFromDisk(filehash, logger)
		origDirsForFileDict[filehash] = None
		logger.log("setting status to deleted")
		miscQueries.setFileStatus(db, filehash, "deleted")

	deleteDirEntries(db, dirhash, logger)


def extractFile(db, destinationDir, filehash, newFilename, logger):
	depotRoot1 = "I:\\objectstore1"
	depotRoot2 = "F:\\objectstore2"
	if not FileUtils.CopyFileFromDepot(db, depotRoot1, destinationDir, filehash, newFilename):
		if not FileUtils.CopyFileFromDepot(db, depotRoot2, destinationDir, filehash, newFilename):
			logger.log("ERROR, file not found")
			exit(1)


def extractFileOrSetNotFound(db, destinationDir, filehash, newFilename, logger):
	depotRoot1 = "I:\\objectstore1"
	depotRoot2 = "F:\\objectstore2"
	if not FileUtils.CopyFileFromDepot(db, depotRoot1, destinationDir, filehash, newFilename):
		if not FileUtils.CopyFileFromDepot(db, depotRoot2, destinationDir, filehash, newFilename):
			logger.log("file not found, setting status")
			miscQueries.setFileStatus(db, filehash, "notFound")

def extractFileOrSetDeleted(db, destinationDir, filehash, newFilename, logger):
	depotRoot1 = "I:\\objectstore1"
	depotRoot2 = "F:\\objectstore2"
	if not FileUtils.CopyFileFromDepot(db, depotRoot1, destinationDir, filehash, newFilename):
		if not FileUtils.CopyFileFromDepot(db, depotRoot2, destinationDir, filehash, newFilename):
			logger.log("file not found, setting status")
			miscQueries.setFileStatus(db, filehash, "deleted")


def deleteAllFilesFromDirAndSubdirs(db, logger, dirpathRoot):
	logger.log("deleting files from %s" % dirpathRoot)

	origDirsForFileDict, origFilesForDirDict = miscQueries.cacheOrigDirsForFileTable(db)

	dirlist = miscQueries.getDirectoriesContainingString(db, dirpathRoot)
	logger.log("got %d dirs" % len(dirlist))
	for dirhash, dirpath in dirlist:
		logger.log("deleting %s: %s" % (dirhash, dirpath))
		deleteFilesInDirUsingCache(db, dirhash, logger, origDirsForFileDict, origFilesForDirDict)


def setToDeleteAllFilesFromDirAndSubdirs(db, logger, dirpathRoot):
	logger.log("removing files from %s" % dirpathRoot)

	origDirsForFileDict, origFilesForDirDict = miscQueries.cacheOrigDirsForFileTable(db)

	dirlist = miscQueries.getDirectoriesContainingString(db, dirpathRoot)
	logger.log("got %d dirs" % len(dirlist))

	for dirhash, dirpath in dirlist:
		'''
		if "collage" in dirpath.lower():
			logger.log("Skipping %s" % dirpath)
			continue
		if "contentmanager" in dirpath.lower():
			logger.log("Skipping %s" % dirpath)
			continue
		if "\\MPV" in dirpath:
			logger.log("Skipping %s" % dirpath)
			continue
		if "\\mpv" in dirpath.lower():
			logger.log("lowercase mpv!!! %s" % dirpath)
			exit(1)

		'''



		logger.log("setting status for %s: %s" % (dirhash, dirpath))
		setToRemoveStatusForFilesInDirUsingCache(db, dirhash, logger, origDirsForFileDict, origFilesForDirDict)



def extractAllFilesFromDirAndSubdirs(db, logger, extractDir, dirpathRoot):
	depotRoot1 = "I:\\objectstore1"
	depotRoot2 = "F:\\objectstore2"
	logger.log("extracting files from %s to %s" % (dirpathRoot, extractDir))
	dirfilelist = miscQueries.getAllPathsAndFilenamesFromDirAndSubdirs(db, dirpathRoot)
	logger.log("got %d dirs" % len(dirfilelist.keys()))

	for dirpath in dirfilelist:
		logger.log("directory %s" % dirpath)
		filelist = dirfilelist[dirpath]
		logger.log("%d files" % len(filelist))
		for filehash, filename in filelist:
			logger.log("filehash: %s, filename: %s" % (filehash, filename))
			fixedDirpath = dirpath.replace(":", "_")
			logger.log(fixedDirpath)
			destinationDir = os.path.join(extractDir, fixedDirpath)
			logger.log("destination: %s" % destinationDir)
			if not os.path.isdir(destinationDir):
				os.makedirs(destinationDir)

			if not FileUtils.CopyFileFromDepot(db, depotRoot1, destinationDir, filehash, filename):
				if not FileUtils.CopyFileFromDepot(db, depotRoot2, destinationDir, filehash, filename):
					logger.log("#############################file not found, setting status")
					miscQueries.setFileStatus(db, filehash, "notFound")


def logInfoForFirstCorruptFile(db, logger):
	list = miscQueries.getFilesWithStatus(db, "corrupted", 1)
	filehash = list[0][0]
	logger.log(filehash)
	logInfoForFile(db, filehash, logger)
	return filehash

def deleteFile(db, filehash, logger):
	deleteFileFromDisk(filehash, logger)
	miscQueries.setFileStatus(db, filehash, "deleted")
	#logInfoForLargestFile(db, logger)
	logInfoForFirstCorruptFile(db, logger)

def keep(db, filehash, logger):
	miscQueries.setFileStatus(db, filehash, "keepForNow")
	logInfoForLargestFile(db, logger)


def markToReplace(db, filehash, logger):
	miscQueries.setFileStatus(db, filehash, "toReplace")
	logInfoForFirstCorruptFile(db, logger)


def moveOutCorruptedFilesFromDepot(db, logger):
	depotRootPath = "F:\\objectstore2"
	destinationDirPath = "F:\\corruptFiles"

	list = miscQueries.getFilesWithStatus(db, "corrupted")
	for item in list:
		filehash = item[0]
		logger.log(filehash)

		moved = FileUtils.MoveFileFromDepot(db, depotRootPath, destinationDirPath, filehash, filehash)
		if moved:
			logger.log("moved %s" % filehash)
			miscQueries.setFileStatus(db, filehash, "toReplace")


 


def getFilepathInDepot(filehash):
	depotRoot1 = "I:\\objectstore1"
	depotRoot2 = "F:\\objectstore2"

	filepath = FileUtils.getPathOfFileInDepot(depotRoot1, filehash)
	if not filepath:
		filepath = FileUtils.getPathOfFileInDepot(depotRoot2, filehash)
	return filepath



logger = DbLogger.dbLogger()

drive1 = "I:"
drive2 = "F:"


#spaceForDepot1 = FileUtils.windowsSpecificGetFreeSpace(drive1)
#logger.log("drive for %s has: %d free" % (drive1, spaceForDepot1) )

#spaceForDepot2 = FileUtils.getFreeSpaceOnDepotDrive(drive2)
#logger.log("drive for %s has: %d free" % (drive2, spaceForDepot2) )

dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/db/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)

filehash = "9BDE6885D922CCA4C558BE498CD612DC90FFC5EF"
logInfoForFile(db, filehash, logger)
#deleteFile(db, filehash, logger)
#markToReplace(db, filehash, logger)

#dirhash = "3ECDD248EDCA191B2E618BE83E04947C77EE120C"
#miscQueries.deleteDirectoryEntryForFile(db, filehash, dirhash)

#
#deleteFilesInDir(db, "C6DCDC25FCC06BC5F87CE220A217B02577970B71", logger)

dirpathList = [

	]

#for dirpath in dirpathList:
#	setToDeleteAllFilesFromDirAndSubdirs(db, logger, dirpath)



dirpath  = "E:\\tempBackup_xEtc"


#extractAllFilesFromDirAndSubdirs(db, logger,"F:\\extract",dirpath )

setToDeleteAllFilesFromDirAndSubdirs(db, logger, dirpath)
#deleteAllFilesFromDirAndSubdirs(db, logger, "J:\\m2drive\\m4\\v2\\MicrosfotWorkToSort\\macallan\\private")
#deleteAllFilesFromDirAndSubdirs(db, logger, dirpath)

#filehash = logInfoForLargestFile(db, logger)

#keep(db, filehash, logger)
#deleteFile(db, filehash, logger)

#extractFileOrSetDeleted(db, "F:\\extract", filehash, filehash, logger)
#extractFileOrSetNotFound(db, "F:\\extract", filehash, filehash, logger)
#extractFile(db, "F:\\extract", filehash, filehash, logger)


#filehash = "E3ED49B8581DE969A2FF2FC0B5DBB07B6FEBBB7C"
#miscQueries.setFileStatus(db, filehash, "toRemoveCompletely")
#miscQueries.setFileStatus(db, filehash, "notFound")
#
