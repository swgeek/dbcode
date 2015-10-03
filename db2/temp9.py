# delete everything under a particular directory
# from mainGetLargestFileEtc, not sure if need to refactor, so may throw this away
# gets largest file and deletes
# comment out delete for first run, uncomment if want to delete

import os.path

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


# some problems here if same file with two names in a directory.
# think this deletes the directory but not the file
# check
def setToRemoveStatusForFilesInDirUsingCache(db, dirhash, logger, origDirsForFileDict, origFilesForDirDict):

	filelist = origFilesForDirDict.get(dirhash)
	if not filelist:
		deleteDirEntries(db, dirhash, logger)
		logger.log("no files found for this dir")
		return

	for filehash, filename in filelist:
		logger.log("filehash: %s, filename: %s" % (filehash, filename))

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


def setToDeleteAllFilesFromDirAndSubdirs(db, logger, dirpathRoot):
	logger.log("removing files from %s" % dirpathRoot)

	origDirsForFileDict, origFilesForDirDict = miscQueries.cacheOrigDirsForFileTable(db)

	dirlist = miscQueries.getDirectoriesContainingString(db, dirpathRoot)
	logger.log("got %d dirs" % len(dirlist))
	for dirhash, dirpath in dirlist:
		logger.log("setting status for %s: %s" % (dirhash, dirpath))
		setToRemoveStatusForFilesInDirUsingCache(db, dirhash, logger, origDirsForFileDict, origFilesForDirDict)




logger = DbLogger.dbLogger()
dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/db/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)


'''
filehash = "3D0C99C51F9B7C359CC103A4B7D82C80C5035235"
info = getNewFileInfo(db, filehash)
logger.log(str(info))

info = getOriginalDirectoriesForFile(db, filehash)
logger.log(str(info))

'''
dirpath = "C:\\Users\\m_000\\Desktop\\m4\\VSProjects_thinkMostlyPlayTemp\\ConsoleApplication1"
#dirhash = "E11C8B09C26E8116361F8E08E6A18FE83B5B7043"
#dirpath = getOriginalDirPathForDirHash(db, dirhash)
#logger.log(dirpath)
#exit(1)
setToDeleteAllFilesFromDirAndSubdirs(db, logger, dirpath)
