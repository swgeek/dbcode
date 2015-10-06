# gets all files in a directory, finds every instance of each file that is already in the depot and lists the corresponding directores in db

import os.path

import FileUtils
import DbLogger
import CoreDb
import Sha1HashUtilities
import miscQueries
import DbSchema

def getOriginalDirPathForDirHash(db, dirhash):
	command = "select dirPath from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoriesTable, dirhash)
	result = db.ExecuteSqlQueryForSingleString(command)
	return result


def getOriginalDirectoriesForFile(db, filehash):
	command = "select * from %s where filehash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, filehash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	return results


def getFilehashListFromDirPath(rootDirPath, logger, excludeList = []):
	filehashList = []
	fileListing = FileUtils.getListOfAllFilesInDir(rootDirPath)
	for dirpath, filename in fileListing:
		if filename in excludeList:
			logger.log("skipping %s" % filename)
			continue
		filepath = os.path.join(dirpath, filename)
		logger.log("%s" % filepath)
		filehash = Sha1HashUtilities.HashFile(filepath)
		filehash = filehash.upper()
		filehashList.append((filehash, filepath))

	return filehashList


def checkIfFilesInDbAndListDirs(db, fileList, logger):
	logger.log("%d files" % len(fileList))

	dirSet = set()

	for filehash, filepath in fileList:
		logger.log("%s: %s" % (filehash, filepath))
		if miscQueries.checkIfFilehashInDatabase(db, filehash):
			logger.log("%s exists in database, getting directories" % filepath)
			dirlist = getOriginalDirectoriesForFile(db, filehash)
			for dirInfo in dirlist:
				logger.log("\t %s" % str(dirInfo))
				dirhash = dirInfo[2]
				dirSet.add(dirhash)
		else:
			logger.log("\t not in database")


	return dirSet


def getFilesInDirInDepot(db, dirhash, logger):
	filehashSet = set()
	fileInfoList = miscQueries.getAllFilesFromDir(db, dirhash)
	for fileInfo in fileInfoList:
		logger.log("\t\t %s" % str(fileInfo))
		filehash = fileInfo[0]
		filehashSet.add(filehash)
	return filehashSet




logger = DbLogger.dbLogger()
dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/db/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)

rootDirPath  = "E:\\20150411_AnDinnerThinkFinal"

excludeList = ["custom.css", "logo.png"]
filehashAndPathList = getFilehashListFromDirPath(rootDirPath, logger)
dirSet = checkIfFilesInDbAndListDirs(db, filehashAndPathList, logger)

allFilesInDbSet = set()

logger.log("all directories:")
for dirhash in dirSet:
	dirpath = getOriginalDirPathForDirHash(db, dirhash)
	logger.log("\t%s: %s" % (dirhash, dirpath))
	allFilesInDbSet = allFilesInDbSet.union( getFilesInDirInDepot(db, dirhash, logger) )

filehashList = [x[0] for x in filehashAndPathList]
filehashSet = set(filehashList)

diff1 = allFilesInDbSet - filehashSet
if diff1:
	logger.log("these files are in db but not on list")
	logger.log(str(diff1))
else:
	logger.log("no new files in db")



