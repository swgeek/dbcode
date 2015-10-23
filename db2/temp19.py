# imports files into the db

import os.path

import FileUtils
import DbLogger
import CoreDb
import Sha1HashUtilities
import miscQueries
import DbSchema


def getFilehashListFromDirPath(rootDirPath, logger, excludeList = []):
	filehashList = []
	fileListing = FileUtils.getListOfAllFilesInDir(rootDirPath)
	logger.log("%d entries" % len(fileListing))
	for dirpath, filename in fileListing:
		if filename in excludeList:
			logger.log("skipping %s" % filename)
			continue

		filepath = os.path.join(dirpath, filename)
		logger.log("%s" % filepath)
		filehash = Sha1HashUtilities.HashFile(filepath)
		filehash = filehash.upper()
		filehashList.append((filehash, dirpath, filename))

	return filehashList



logger = DbLogger.dbLogger()
dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/db/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)

rootDirPath  = u"F:\\fromSeagate"


#excludeList = ["custom.css", "logo.png"]
filehashAndPathList = getFilehashListFromDirPath(rootDirPath, logger)

logger.log("%d files" % len(filehashAndPathList))
count = 0

depotRootPath = "E:\\objectstore2"
for filehash, dirpath, filename in filehashAndPathList:
	count += 1
	logger.log("%d: %s:%s:%s" % (count, filehash, dirpath, filename))

	filestatus = "notFound"
	fileInfo = miscQueries.getFileInfo(db, filehash)
	if fileInfo:
		filestatus = fileInfo[3]

	if filestatus == "notFound":
		logger.log("\tcopying file into depot")
		filepath = os.path.join(dirpath, filename)
		FileUtils.CopyFileIntoDepot(depotRootPath, filepath, filehash, logger)
		if not fileInfo:
			filesize = os.path.getsize(filepath)
			logger.log("adding to files table")
			miscQueries.insertFileEntry(db, filehash, filesize, 1)

	newDirPath = dirpath.replace("F:", "D:")
	dirhash = Sha1HashUtilities.HashString(newDirPath)
	if not miscQueries.checkIfDirhashInDatabase(db, dirhash):
		logger.log("\tnew dir path %s" % newDirPath)
		miscQueries.insertDirHash(db, dirhash, newDirPath)

	if not miscQueries.checkIfFileDirectoryInDatabase(db, filehash, filename, dirhash):
		miscQueries.insertOriginalDir(db, filehash, filename, dirhash)



