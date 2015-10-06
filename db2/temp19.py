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

rootDirPath  = "E:\\20150411_AnDinnerThinkFinal"

#excludeList = ["custom.css", "logo.png"]
filehashAndPathList = getFilehashListFromDirPath(rootDirPath, logger)

depotRootPath = "I:\objectstore1"
for filehash, dirpath, filename in filehashAndPathList:
	logger.log("%s:%s:%s" % (filehash, dirpath, filename))
	filepath = os.path.join(dirpath, filename)
	logger.log(filepath)
	filesize = os.path.getsize(filepath)
	newDirPath = dirpath.replace("E:", "K:")
	dirhash = Sha1HashUtilities.HashString(newDirPath)
	if not miscQueries.checkIfDirhashInDatabase(db, dirhash):
		logger.log("new dir path %s" % newDirPath)
		miscQueries.insertDirHash(db, dirhash, newDirPath)
	FileUtils.CopyFileIntoDepot(depotRootPath, filepath, filehash, logger)
	logger.log("adding to files table")
	if not miscQueries.checkIfFileDirectoryInDatabase(db, filehash, filename, dirhash):
		miscQueries.insertOriginalDir(db, filehash, filename, dirhash)
	miscQueries.insertFileEntry(db, filehash, filesize, 1)

