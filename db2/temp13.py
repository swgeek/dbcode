
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


def extractFileOrSetNotFound(db, destinationDir, filehash, newFilename, logger):
	depotRoot1 = "I:\\objectstore1"
	depotRoot2 = "F:\\objectstore2"
	if not FileUtils.CopyFileFromDepot(db, depotRoot1, destinationDir, filehash, newFilename):
		if not FileUtils.CopyFileFromDepot(db, depotRoot2, destinationDir, filehash, newFilename):
			logger.log("ERROR: file not found, setting status notFound")
			miscQueries.setFileStatus(db, filehash, "notFound")





logger = DbLogger.dbLogger()
dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/db/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)

dirpathRoot = "E:\\tempBackup_xEtc"

extractDir = "F:\extract"

'''
dirhash = "C0CE47D7204E5DDDD5A2EAC9F228D3AB3C526357"
info = miscQueries.getDirectoryPath(db, dirhash)
logger.log("%s" % str(info))
info = miscQueries.getAllFilesFromDir(db, dirhash)
logger.log("%s" % str(info))
exit(1)
'''

logger.log("extracting files from %s to %s" % (dirpathRoot, extractDir))

origDirsForFileDict, origFilesForDirDict = miscQueries.cacheOrigDirsForFileTable(db)
origDirsForFileDict = None

dirDict = dict(miscQueries.getDirectoriesContainingString(db, dirpathRoot))
logger.log("got %d dirs" % len(dirDict.keys()))

for dirhash in dirDict:
	origDirPath = dirDict[dirhash]
	fixedDirpath = origDirPath.replace(":", "_")	

	logger.log("extracting directory %s: %s" % (dirhash, origDirPath))
	filelist = origFilesForDirDict.get(dirhash)
	if not filelist:
		continue

	for filehash, filename in filelist:
		logger.log("file: %s" % filename)
		destinationDir = os.path.join(extractDir, fixedDirpath)
		logger.log("destination: %s" % destinationDir)
		if not os.path.isdir(destinationDir):
			os.makedirs(destinationDir)
		extractFileOrSetNotFound(db, destinationDir, filehash, filename, logger)
