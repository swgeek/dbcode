
# copy all files from one depot into another that do not exist in that depot
# hopefully temporary code, just to copy missing files one time

import os
import shutil

import CoreDb
import DbLogger
import miscQueries
import DbSchema
import FileUtils
import Sha1HashUtilities


def CopyFileIntoDepotIfDoesNotExist(depotRootPath, sourceFilePath, filehash, logger):
		subdir = filehash[0:2]
		destinationDirPath = os.path.join(depotRootPath, subdir)
		destinationFilePath = os.path.join(depotRootPath, subdir, filehash)

		if not os.path.isdir(destinationDirPath):
			os.mkdir(destinationDirPath)

		if os.path.isfile(destinationFilePath):
			logger.log( "%s already exists" % destinationFilePath )
		else:
			logger.log("copying %s to %s" % (sourceFilePath, destinationFilePath))
			shutil.copyfile(sourceFilePath, destinationFilePath)


dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/db/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)

logger = DbLogger.dbLogger()

destDepotRoot = "F:\\objectstore2Moved"
sourceDepotRoot = "E:\\objectstore2"

subdirlist = os.listdir(sourceDepotRoot)

for subdir in subdirlist:
	# skip non directories
	subdirpath = os.path.join(sourceDepotRoot, subdir)
	if not os.path.isdir(subdirpath):
		continue

	logger.log("directory %s" % subdirpath)

	filelist = os.listdir(subdirpath)

	for filename in filelist:
		# TODO: check if filehash and not random file
		filepath = os.path.join(subdirpath, filename)
		CopyFileIntoDepotIfDoesNotExist(destDepotRoot, filepath, filename, logger)


