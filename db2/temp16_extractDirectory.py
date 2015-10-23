
# extract everything from specified directory


import os.path
import os

import miscQueries
import CoreDb
import DbLogger
import time
import DbSchema
import FileUtils


def extractAllFilesFromDirAndSubdirs(db, logger, extractDir, dirpathRoot):
	depotRoot1 = "I:\\objectstore1"
	depotRoot2 = "E:\\objectstore2"
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


logger = DbLogger.dbLogger()
dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/db/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)

dirpath  = ""	
dirpath  = ""	
dirpath  = ""	
dirpath  = ""	
dirpath  = ""	
dirpath  = "A:\\fromAir_20151012\\2015\\2015-05-14"	


extractDir = "E:\\extract"
extractAllFilesFromDirAndSubdirs(db, logger,extractDir,dirpath )

