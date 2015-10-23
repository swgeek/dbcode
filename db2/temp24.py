
# delete files in second depot that already exist in first depot

import os
import shutil

import CoreDb
import DbLogger
import miscQueries
import DbSchema
import FileUtils
import Sha1HashUtilities


def DeleteFileIfExistsInMainDepot(depotRootPath, secondaryFilePath, filehash, logger):
		subdir = filehash[0:2]
		dirpath = os.path.join(depotRootPath, subdir)
		filepath = os.path.join(depotRootPath, subdir, filehash)

		if os.path.isfile(filepath):
			logger.log( "%s already exists, deleting second copy" % filepath )
			filesize1 = filesize = os.path.getsize(filepath)
			filesize2 = filesize = os.path.getsize(secondaryFilePath)
			if filesize1 != filesize2:
				logger.log("error: filesizes do not match!")
				exit(1)
			logger.log("deleting %s" % secondaryFilePath)
			os.remove(secondaryFilePath)
		else:
			print ".",


dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/db/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)

logger = DbLogger.dbLogger()

mainDepotRoot = "I:\\objectstore1"
secondDepotRoot = "E:\\objectstore2"

subdirlist = os.listdir(secondDepotRoot)

for subdir in subdirlist:
	# skip non directories
	subdirpath = os.path.join(secondDepotRoot, subdir)
	if not os.path.isdir(subdirpath):
		continue

	logger.log("directory %s" % subdirpath)

	filelist = os.listdir(subdirpath)

	for filename in filelist:
		# TODO: check if filehash and not random file
		filepath = os.path.join(subdirpath, filename)
		DeleteFileIfExistsInMainDepot(mainDepotRoot, filepath, filename, logger)


