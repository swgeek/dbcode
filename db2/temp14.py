# gets all files in a directory, sets corresponding files to status "toDeleteCompletely" if they exist in database

import os.path

import FileUtils
import DbLogger
import CoreDb
import Sha1HashUtilities
import miscQueries

logger = DbLogger.dbLogger()
dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/db/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)

rootDirPath  = "E:\\20141115_hss"

filelist = FileUtils.getListOfAllFilesInDir(rootDirPath)

logger.log("%d files" % len(filelist))

for dirpath, filename in filelist:
	logger.log("%s: %s" % (dirpath, filename))
	filepath = os.path.join(dirpath, filename)
	filehash = Sha1HashUtilities.HashFile(filepath)
	filehash = filehash.upper()
	if miscQueries.checkIfFilehashInDatabase(db, filehash):
		logger.log("\t exists in database, set to delete")
		miscQueries.setFileStatus(db, filehash, "toDeleteCompletely")
	else:
		logger.log("\t not in database")
