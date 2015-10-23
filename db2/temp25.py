# finds files in depot that are not in files table and makes an entry into the filestable
# not quite an import as does not make directory entry, just files table.
# NOTE: will need another pass to either find directory entry or mark as orphan

import os.path

import FileUtils
import DbLogger
import CoreDb
import Sha1HashUtilities
import miscQueries
import DbSchema


dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/db/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)

logger = DbLogger.dbLogger()

depotRoot = "E:\\objectstore2"


# get entire list of files in db
command = "select filehash from %s;" % (DbSchema.newFilesTable)
result = db.ExecuteSqlQueryReturningMultipleRows(command)
filesInDb = [x[0] for x in result]
filesInDbset = set(filesInDb)

subdirlist = os.listdir(depotRoot)

for subdir in subdirlist:
	# skip non directories
	subdirpath = os.path.join(depotRoot, subdir)
	if not os.path.isdir(subdirpath):
		continue

	logger.log("directory %s" % subdirpath)

	filelist = os.listdir(subdirpath)

	for filehash in filelist:
		# TODO: check if filehash and not random file
		if filehash not in filesInDbset:
			logger.log("found orphan: %s" % filehash)
			filepath = os.path.join(subdirpath, filehash)
			filesize = os.path.getsize(filepath)
			logger.log("\tadding %s to files table" % filehash)
			miscQueries.insertFileEntry(db, filehash, filesize, 1)

