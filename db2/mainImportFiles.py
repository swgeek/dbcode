
import CoreDb
import miscQueries
import CopyFilesEtc
import HashFilesEtc
import DbLogger
import Sha1HashUtilities
import os

# TODO: get rid of filesize from files and filelisting. 
# have only status in files, rename to filestatus
# have only depot location in listing. Can have multiple entries here!
# have new table with filesize
# should I also have hash for filename? Then can use that in originalDirectory for file.
# option. Use filesize as part of name, e.g. filehash_filesize. not sure on this one.
# advantage: easy to check if file copied properly. Probably better not to, have database table for size and use that.


db = CoreDb.CoreDb("C:\\depotListing\\listingDb.sqlite")
logger = DbLogger.dbLogger()

dirpath = "H:\\fromDesktop"

objectStores = miscQueries.getAllObjectStores(db)
logger.log(objectStores)

# quick test of 
# should I use depotId instead of path? probably better
depotRoot = "H:\\objectstore2"
#depotId = miscQueries.GetObjectStoreId(db, depotRoot)
depotId = 2
logger.log("got depotId %d" % depotId)

filesToAdd = HashFilesEtc.getListOfFilesInSubdir(dirpath, logger)
logger.log(filesToAdd)

# probably more efficient to batch this somehow
# doing it individually allows me to break it partway and pick up where I left off
# find better way to do this, probably have to move to postgres though
for filename, dirpath, filehash in filesToAdd:

	logger.log("handling %s, %s, %s" % (filename, dirpath, filehash))

	filepath = os.path.join(dirpath, filename)

	filehash = filehash.upper()

	if miscQueries.checkIfFilehashInDatabase(db, filehash):
		logger.log("already in database")
	else:
		logger.log("not in database, need to add")
		CopyFilesEtc.CopyFileIntoDepot(depotRoot, filepath, filehash, logger)
		filesize = os.path.getsize(filepath)

		if miscQueries.checkIfFileListingEntryInDatabase(db, filehash, depotId):
			logger.log("already in filelisting, not adding")
		else:
			logger.log("adding to filelisting")
			miscQueries.insertFileListing(db, filehash, depotId, filesize)

		logger.log("adding to files table")
		miscQueries.insertFileEntry(db, filehash, filesize)

	# can make this more efficient by putting directories in at earlier stage, so 
	# do not repeatly try to insert same dir

	# add dirhash, dirpath to originalDirectories
	dirhash = Sha1HashUtilities.HashString(dirpath).upper()
	if miscQueries.checkIfDirhashInDatabase(db, dirhash):
		logger.log(" dir %s already in database, not adding" % dirpath)
	else:
		logger.log(" inserting dir %s" % dirpath)
		miscQueries.insertDirHash(db, dirhash, dirpath)

	# add filehash, filename, dirhash to originalDirectoryForFile
	if miscQueries.checkIfFileDirectoryInDatabase(db, filehash, filename, dirhash):
		logger.log(" original dir %s for %s already in database, not adding" % (dirpath, filename))
	else:
		logger.log(" inserting original dir %s for %s" % (dirpath, filename))
		miscQueries.insertOriginalDir(db, filehash, filename, dirhash)

logger.log("done")
print "done"
