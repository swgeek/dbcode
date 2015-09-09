# probably more efficient to batch this somehow
# doing it individually allows me to break it partway and pick up where I left off
# maybe create filelist?
import CoreDb
import DbLogger
import HashFilesEtc
import os
import miscQueries
import FileUtils
import Sha1HashUtilities

db = CoreDb.CoreDb("C:\\depotListing\\listingDb.sqlite")
logger = DbLogger.dbLogger()

dirpath = "H:\\fromAir_20150809"
depotRoot = "I:\\objectstore1"

depotInfo = miscQueries.getDepotInfo(db)
logger.log("depotInfo:")
for entry in depotInfo:
	logger.log(entry)
depotId = 1 # hardcode for now

dirsAddedToDb = 0
filesAdded = 0
fileLocationsAdded = 0
dirsAlreadyInDb = 0
filesAlreadyInDb = 0
fileLocationsAlreadyInDb = 0

filesToAdd = HashFilesEtc.getListOfFilesInDirAndSubdirs(dirpath, logger)
logger.log("got: %d files" % len(filesToAdd))

# add directories to database
allDirs = set()
for filename, dirpath, filehash in filesToAdd:
	allDirs.add(dirpath)

for dirpath in allDirs:
	logger.log("inserting directory %s" % dirpath)
	dirhash = Sha1HashUtilities.HashString(dirpath).upper()
	if miscQueries.checkIfDirhashInDatabase(db, dirhash):
		logger.log(" dir %s already in database, not adding" % dirpath)
		dirsAlreadyInDb += 1
	else:
		logger.log(" inserting dir %s" % dirpath)
		miscQueries.insertDirHash(db, dirhash, dirpath)
		dirsAddedToDb += 1


for filename, dirpath, filehash in filesToAdd:
	logger.log("adding file: %s, %s, %s" % (filename, dirpath, filehash))

	filepath = os.path.join(dirpath, filename)
	filehash = filehash.upper()

	# if file not already in depot/database, insert
	if miscQueries.checkIfFilehashInDatabase(db, filehash):
		logger.log("already in database")
		filesAlreadyInDb += 1
	else:
		logger.log("not in database, need to add")
		FileUtils.CopyFileIntoDepot(depotRoot, filepath, filehash, logger)
		filesize = os.path.getsize(filepath)
		logger.log("adding to files table")
		miscQueries.insertFileEntry(db, filehash, filesize, depotId)
		filesAdded += 1

	# enter directory info for file
	# hate to hash dirpath again and again - save from earlier? or not worth the savings?
	dirhash = Sha1HashUtilities.HashString(dirpath).upper()

	# add filehash, filename, dirhash to originalDirectoryForFile
	if miscQueries.checkIfFileDirectoryInDatabase(db, filehash, filename, dirhash):
		logger.log(" original dir %s for %s already in database, not adding" % (dirpath, filename))
		fileLocationsAlreadyInDb += 1
	else:
		logger.log(" inserting original dir %s for %s" % (dirpath, filename))
		miscQueries.insertOriginalDir(db, filehash, filename, dirhash)
		fileLocationsAdded += 1

logger.log("done")

logger.log("added %d dirs" % dirsAddedToDb)
logger.log("added %d files" % filesAdded)
logger.log("added %d fileLocations" % fileLocationsAdded)
logger.log(" %d dirs already in db" % dirsAlreadyInDb)
logger.log(" %d files already in db" % filesAlreadyInDb)
logger.log(" %d fileLocations already in db" % fileLocationsAlreadyInDb)

