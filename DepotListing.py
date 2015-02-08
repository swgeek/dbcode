
import DbInterface
import os.path
import SHA1HashUtilities
import DepotInterface

# BIG NOTE: moved stuff to DepotInterface, need to update code below. Will do that next time I use it.

databaseFilePathName = "C:\\depotListing\\listingDb.sqlite"

#createListingDb("C:\\depotListing\\listingDb.sqlite")

db = DbInterface.DbInterface(databaseFilePathName)


# depot 1 in db
#depotPath = "G:\\objectstore1"
#depotId =  newDepotEntry(db, "main store, 4tb drive", depotPath)

#depot 2 in db
#depotPath = "H:\objectstore2"
#depotId =  newDepotEntry(db, "newest store, 750gb drive", depotPath)

#depot 4 in db
#depotPath = "H:\\backup1a"
#depotId =  newDepotEntry(db, "backup1a, 1GB drive", depotPath)

#depot 5 in db
#depotPath = "H:\\backup1b"
#depotId =  newDepotEntry(db, "backup1b, 1TB drive", depotPath)

#depot 6 in db
#depotPath = "H:\\backup1c"
#depotId =  newDepotEntry(db, "backup1c, 1TB drive", depotPath)

#depot 7 in db
#depotPath = "H:\\deletedFiles"
#depotId =  newDepotEntry(db, "deletedFiles, 2TB drive", depotPath)

#depot 8 in db
#depotPath = "H:\\deletedFilesBackup1"
#depotId =  newDepotEntry(db, "deletedFiles backup, 2TB drive", depotPath)

#depot 9 in db
#depotPath = "H:\\objectStore1Backup"
#depotId =  newDepotEntry(db, "deletedFiles backup, 2TB drive", depotPath)

#depot 10 in db
#depotPath = "F:\objectstore2Backup"
#depotId =  newDepotEntry(db, "new store backup, 750G drive", depotPath)

#depot 11 in db
depotPath = "F:\\moreBackup"
#depotId =  newDepotEntry(db, "new store backup, 750G drive", depotPath)

depotId = 11
print "depotid is %d" % depotId


db.startTransaction()

for dirName in os.listdir(depotPath):
	dirPath = os.path.join(depotPath, dirName)
	print "processing %s" % dirPath
	for fileName in os.listdir(dirPath):
		partialFileName, fileExtension = os.path.splitext(fileName)
		if fileExtension != "":
			print "Error: file %s has extension %s" % (fileName, fileExtension)
			exit(1)
		# get filesize
		# add into database

		filesize = os.path.getsize(os.path.join(dirPath, fileName))
		newFileListingEntryAsPartOfTransaction(db, fileName, depotId, filesize)
		#incrementLocationCountWithinTransaction(db, fileName)

db.EndTransaction()

getCountForEachDepot(db)

getCountOfLocationCounts(db)

createLocationCountTable(db)
initializeLocationCounts(db)
getCountOfLocationCounts(db)
