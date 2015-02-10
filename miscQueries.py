# queries I use to sort, e.g. get largest files

import DbInterface
import DepotInterface

databaseFilePathName = "C:\\depotListing\\listingDb.sqlite"
db = DbInterface.DbInterface(databaseFilePathName)


#reader = DepotInterface.getReaderForFilesWithFilenameStartingWithDotUnderscore(db)


#DepotInterface.createLargestFilesTable(db)
#DepotInterface.cacheLargestFilesInfo(db)


reader = DepotInterface.getReaderForLargestFilesFromCache(db)
for i in range(1):
	result = reader.next()
	if result is None:
		break
	filehash, filesize, status, filenames, directories, depotIds = result
	print filehash
	print filenames
	dlist = directories.split()
	for d in dlist:
		print DepotInterface.getDirectoryPath(db, d)
	if depotIds == "":
		print "not found"
		#DepotInterface.handleNotFoundFileEntry(db, filehash, filesize, filenames, directories)
		DepotInterface.removeFromLargestFilesCache(db, filehash)
	else:
		print depotIds
		depotList = depotIds.split()
		for depotId in depotList:
			print DepotInterface.getDepotPath(db, int(depotId))


