# queries I use to sort, e.g. get largest files

import DbInterface
import DepotInterface

databaseFilePathName = "C:\\depotListing\\listingDb.sqlite"
db = DbInterface.DbInterface(databaseFilePathName)


#reader = DepotInterface.getReaderForFilesWithFilenameStartingWithDotUnderscore(db)


#DepotInterface.createLargestFilesTable(db)
#DepotInterface.cacheLargestFilesInfo(db)

reader = DepotInterface.getReaderForLargestFilesFromCache(db)
for i in range(10):
	print reader.next()