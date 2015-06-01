import DbSchema
import DepotInterface
import CoreDb
import sortLater

databaseFilePathName = "C:\\depotListing\\listingDb.sqlite"
db = CoreDb.CoreDb(databaseFilePathName)

for depot in DepotInterface.getDepotInfo(db):
	print depot

#print "reinit location counts"
#DepotInterface.ReInitializeLocationCounts(db)

# get num of copies 
sortLater.printNumOfCopiesOfEachFileInDepot(db, 2)


#command = "select filehash from %s join %s using (filehash) where locations = 1 and depotId = %d" % (DbSchema.FileListingTable, DbSchema.LocationCountTable, 8)
#results = db.ExecuteSqlQueryReturningMultipleRows(command)
#for h in results:
#	filehash =  h[0]
#	sortLater.printFileInfo(db, filehash)

#sortLater.removeDepotAndAllReferences(db, 12)