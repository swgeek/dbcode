# 		get counts, diskspace
import DbSchema
import miscQueries
import FileUtils
import DbLogger
import CoreDb

def getCounts(db, logger):
	count = miscQueries.numberOfRows(db, DbSchema.OriginalDirectoriesTable)
	logger.log("OriginalDirectoriesTable has %d entries" % count)

	count = miscQueries.numberOfRows(db, DbSchema.OriginalDirectoryForFileTable)
	logger.log("OriginalDirectoryForFileTable has %d entries" % count)

	count = miscQueries.numberOfRows(db, DbSchema.newFilesTable)
	logger.log("newFilesTable has %d entries" % count)


def getStatusCounts(db, logger):
	statusList = miscQueries.getStatusTypes(db)
	for status in statusList:
		if status == "None":
			status = None
		count = miscQueries.getCountOfFilesWithStatus(db, status)
		logger.log("%s: %d" % (status, count))


def getWindowsSpace(logger):
	drive1 = "I:"
	drive2 = "E:"

	spaceForDepot1 = FileUtils.windowsSpecificGetFreeSpace(drive1)
	logger.log("drive for %s has: %d free" % (drive1, spaceForDepot1) )

	spaceForDepot2 = FileUtils.getFreeSpaceOnDepotDrive(drive2)
	logger.log("drive for %s has: %d free" % (drive2, spaceForDepot2) )


logger = DbLogger.dbLogger()
dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/db/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)

getCounts(db, logger)
getStatusCounts(db, logger)
getWindowsSpace(logger)