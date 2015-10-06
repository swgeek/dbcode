
# lists all directories with specified search string

import miscQueries
import CoreDb
import DbLogger
import time

dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/db/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)
logger = DbLogger.dbLogger()

startTime = time.time()

logger.log("start time: %s => time 0" % str(time.time()))

searchString = "AnDinner"
dirInfo = miscQueries.getDirectoriesContainingString(db, searchString)

dirList = [x[1] for x in dirInfo]
dirList.sort()
for entry in dirList:
	logger.log(entry)


logger.log("end time: %s => time 0" % str(time.time()))
