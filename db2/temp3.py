
# similar to getDirInfo.py 

import miscQueries
import CoreDb
import DbLogger
import time

dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/fmapp/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)
logger = DbLogger.dbLogger()

startTime = time.time()

logger.log("start time: %s => time 0" % str(time.time()))

searchString = "m6\\dvds3\\strobist"
dirList = miscQueries.getDirectoriesContainingString(db, searchString)
for entry in dirList:
	logger.log(entry)

logger.log("end time: %s => time 0" % str(time.time()))
