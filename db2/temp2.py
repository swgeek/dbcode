


# removes files from database
# similar to removeFileCompletely, just starting over.


import time

import getFileList
import miscQueries
import CoreDb
import DbLogger
import HashFilesEtc
import getFileList

db = CoreDb.CoreDb("C:\\depotListing\\listingDb.sqlite")
logger = DbLogger.dbLogger()

startTime = time.time()

logger.log("start time: %s => time 0" % str(time.time()))
countsList = miscQueries.getCounts(db)
logger.log("counts:")
for entry in countsList:
	logger.log("\t%s: %d" % entry)
logger.log("time: %s" % str(time.time() - startTime))

depotRoot1 = "I:\\objectstore1"
depotRoot2 = "H:\\objectstore2"

spaceForDepot1 = HashFilesEtc.getFreeSpaceOnDepotDrive(depotRoot1)
logger.log("drive for %s has: %d free" % (depotRoot1, spaceForDepot1) )

spaceForDepot2 = HashFilesEtc.getFreeSpaceOnDepotDrive(depotRoot2)
logger.log("drive for %s has: %d free" % (depotRoot2, spaceForDepot2) )

logger.log("time: %s" % str(time.time() - startTime))

fileList = getFileList.getFileList()
logger.log("%d files to remove" % len(fileList))

for i in range(10):
	logger.log(fileList[i])
	filehash = fileList[i][2]
	logger.log(filehash)