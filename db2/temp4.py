# similar to getDirInfo.py 

import FileUtils
import CoreDb
import DbLogger
import time

db = CoreDb.CoreDb("C:\\depotListing\\listingDb.sqlite")
logger = DbLogger.dbLogger()

startTime = time.time()

logger.log("start time: %s => time 0" % str(time.time()))

dirhash = "E11C8B09C26E8116361F8E08E6A18FE83B5B7043"
destinationDir = "E:\\extract"

FileUtils.extractAllFilesFromDirectory(db, dirhash, destinationDir)

logger.log("end time: %s " % str(time.time()))
