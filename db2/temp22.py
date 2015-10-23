# info about files with a particular status
import DbSchema
import miscQueries
import FileUtils
import DbLogger
import CoreDb




logger = DbLogger.dbLogger()
dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/db/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)


depotList = FileUtils.getDepotList()
logger.log("depots: %s" % str(depotList))

status = None
statusCount = miscQueries.getCountOfFilesWithStatus(db, status)
logger.log("%s: %d" % (status, statusCount))

dirInfoCount = 0
diskFileCount = 0

setStatus = False

count = 0

fileList = miscQueries.getFilesWithStatus(db, status)
for fileInfo in fileList:
	filehash = fileInfo[0]
	count += 1
	logger.log("%d: %s" % (count, filehash))

	filepath = FileUtils.getFilepathOnDisk(filehash)
	if filepath:
		diskFileCount += 1
	logger.log("\t filepath: %s" % str(filepath))

	dirlist = miscQueries.getOriginalDirectoriesForFile(db, filehash)
	if dirlist:
		dirInfoCount += 1
	for dirInfo in dirlist:
		logger.log("\tdirinfo: %s" % str(dirInfo))

	if not filepath:
		logger.log("no filepath, setting status to notFound")
		miscQueries.setFileStatus(db, filehash, "notFound")

	if setStatus:
		if dirlist and filepath:
			# have all info, clear status
			logger.log("have dirinfo and filepath, setting status to None")
			miscQueries.setFileStatus(db, filehash, None)

		if dirlist and not filepath:
			# have dir info, but cannot find file, set status to NotFound
			logger.log("have dirinfo but no filepath, setting status to notFound")
			miscQueries.setFileStatus(db, filehash, "notFound")
		

logger.log("files with status %s: %d" % (status, statusCount))
logger.log("found dirinfo for %d files" % dirInfoCount)
logger.log("found disk files for %d files" % diskFileCount)

exit(1)

