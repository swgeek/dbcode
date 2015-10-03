
# copy a dir and log files that could not be copied (corrupt drive etc)

import os
import shutil

import CoreDb
import DbLogger
import miscQueries
import DbSchema
import FileUtils

logger = DbLogger.dbLogger()

dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/fmapp/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)




'''
sourceDir = "H:\\tryagain\91"
destinationDir = "E:\\tryagain\91"

if not os.path.isdir(destinationDir):
	os.mkdir(destinationDir)

filelist = 	os.listdir(sourceDir)

for filehash in filelist:
	sourcePath = os.path.join(sourceDir, filehash)
	destPath = os.path.join(destinationDir, filehash)
	try:
		shutil.copyfile(sourcePath, destPath)
	except:
		logger.log("cannot copy %s" % filehash)
		miscQueries.setFileStatus(db, filehash, "cannotCopy")
'''

filelist = miscQueries.getFilesWithStatus(db, "cannotCopy")
for fileinfo in filelist:
	#logger.log(fileinfo)
	filehash = fileinfo[0]
	logger.log(filehash)
	continue
	dirInfoList = miscQueries.getOriginalDirectoriesForFile(db, filehash)
	for dirInfo in dirInfoList:
		logger.log(dirInfo)
		dirhash = dirInfo[2]
		logger.log(dirhash)
		logger.log(miscQueries.getDirectoryPath(db, dirhash))

		depotRootPath = "F:\\objectstore2Part2"
		destinationDirPath = "E:\\copy"
		if not FileUtils.CopyFileFromDepot(db, depotRootPath, destinationDirPath, filehash, filehash):
			logger.log("could not copy %s" % filehash)