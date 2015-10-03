
# copy a dir and log files that could not be copied (corrupt drive etc)
# check if filename matches filehash, flag if not

import os
import shutil

import CoreDb
import DbLogger
import miscQueries
import DbSchema
import FileUtils
import Sha1HashUtilities


def MoveFileIntoDepotIfDoesNotExist(depotRootPath, sourceFilePath, filehash, logger):
		subdir = filehash[0:2]
		destinationDirPath = os.path.join(depotRootPath, subdir)
		destinationFilePath = os.path.join(depotRootPath, subdir, filehash)

		if not os.path.isdir(destinationDirPath):
			os.mkdir(destinationDirPath)

		if os.path.isfile(destinationFilePath):
			logger.log( "ERROR: %s already exists" % destinationFilePath )
			exit(1)
		else:
			logger.log("moving %s to %s" % (sourceFilePath, destinationFilePath))
			shutil.move(sourceFilePath, destinationFilePath)


def checkReplacements(db, depotRoot, replacementsDir, logger):

	# get files in replacement dir
	filelist = os.listdir(replacementsDir)
	for filename in filelist:
		logger.log(filename)

		filepath = os.path.join(replacementsDir, filename)
		logger.log("file: %s" % filepath)

		filehash = Sha1HashUtilities.HashFile(filepath)
		logger.log("hash: %s" % filehash)

		if filehash.upper() == filename.upper():
			logger.log("success")
			MoveFileIntoDepotIfDoesNotExist(depotRoot, filepath, filehash, logger)
			miscQueries.setFileStatus(db, filehash, "replaced")
			
		else:
			logger.log("hash failed, also corrupt")
			os.remove(filepath)


def getCounts(db, logger):
	count = miscQueries.getCountOfFilesWithStatus(db, "corrupted")
	logger.log("number of files with corrupted status: %d" % count)

	count = miscQueries.getCountOfFilesWithStatus(db, "toReplace")
	logger.log("number of files with toReplace status: %d" % count)

	count = miscQueries.getCountOfFilesWithStatus(db, "notFound")
	logger.log("number of files with notFound status: %d" % count)

	count = miscQueries.getCountOfFilesWithStatus(db, "foundReplacement")
	logger.log("number of files with foundReplacement status: %d" % count)

	count = miscQueries.getCountOfFilesWithStatus(db, "replaced")
	logger.log("number of files with corrupted status: %d" % count)




def getFilesFromBackup(db, logger):
	depotRootPath = "F:\objectstore1p5"
	destinationDirPath = "I:\\replacements"

	#list = miscQueries.getFilesWithStatus(db, "foundReplacement")
	#for item in list:
	#	filehash = item[0]
	#	miscQueries.setFileStatus(db, filehash, "notFound")
	#exit(1)

	list = miscQueries.getFilesWithStatus(db, "notFound")
	for item in list:
		filehash = item[0]
		#logger.log(filehash)

		copied = FileUtils.CopyFileFromDepot(db, depotRootPath, destinationDirPath, filehash, filehash)
		if copied:
			logger.log("copied %s" % filehash)
			miscQueries.setFileStatus(db, filehash, "foundReplacement")


dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/db/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)

logger = DbLogger.dbLogger()

getCounts(db, logger)

getFilesFromBackup(db, logger)

checkReplacements(db, "I:\\objectstore1", "I:\\replacements", logger)

getCounts(db, logger)

exit(1)

'''

count = miscQueries.getCountOfFilesWithStatus(db, "corrupted")
logger.log("number of files with corrupted status: %d" % count)

depotRoot = "F:\\objectstore2"

corruptCount = 0
missingCount = 0

subdirlist = os.listdir(depotRoot)

for subdir in subdirlist:
	# skip non directories
	subdirpath = os.path.join(depotRoot, subdir)
	if not os.path.isdir(subdirpath):
		continue

	logger.log("directory %s" % subdirpath)

	filelist = os.listdir(subdirpath)

	for filename in filelist:
		# TODO: check if filehash and not random file
		logger.log(filename)
		filepath = os.path.join(subdirpath, filename)
		logger.log("file: %s" % filepath)

		filehash = Sha1HashUtilities.HashFile(filepath)
		logger.log("hash: %s" % filehash)

		if filehash.upper() != filename.upper():
			logger.log("ERROR: file %s" % filename)

			if not miscQueries.checkIfFilehashInDatabase(db, filename):
				logger.log("ERROR: file %s missing in database")
				missingCount += 1
			else:
				miscQueries.setFileStatus(db, filename, "corrupted")
				corruptCount +=1
			logger.log("corrupt files: %d" % corruptCount)
			logger.log("missing files: %d" % missingCount)


logger.log("corrupt files: %d" % corruptCount)
logger.log("missing files: %d" % missingCount)

count = miscQueries.getCountOfFilesWithStatus(db, "corrupted")
logger.log("number of files with corrupted status: %d" % count)

'''
