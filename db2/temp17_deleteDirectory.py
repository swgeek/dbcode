# delete everything under a particular directory
#
# similar to temp9, but not sure if temp9 works properly.
# keeping it simple, no cache, so easier to see what I am doing here...
import os.path

import miscQueries
import CoreDb
import DbLogger
import time
import DbSchema
import FileUtils


def getOriginalDirPathForDirHash(db, dirhash):
	command = "select dirPath from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoriesTable, dirhash)
	result = db.ExecuteSqlQueryForSingleString(command)
	return result


def getOriginalDirectoriesForFile(db, filehash):
	command = "select dirPathHash from %s where filehash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, filehash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	# change from list of tuples to list of dirhash values
	results = [x[0] for x in results]
	return results


def logInfoForFile(db, filehash, logger):
	newFileInfo = getNewFileInfo(db, filehash)
	logger.log("new file info: %s" % str(newFileInfo))
	oldFileInfo = getOldFileInfo(db, filehash)
	logger.log("old file info: %s" % str(oldFileInfo))

	dirs = getOriginalDirectoriesForFile(db, filehash)
	logger.log("original dirs:")

	for row in dirs:
		logger.log("\trow: %s" % (str(row)))
		dirhash = row[2]
		dirpath = getOriginalDirPathForDirHash(db, dirhash)
		logger.log("\tpath:%s" % dirpath)
		# following format only as delete dir utility expects it. Maybe change
		#logger.log("(\"%s\",\"%s\",0)," % (dirhash, dirpath))
		logger.log("")




def getFilesFromDir(db, dirhash):
	command = "select filehash, filename from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, dirhash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	return results


def deleteDirEntries(db, dirhash, logger):
	logger.log("deleting dir %s" % dirhash)
	# delete from originalDirectoriesForFile
	command = "delete from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, dirhash)
	db.ExecuteNonQuerySql(command)
	command = "delete from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoriesTable, dirhash)
	db.ExecuteNonQuerySql(command)


def setToRemoveStatusForFilesInDir(db, dirhash, logger):

	filelist = getFilesFromDir(db, dirhash)
	if not filelist:
		logger.log("\tno files in this dir")
		return

	for filehash, filename in filelist:
		logger.log("\t%s: %s" % (filehash, filename))

		dirInfo = getOriginalDirectoriesForFile(db, filehash)

		logger.log("\t in dirs: %s" % str(dirInfo))

		dirInfoSet = set(dirInfo)

		if dirhash not in dirInfoSet:
			logger.log("################### ERROR: this dirhash not in list of dirs")
			logger.log(str(dirInfoSet))
			continue

		if len(dirInfoSet) > 1:
			logger.log("multiple locations, not deleting file")
			continue

		# this is the only directory the file exists in, safe to remove
		logger.log("set toremove status for %s" % filehash)
		miscQueries.setFileStatus(db, filehash, "toRemove")



def setToDeleteAllFilesFromDirAndSubdirs(db, logger, dirpathRoot):
	logger.log("removing files from %s" % dirpathRoot)

	dirlist = miscQueries.getDirectoriesContainingString(db, dirpathRoot)
	logger.log("got %d dirs" % len(dirlist))
	for dirhash, dirpath in dirlist:
		logger.log("removing directory %s: %s" % (dirhash, dirpath))
		setToRemoveStatusForFilesInDir(db, dirhash, logger)
		deleteDirEntries(db, dirhash, logger)




logger = DbLogger.dbLogger()
dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/db/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)

dirpath  = "A:\\fromAir_20151012\\2015\\2015-05-02"	

setToDeleteAllFilesFromDirAndSubdirs(db, logger, dirpath)
