import CoreDb
import DbSchema
import DbLogger
import os

def getLargestUnsortedFileInfo(db):
	command = "select * from %s  where status is null order by filesize desc limit 1;" % DbSchema.newFilesTable
	result = db.ExecuteSqlQueryReturningSingleRow(command)
	return result


def getOldFileInfo(db, filehash):
	command = "select * from %s where filehash = '%s';" % (DbSchema.oldFilesTable, filehash)
	result = db.ExecuteSqlQueryReturningSingleRow(command)
	return result


def getFileInfo(db, filehash):
	command = "select * from %s where filehash = '%s';" % (DbSchema.newFilesTable, filehash)
	result = db.ExecuteSqlQueryReturningSingleRow(command)
	return result


def getOriginalDirectoriesForFile(db, filehash):
	command = "select * from %s where filehash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, filehash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	return results


def getOriginalDirPathForDirHash(db, dirhash):
	command = "select dirPath from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoriesTable, dirhash)
	result = db.ExecuteSqlQueryForSingleString(command)
	return result


def getFilesFromDir(db, dirhash):
	command = "select filehash, filename from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, dirhash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	return results


def getUndeletedFilesFromDir(db, dirhash):
	command = "select filehash, filename from %s " % DbSchema.OriginalDirectoryForFileTable + \
		"join %s using (filehash) " % DbSchema.newFilesTable + \
		" where dirPathHash = '%s' and (status <> 'deleted' or status is null) ;" % dirhash
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	return results


def getUnsortedFilesFromDir(db, dirhash):
	command = "select filehash, filename from %s " % DbSchema.OriginalDirectoryForFileTable + \
		"join %s using (filehash) " % DbSchema.newFilesTable + \
		" where dirPathHash = '%s' and status is null ;" % dirhash
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	return results

def setFileStatus(db, fileHash, newStatus):
	command = "update %s set status = \"%s\" where filehash = \"%s\";" % (DbSchema.newFilesTable, newStatus, fileHash)
	db.ExecuteNonQuerySql(command)

def getFilesWithStatus(db, status):
	command = "select * from %s where status = '%s';" % (DbSchema.newFilesTable, status)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	return results


def DeleteFileFromDepot(depotRootPath, filehash):
		subdir = filehash[0:2]
		filepath = os.path.join(depotRootPath, subdir, filehash)

		if not os.path.isfile(filepath):
			return False

		os.remove(filepath)
		return True


def getDirectoriesWithPartialPath(db, pathStart):
	command = "select * from %s where dirPath like '%s%%';" % (DbSchema.OriginalDirectoriesTable, pathStart)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	return results


db = CoreDb.CoreDb("C:\\depotListing\\listingDb.sqlite")
logger = DbLogger.dbLogger()

###################################

rootDir = "J:\\m2drive\\m3\\robert_\\robert\\robertCurrent"
dirs = getDirectoriesWithPartialPath(db, rootDir)
logger.log(dirs)
exit(1)






##############################
#largestRow = getLargestUnsortedFileInfo(db)
largestRow = (u'E3ED49B8581DE969A2FF2FC0B5DBB07B6FEBBB7C', 3954050048L, 1, None)


logger.log("largest row:")
logger.log(largestRow)


filehash = largestRow[0]

oldFileInfo = getOldFileInfo(db, filehash)
logger.log("old file info: %s" % str(oldFileInfo))
dirs = getOriginalDirectoriesForFile(db, filehash)
logger.log("original dirs:")

for row in dirs:
	dirhash = row[2]
	logger.log(dirhash)
	filename = row[1]
	logger.log(filename)
	dirpath = getOriginalDirPathForDirHash(db, dirhash)
	logger.log(dirpath)
	logger.log("")
	continue

	# this part is manual, e.g. here assuming only one filename and one directory

	# newDir
	#fileList = getFilesFromDir(db, dirhash)
	fileList = getUnsortedFilesFromDir(db, dirhash)
	logger.log("undeleted files %s" % str(fileList))

	depotRoot = "H:\\objectstore2"
	for row in fileList:
		# get filehash
		filehash = row[0]

		if filehash == "A8B5DF0B0816280AE18017BC4B119C77B6C6EB79":
			logger.log("keeping %s" % filehash)
			setFileStatus(db, filehash, "keep")
			continue
		else:
			logger.log("deleting %s"% filehash)

		# got locations
		otherdirs = getOriginalDirectoriesForFile(db, filehash)
		logger.log("original dirs for %s:" % filehash)
		logger.log(otherdirs)

		# mark as toDelete
		setFileStatus(db, filehash, "todelete")

		# delete from repo
		result = DeleteFileFromDepot(depotRoot, filehash)
		logger.log( "delete result %s " % str(result))
		setFileStatus(db, filehash, "deleted")

		# what to do about directory? for now, keep it simple, keep in directory table
		# mark as deleted

	logger.log( getFilesWithStatus(db, "todelete"))
	logger.log(  getFilesWithStatus(db, "deleted"))
	logger.log(  getFilesWithStatus(db, "keep"))

