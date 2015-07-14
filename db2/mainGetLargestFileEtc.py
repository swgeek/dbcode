import CoreDb
import DbSchema
import DbLogger
import os
import miscQueries

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

def logOriginalDirectoriesForFile(db, filehash, logger):
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


def getOriginalDirPathForDirHash(db, dirhash):
	command = "select dirPath from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoriesTable, dirhash)
	result = db.ExecuteSqlQueryForSingleString(command)
	return result


def getEntireOriginalDirTable(db):
	command = "select dirPathHash, filehash, filename from %s;" % DbSchema.OriginalDirectoryForFileTable 
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	return result


def getFilesFromDir(db, dirhash):
	command = "select filehash, filename from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, dirhash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	return results


def getFilesFromDirUsingList(dirTableContents, dirPathHash):
	result = []
	for entry in dirTableContents:
		if entry[0] == dirPathHash:
			result.append(entry[1:])
	return result

def getDirLocationCountForFileUsingList(dirTableContents, filehash):
	count = 0
	for entry in dirTableContents:
		if entry[1] == filehash:
			count += 1
	return count


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


def getDirectoriesWithSearchString(db, searchString):
	command = "select * from %s where dirPath like '%%%s%%';" % (DbSchema.OriginalDirectoriesTable, searchString)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	return results


def deleteDirEntries(db, dirPathHash):
	command = "delete from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, dirPathHash)
	db.ExecuteNonQuerySql(command)

	command = "delete from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoriesTable, dirPathHash)
	db.ExecuteNonQuerySql(command)



db = CoreDb.CoreDb("C:\\depotListing\\listingDb.sqlite")
logger = DbLogger.dbLogger()

###################################

#fileHash = "C0C47AA4573C0AF675106CF213AF0BC67080A48A"
#logOriginalDirectoriesForFile(db, fileHash, logger)


dirTableContents = getEntireOriginalDirTable(db)
logger.log(len(dirTableContents))

#rootDir = "I:\\m\\fromUsbProbDups\\fromMac_20130912\\codeExercises\\opengles-book-samples-read-only"
rootDir = "I:\\m\\fromUsbProbDups\\moved\\fromMac20130729\\moveToUsb\\opengles-book"
#searchString = "opengles-book"
logger.log(miscQueries.numberOfRows(db, DbSchema.OriginalDirectoryForFileTable))
logger.log(miscQueries.numberOfRows(db, DbSchema.OriginalDirectoriesTable))
dirs = getDirectoriesWithSearchString(db, rootDir)
logger.log(len(dirs))
#for entry in dirs:
#	logger.log(entry)

#deleteDirectoriesWithPartialPath(db, rootDir)

#logger.log(miscQueries.numberOfRows(db, DbSchema.OriginalDirectoriesTable))
#dirs = getDirectoriesWithSearchString(db, rootDir)
#logger.log(len(dirs))

for i, entry in enumerate(dirs):
	dirhash = entry[0]
	dirpath = entry[1]
	logger.log("%d: dirhash: %s, path: %s" % (i, dirhash, dirpath))

	# get all files in this directory
	filesInDir = getFilesFromDirUsingList(dirTableContents, dirhash)

	for f in filesInDir:
		filehash = f[0]
		filename = f[1]
		logger.log("\t %s, %s" % (filehash, filename))

		# get count of directories for this file
		locationCount = getDirLocationCountForFileUsingList(dirTableContents, filehash)
		logger.log("\t %d locations" % locationCount)

		if locationCount == 1:
			logger.log("############## deleting file ##################################################")
			setFileStatus(db, "todelete")

	deleteDirEntries(db, dirhash)
logger.log(miscQueries.numberOfRows(db, DbSchema.OriginalDirectoryForFileTable))
logger.log(miscQueries.numberOfRows(db, DbSchema.OriginalDirectoriesTable))


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

