# core depot functions, i.e. to put stuff into and out of the db
# will have to break up into seperate modules as gets bigger

# notice that have to pass database in each function, i.e. not a class with state.
# decide if that is better or if we should pass db in at init time

#TODO: TEST MODULE!

import os.path
import SHA1HashUtilities
import DbInterface
import string
import time
import shutil
from DbSchema import *




def createListingDb(dbFilePath):

	directory, filename = os.path.split(dbFilePath)
	if not os.path.isdir(directory):
		raise Exception("dir %s does not exist" % directory)

	if os.path.isfile(dbFilePath):
		raise Exception("file %s already exists" % dbFilePath)

	DbInterface.DbInterface.CreateEmptyDbFile(dbFilePath)
	newDb = DbInterface.DbInterface(dbFilePath)

	newDb.OpenConnection()

	createTableCommand = "create table %s (%s);" % (objectStoresTable, objectStoresSchema)
	newDb.ExecuteNonQuerySql(createTableCommand)

	createTableCommand = "create table %s (%s);" % (FileListingTable, FileListingSchema)
	newDb.ExecuteNonQuerySql(createTableCommand)

	newDb.CloseConnection()



def createTable(db, tableName, tableSchema):
	createTableCommand = "create table %s (%s);" % (tableName, tableSchema)
	db.ExecuteNonQuerySql(createTableCommand)

def dropTable(db, tableName):
	createTableCommand = "drop table %s;" % tableName
	db.ExecuteNonQuerySql(createTableCommand)


def createLocationCountTable(db):
	createTable(db, LocationCountTable, LocationCountSchema)


def createFilenamesTable(db):
	createTable(db, FilenamesTable, FilenamesSchema)


def createFilesTable(db):
	createTable(db, FilesTable, FilesSchema)



def createFilenameCountTable(db):
	createTable(db, FilenameCountTable, FilenameCountSchema)



def createOldStatusTable(db):
	createTable(db, OldStatusTable, OldStatusSchema)


def createOldFileLinkTable(db):
	createTable(db, OldFileLinkTable, OldFileLinkSchema)


def createOriginalDirectoriesTable(db):
	createTable(db, OriginalDirectoriesTable, OriginalDirectoriesSchema)


def createOriginalDirectoryForFileTable(db):
	createTable(db, OriginalDirectoryForFileTable, OriginalDirectoryForFileSchema)

def createOldOriginalRootDirectoryTable(db):
	createTable(db, OldOriginalRootDirectoryTable, OldOriginalRootDirectorySchema)


def createDirSubDirTable(db):
	createTable(db, DirSubDirTable, DirSubDirSchema)


def createLargestFilesTable(db):
	createTable(db, LargestFilesTable, LargestFilesSchema)


def createNotFoundFilesTable(db):
	createTable(db, NotFoundFilesTable, NotFoundFilesSchema)


def createToDeleteTable(db):
	createTable(db, ToDeleteTable, ToDeleteSchema)

def createToKeepForNowTable(db):
	createTable(db, ToKeepForNowTable, ToKeepForNowSchema)


def createCurrentBackupTable(db):
	createTable(db, CurrentBackupTable, CurrentBackupSchema)

# new table entries

# TODO: there is duplicated code with transaction and non transaction versions. Figure out best way to abstract
def newFileEntryWithinTransaction(db, filehash, filesize, status=None):
	if status is None:
		command = "insert into %s (filehash, filesize) values ('%s', %d);" % (FilesTable, filehash, filesize)
	else:
		command = "insert into %s (filehash, filesize, status) values ('%s', %d, %s);" % (FilesTable, filehash, filesize, status)

	db.ExecuteNonQuerySqlWithinTransaction(command)


def newFileEntry(db, filehash, filesize, status=None):
	db.OpenConnection()
	if status is None:
		command = "insert into %s (filehash, filesize) values ('%s', %d);" % (FilesTable, filehash, filesize)
	else:
		command = "insert into %s (filehash, filesize, status) values ('%s', %d, %s);" % (FilesTable, filehash, filesize, status)

	db.ExecuteNonQuerySql(command)
	db.CloseConnection()


def newDepotEntry(db, description, path):
	db.OpenConnection()

	command = "insert into %s (description, path) values (\"%s\", \"%s\");" % (objectStoresTable, description, path)
	db.ExecuteNonQuerySql(command)

	command = "select depotId from %s where description = \"%s\" and path = \"%s\" " % (objectStoresTable, description, path)
	depotId = db.ExecuteSqlQueryReturningSingleInt(command)

	db.CloseConnection()

	return depotId


def newFileListingEntry(db, filehash, depotId, filesize):
	db.OpenConnection()
	command = "insert into %s (filehash, depotId, filesize) values (\"%s\", \"%s\", %d);" % (FileListingTable, filehash, depotId, filesize)
	db.ExecuteNonQuerySql(command)
	db.CloseConnection()


def newFileListingEntryAsPartOfTransaction(db, filehash, depotId, filesize):
	command = "insert into %s (filehash, depotId, filesize) values (\"%s\", \"%s\", %d);" % (FileListingTable, filehash, depotId, filesize)
	db.ExecuteNonQuerySqlWithinTransaction(command)


def updateFileListingEntryAsPartOfTransaction(db, filehash, oldDepotId, newDepotId):
	command = "update %s set depotId = %d where filehash =  \"%s\" and depotId = %d ;" % (FileListingTable, newDepotId, filehash, oldDepotId)
	db.ExecuteNonQuerySqlWithinTransaction(command)



def incrementLocationCountWithinTransaction(db, filehash):
	command = "select locations from %s where filehash = \"%s\"" % (LocationCountTable, filehash)
	oldCount = db.ExecuteSqlQueryReturningSingleInt(command)
	newCount = oldCount + 1
	command = "update %s set locations = %d where filehash = \"%s\";" % (LocationCountTable, newCount, filehash)
	db.ExecuteNonQuerySqlWithinTransaction(command)


def addFilename(db, filehash, filename):
	db.OpenConnection()
	command = "select count(*) from %s where filehash=\"%s\" and filename=\"%s\"" % (FilenamesTable, filehash, filename)
	count = db.ExecuteSqlQueryReturningSingleInt(command)
	if count == 0:
		command = "insert into %s (filehash, filename) values (\"%s\", \"%s\");" % (FilenamesTable, filehash, filename )
		db.ExecuteNonQuerySql(command)
	db.CloseConnection()


def addOldStatusAsPartOfTransaction(db, filehash, oldStatus):
	command = "insert into %s (filehash, oldStatus) values (\"%s\", \"%s\");" % (OldStatusTable, filehash, oldStatus )
	db.ExecuteNonQuerySqlWithinTransaction(command)


def addOldFileLinkAsPartOfTransaction(db, filehash, filelinkhash):
	command = "insert into %s (filehash, linkFileHash) values (\"%s\", \"%s\");" % (OldFileLinkTable, filehash, filelinkhash )
	db.ExecuteNonQuerySqlWithinTransaction(command)


def addDirPathAsPartOfTransaction(db, dirPathHash, dirpath):
	command = "insert into %s (dirPathHash, dirPath) values (\"%s\", \"%s\");" % (OriginalDirectoriesTable, dirPathHash, dirpath )
	db.ExecuteNonQuerySqlWithinTransaction(command)


def addOriginalDirectoryForFileAsPartOfTransaction(db, filehash, filename, dirPathHash):
	command = "insert into %s (filehash, filename, dirPathHash) values (\"%s\", \"%s\", \"%s\");" % (OriginalDirectoryForFileTable, filehash, filename, dirPathHash )
	db.ExecuteNonQuerySqlWithinTransaction(command)


def addOldOriginalRootDirectoryAsPartOfTransaction(db, rootdir):
	command = "insert into %s (rootdir) values (\"%s\");" % (OldOriginalRootDirectoryTable, rootdir )
	db.ExecuteNonQuerySqlWithinTransaction(command)


def addDirSubDirAsPartOfTransaction(db, dirPathHash, subDirPathHash):
	command = "insert into %s (dirPathHash, subDirPathHash) values (\"%s\", \"%s\");" % (DirSubDirTable, dirPathHash, subDirPathHash)
	db.ExecuteNonQuerySqlWithinTransaction(command)


def addLargestFileEntry(db, filehash, filesize, status, filenames, directories, depotIds):
	command = "insert into %s (filehash, filesize, status, filenames, directories, depotIds) values (\"%s\", %d, \"%s\", \"%s\", \"%s\", \"%s\");" % \
				(LargestFilesTable, filehash, filesize, status, filenames, directories, depotIds)

	db.ExecuteNonQuerySql(command)


def updateDepotPath(db, depotId, newpath):
	command = "update %s set path = '%s' where depotId = %d;" % (objectStoresTable, newpath, depotId)
	result = db.ExecuteSqlQueryForSingleString(command)
	return result


def addToKeepForNowEntry(db, filehash):
	command = "insert into %s (filehash) values (\"%s\");" % (ToKeepForNowTable, filehash)
	db.ExecuteNonQuerySql(command)

def removeFromLargestFilesCache(db, filehash):
	command = "delete from %s where filehash = '%s';" % (LargestFilesTable, filehash)
	db.ExecuteNonQuerySql(command)


def printFileInfo(db, filehash):
	command = "select * from %s where filehash = '%s';" % (FilesTable, filehash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	print results

	command = "select * from %s where filehash = '%s';" % (FileListingTable, filehash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	print results

	command = "select * from %s where filehash = '%s';" % (OriginalDirectoryForFileTable, filehash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	print results

	command = "select * from %s where filehash = '%s';" % (LocationCountTable, filehash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	print results

	command = "select * from %s where filehash = '%s';" % (FilenameCountTable, filehash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	print results


# handle deleted files etc...

def addToDeleteEntry(db, filehash, filesize, filenames, directories):
	oldStatus = getOldStatus(db, filehash)
	command = "insert into %s (filehash, oldStatus, filesize, filenames, directories) values (\"%s\", \"%s\", %d, \"%s\", \"%s\");" % \
				(ToDeleteTable, filehash, oldStatus, filesize, filenames, directories)
	db.ExecuteNonQuerySql(command)


def addNotFoundEntry(db, filehash, filesize, filenames, directories):
	oldStatus = getOldStatus(db, filehash)
	command = "insert into %s (filehash, oldStatus, filesize, filenames, directories) values (\"%s\", \"%s\", %d, \"%s\", \"%s\");" % \
				(NotFoundFilesTable, filehash, oldStatus, filesize, filenames, directories)
	db.ExecuteNonQuerySql(command)


def AddFileBackedUpInCurrentPass(db, filehash):
	command = "insert into %s (filehash) values(\"%s\");" % (CurrentBackupTable, filehash)
	db.ExecuteNonQuerySql(command)


def deleteFileInfoFromTodoTables(db, filehash):
	print "before:"
	printFileInfo(db, filehash)

	commandList = []

	command = "delete from %s where filehash = '%s';" % (FilesTable, filehash)
	commandList.append(command)

#	command = "delete from %s where filehash = '%s';" % (FileListingTable, filehash)
#	commandList.append(command)

	command = "delete from %s where filehash = '%s';" % (OriginalDirectoryForFileTable, filehash)
	commandList.append(command)

#	command = "delete from %s where filehash = '%s';" % (LocationCountTable, filehash)
#	commandList.append(command)

	command = "delete from %s where filehash = '%s';" % (FilenameCountTable, filehash)
	commandList.append(command)

	print commandList

	db.ExecuteMultipleSqlStatementsWithRollback(commandList)
 
 	time.sleep(10)

 	print "after:"
	printFileInfo(db, filehash)

# initialize derived tables

def ReInitializeLocationCounts(db):
	dropTable(db, LocationCountTable)
	createLocationCountTable(db)
	command = "insert into %s (filehash, locations) select filehash, count(filehash) from %s group by filehash;" % (LocationCountTable, FileListingTable)
	db.ExecuteNonQuerySql(command)


def initializeFilenameCounts(db):
	command = "insert into %s (filehash, names) select filehash, count(filehash) from %s group by filehash;" % (FilenameCountTable, FilenamesTable)
	db.ExecuteNonQuerySql(command)



# pure queries

def FileIsInDatabase(db, filehash):
    command = "select count(*) from %s where filehash = \"%s\"" % (FilesTable, filehash)
    result = db.ExecuteSqlQueryReturningSingleInt(command)

    if result == 0:
        return False   # not in database
    else:
        return True


def getReaderForAllFilehashValues(db):
	command = "select filehash from %s" % oldDbFilesTable
	reader = oldDb.ExecuteSqlQueryReturningReader(command)

	count0 = 0
	count1 = 0
	countOther = 0

	row = reader.next()
	while row:
		filehash = row[0]
		command2 = "select count(*) from %s where filehash = '%s';" % (FileListingTable, filehash)
		count = newDb.ExecuteSqlQueryReturningSingleInt(command2)


# print contents

def getCountForEachDepot(db):
	command = "select depotId from %s" % objectStoresTable
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	print "depotList"
	for row in results:
		depotId = row[0]
		command = "select count(filehash) from %s where depotId = %d" % (FileListingTable, depotId)
		count = db.ExecuteSqlQueryReturningSingleInt(command)
		print "%d: %d" % (depotId, count)


def getCountOfLocationCounts(db):
	command = "select locations, count(locations) from %s group by locations;" % LocationCountTable
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	for row in results:
		print row


def getCountOfFilenameCounts(db):
	command = "select names, count(names) from %s group by names;" % FilenameCountTable
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	for row in results:
		print row


def printAllOldRootDirs(db):
	command = "select rootdir from %s;" % OldOriginalRootDirectoryTable
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	for row in results:
		print row

def getFilenames(db, filehash):
	command = "select filename from %s where filehash = '%s'" % (FilenamesTable, filehash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	filenames = []
	for row in results:
		filenames.append(row[0])
	return filenames


def getDirectoryPath(db, dirPathHash):
	command = "select dirPath from %s where dirPathHash = '%s';" % (OriginalDirectoriesTable, dirPathHash)
	return db.ExecuteSqlQueryForSingleString(command)


def getDirectories(db, filehash):
	command = "select dirPathHash from %s where filehash = '%s'" % (OriginalDirectoryForFileTable, filehash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	directories = []
	for row in results:
		directories.append(row[0])
	return directories

def getFilenamesAndDirectories(db, filehash):
	command = "select filename, dirPathHash from %s where filehash = '%s'" % (OriginalDirectoryForFileTable, filehash)
	dirRows = db.ExecuteSqlQueryReturningMultipleRows(command)
	results = []
	for row in dirRows:
		results.append((row[0], row[1]))
	return results

def getFilesInDirectory(db, dirpathhash):
	command = "select * from %s where dirPathHash = '%s'" % (OriginalDirectoryForFileTable, dirpathhash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	return results


def getDepotIds(db, filehash):
	command = "select depotId from %s where filehash = '%s'" % (FileListingTable, filehash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	depotIds = []
	for row in results:
		depotIds.append(row[0])
	return depotIds


def getDepotPath(db, depotId):
	command = "select path from %s where depotId = %d;" % (objectStoresTable, depotId)
	result = db.ExecuteSqlQueryForSingleString(command)
	return result

def getOldStatus(db, filehash):
	command = "select oldStatus from %s where filehash = '%s';" % (OldStatusTable, filehash)
	result = db.ExecuteSqlQueryForSingleString(command)
	return result

def getToDeleteFileInfo(db, filehash):
	command = "select * from %s where filehash = '%s';" % (ToDeleteTable, filehash)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	if len(result) == 0:
		return None
	return result[0]

def getNotFoundFileInfo(db, filehash):
	command = "select * from %s where filehash = '%s';" % (NotFoundFilesTable, filehash)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	if len(result) == 0:
		return None
	return result[0]


# move stuff below here around to organize
def getReaderForFilesByDescendingFilesize(db):
	command = "select * from %s order by filesize desc" % FilesTable
	return db.ExecuteSqlQueryReturningReader(command)


def getReaderForFilesWithFilenameStartingWithDotUnderscore(db):
	command = "select * from %s where filename LIKE '.\\_%%' ESCAPE '\\'" % FilenamesTable
	return db.ExecuteSqlQueryReturningReader(command)


# temporary, will replace with better query, e.g. top 10 todo 
def getReaderForLargestFilesFromCache(db):
	command = "select * from %s order by filesize desc" % LargestFilesTable
	return db.ExecuteSqlQueryReturningReader(command)
	

def checkIfFileBackedUpInCurrentPass(db, filehash):
	command = "select * from %s where filehash = '%s'" % (CurrentBackupTable, filehash)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	return len(result) == 1


# create "cache tables, i.e. temp tables for working"

def cacheLargestFilesInfo(db):

	reader = getReaderForFilesByDescendingFilesize(db)

	largestFileValues = []
	for i in range(2000):
		result = reader.next()
		filehash, filesize, status = result

		filenames = getFilenames(db, filehash)
		# use ; as separator in case filename contains spaces
		filenamesString = ';'.join(filenames)

		directories = getDirectories(db, filehash)
		directoriesString = string.join(directories)

		depotIds = getDepotIds(db, filehash)
		depotIdsString = string.join(map(str, depotIds))

		largestFileValues.append((filehash, filesize, status, filenamesString, directoriesString, depotIdsString))

	reader.close()

	for entry in largestFileValues:
		filehash, filesize, status, filenamesString, directoriesString, depotIdsString = entry
		addLargestFileEntry(db, filehash, filesize, status, filenamesString, directoriesString, depotIdsString)


def getAllFileInfo(db, filehash):

	command = "select * from %s where filehash = '%s';" % (FilesTable, filehash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	print results
	if len(results) == 0:
		print "***** NONE ***** "
		filesize = None
		status = None
	else:
		tt, filesize, status = results[0]

	filenames = getFilenames(db, filehash)
	# use ; as separator in case filename contains spaces
	filenamesString = ';'.join(filenames)

	directories = getDirectories(db, filehash)
	directoriesString = string.join(directories)

	depotIds = getDepotIds(db, filehash)
	depotIdsString = string.join(map(str, depotIds))

	return filehash, filesize, status, filenamesString, directoriesString, depotIdsString

########## extract etc. this is not db interface so should be moved elsewhere
def extractFile(db, filehash, destinationDir, filename):
	extracted = False
	depotIds = getDepotIds(db, filehash)
	for depot in depotIds:
		depotPath = getDepotPath(db, depot)
		if not os.path.isdir(depotPath):
			continue
		subdir = filehash[0:2]
		filepath = os.path.join(depotPath, subdir, filehash)
		newFilePath = os.path.join(destinationDir, filename)
		print "copying %s to %s" % (filepath, newFilePath)
		shutil.copyfile(filepath, newFilePath)
		extracted = True
		break

	return extracted

def getDepotInfo(db):
	command = "select * from %s;" % objectStoresTable
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	return result
