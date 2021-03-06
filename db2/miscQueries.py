#
# catchall for all query methods.
# obviously need to organize and put into different files, but putting here while developing
#

import DbSchema

# database stuff
def createTable(db, tableName, tableSchema):
	createTableCommand = "create table %s (%s);" % (tableName, tableSchema)
	db.ExecuteNonQuerySql(createTableCommand)

	
# depot stuff
def getDepotInfo(db):
	command = "select * from %s;" % DbSchema.objectStoresTable
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	return result


def getFilesContainingString(db, searchString):
	command = "select filename, filehash from %s where filename like \'%%%s%%\';" % (DbSchema.OriginalDirectoryForFileTable, searchString)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	return result


# bit confusing, returns a dictionary mapping files to a set of directories,
# and also a dictionary mapping dirs to a set of files
def cacheOrigDirsForFileTable(db):
	command = "select * from %s;" % DbSchema.OriginalDirectoryForFileTable
	results = db.ExecuteSqlQueryReturningMultipleRows(command)

	origDirsForFileDict = {}
	origFilesForDirDict = {}
	for row in results:
		filehash = row[0]
		filename = row[1]
		dirpathhash = row[2]
		if filehash in origDirsForFileDict:
			origDirsForFileDict[filehash].append(dirpathhash)
		else:
			origDirsForFileDict[filehash] = [dirpathhash]

		if dirpathhash in origFilesForDirDict:
			origFilesForDirDict[dirpathhash].append((filehash, filename))
		else:
			origFilesForDirDict[dirpathhash] = [(filehash, filename)]

	return origDirsForFileDict, origFilesForDirDict

def getDirectoriesContainingString(db, searchString):
	command = "select * from %s where dirPath like \'%%%s%%\';" % (DbSchema.OriginalDirectoriesTable, searchString)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	return result


def getAllFilesFromDir(db, dirPathHash):
	command = "select filehash, filename from %s where dirPathHash = \"%s\";" % (DbSchema.OriginalDirectoryForFileTable, dirPathHash)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	return result


def getAllFilesFromDirAndSubdirs(db, dirpathRoot):
	command = "select dirPathHash from %s where dirPath like \'%s%%\';" % (DbSchema.OriginalDirectoriesTable, dirpathRoot)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	filelist = []
	for dirhash in result:
		filelist += getAllFilesFromDir(db, dirhash)
		
	return filelist


def getAllPathsAndFilenamesFromDirAndSubdirs(db, dirpathRoot):
	command = "select * from %s where dirPath like \'%s%%\';" % (DbSchema.OriginalDirectoriesTable, dirpathRoot)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	dirfilelist = {}
	for dirhash, dirpath in result:
		dirfilelist[dirpath] = getAllFilesFromDir(db, dirhash)
		
	return dirfilelist

# this could probably be made more efficient, maybe find a better command
def checkIfFilehashInDatabase(db, filehash):
	command = "select filehash from %s where filehash = \"%s\" limit 1;" % (DbSchema.newFilesTable, filehash)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	if len(result) > 0:
		return True
	else:
		return False


def checkIfFileListingEntryInDatabase(db, filehash, depotId):
	command = "select * from %s where filehash = \"%s\" and depotId = %d limit 1" \
			% (DbSchema.FileListingTable, filehash, depotId)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	if len(result) > 0:
		return True
	else:
		return False


def getFileInfo(db, filehash):
	command = "select * from %s where filehash = '%s';" % (DbSchema.newFilesTable, filehash)
	result = db.ExecuteSqlQueryReturningSingleRow(command)
	return result

# this could probably be made more efficient, maybe find a better command
def checkIfDirhashInDatabase(db, dirPathHash):
	command = "select dirPathHash from %s where dirPathHash = \"%s\";" % (DbSchema.OriginalDirectoriesTable, dirPathHash)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	if len(result) > 0:
		return True
	else:
		return False


# this could probably be made more efficient, maybe find a better command
def checkIfFileDirectoryInDatabase(db, filehash, filename, dirhash):
	command = "select * from %s where filehash = \"%s\" and filename = \"%s\" and dirPathHash = \"%s\" limit 1;" \
            % (DbSchema.OriginalDirectoryForFileTable, filehash, filename, dirhash)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	if len(result) > 0:
		return True
	else:
		return False


def GetObjectStoreId(db, objectStorePath):
	depotID = None
	commandString = "select depotId from objectStores where path = \"%s\"" % objectStorePath
	storeID = db.ExecuteSqlQueryReturningSingleRow(commandString)

	if not storeID is None:
		return storeID[0]
	return storeID


def getValuesFromTable(db, tableName, numberOfEntries = None):
	commandString = "select * from %s" % tableName
	if numberOfEntries:
		commandString += " limit %d" % numberOfEntries
	commandString += ";"
	values = db.ExecuteSqlQueryReturningMultipleRows(commandString)
	return values


def getAllObjectStores(db):
	return getValuesFromTable(db, "objectStores")


def insertDirHash(db, dirPathHash, dirPath):
	command = "insert into %s (dirPathHash, dirPath) values (\"%s\", \"%s\")" \
            % (DbSchema.OriginalDirectoriesTable, dirPathHash, dirPath)
	db.ExecuteNonQuerySql(command)


def insertOriginalDir(db, filehash, filename, dirhash):
	command = "insert into %s (filehash, filename, dirPathHash) values (\"%s\", \"%s\", \"%s\")" \
            % (DbSchema.OriginalDirectoryForFileTable, filehash, filename, dirhash)
	db.ExecuteNonQuerySql(command)


def insertFileListing(db, filehash, depotId, filesize):
	command = "insert into %s (filehash, depotId, filesize) values (\"%s\", %d, %d)" \
            % (DbSchema.FileListingTable, filehash, depotId, filesize)
	db.ExecuteNonQuerySql(command)


def insertFileEntry(db, filehash, filesize, depotId):
	command = "insert into %s (filehash, filesize, primaryLocation) values (\"%s\", %d, %d)" \
            % (DbSchema.newFilesTable, filehash, filesize, depotId)
	db.ExecuteNonQuerySql(command)


def insertMultipleFileEntries(db, entryList):
	command = "insert into %s (filehash, filesize, primaryLocation) values (?, ?, ?)" \
            % (DbSchema.newFilesTable)
	db.ExecuteManyNonQuery(command, entryList)


def getDirectoryPath(db, dirPathHash):
	command = "select dirPath from %s where dirPathHash = \"%s\";" % (DbSchema.OriginalDirectoriesTable, dirPathHash)
	return db.ExecuteSqlQueryForSingleString(command)


def numberOfRows(db, tableName):
	command = "select count(*) from %s; " % tableName
	return db.ExecuteSqlQueryReturningSingleInt(command)


def setFileStatus(db, fileHash, newStatus):
	command = "update %s set status = \"%s\" where filehash = \"%s\";" % (DbSchema.newFilesTable, newStatus, fileHash)
	if newStatus is None:
		command =  "update %s set status = null where filehash = \"%s\";" % (DbSchema.newFilesTable, fileHash)
	db.ExecuteNonQuerySql(command)


def getFilesWithStatus(db, status, count = None):
	command = "select * from %s where status is '%s'" % (DbSchema.newFilesTable, status)
	if status is None:
		command = "select * from %s where status is null" % DbSchema.newFilesTable
	if count:
		command += " limit %d" % count
	command += ";"
	values = db.ExecuteSqlQueryReturningMultipleRows(command)
	return values


def getCountOfFilesWithStatus(db, status):
	command = "select count(*) from %s where status is '%s';" % (DbSchema.newFilesTable, status)
	if status is None:
		command =  "select count(*) from %s where status is null;" % DbSchema.newFilesTable
	count = db.ExecuteSqlQueryReturningSingleInt(command)
	return count


def getStatusTypes(db):
	command = "select distinct(status) from %s;" % DbSchema.newFilesTable
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	statusList = [x[0] for x in results]
	return statusList


def getLargestFile(db):
	command = "select * from %s  where status is null order by filesize desc limit 1;" % DbSchema.newFilesTable
	#command = "select * from %s  order by filesize desc limit 1;" % DbSchema.newFilesTable
	result = db.ExecuteSqlQueryReturningSingleRow(command)
	return result


def getCounts(db):
	countsList = []
	count = numberOfRows(db, DbSchema.OriginalDirectoriesTable)
	countsList.append(("OriginalDirectoriesTable", count))

	count = numberOfRows(db, DbSchema.OriginalDirectoryForFileTable)
	countsList.append(("OriginalDirectoryForFileTable", count))

	count = numberOfRows(db, DbSchema.newFilesTable)
	countsList.append(("newFilesTable", count))

	command = "select count(*) from %s where status = \"toRemoveCompletely\"; " \
		% (DbSchema.newFilesTable)
	count = db.ExecuteSqlQueryReturningSingleInt(command)
	countsList.append(("files with status \"toRemoveCompletely\"", count))

	return countsList


def getOriginalDirectoriesForFile(db, filehash):

	command = "select * from %s where filehash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, filehash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	return results


def deleteDirectoryEntryForFile(db,  filehash, dirhash):
	command = "delete from %s where filehash = '%s' and dirPathHash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, filehash, dirhash)
	db.ExecuteNonQuerySql(command)
