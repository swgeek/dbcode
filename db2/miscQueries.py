#
# catchall for all query methods.
# obviously need to organize and put into different files, but putting here while developing
#

import DbSchema

# depot stuff
def getDepotInfo(db):
	command = "select * from %s;" % DbSchema.objectStoresTable
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	return result


def getFilesContainingString(db, searchString):
	command = "select filename, filehash from %s where filename like \'%%%s%%\';" % (DbSchema.OriginalDirectoryForFileTable, searchString)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	return result


def getDirectoriesContainingString(db, searchString):
	command = "select * from %s where dirPath like \'%%%s%%\';" % (DbSchema.OriginalDirectoriesTable, searchString)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	return result


def getAllFilesFromDir(db, dirPathHash):
	command = "select filehash, filename from %s where dirPathHash = \"%s\";" % (DbSchema.OriginalDirectoryForFileTable, dirPathHash)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	return result


# this could probably be made more efficient, maybe find a better command
def checkIfFilehashInDatabase(db, filehash):
	command = "select filehash from %s where filehash = \"%s\";" % (DbSchema.FilesTable, filehash)
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


def insertFileEntry(db, filehash, filesize):
	command = "insert into %s (filehash, filesize) values (\"%s\", %d)" \
            % (DbSchema.FilesTable, filehash, filesize)
	db.ExecuteNonQuerySql(command)
