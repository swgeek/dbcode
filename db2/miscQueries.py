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
