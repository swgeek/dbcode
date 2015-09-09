import time

import CoreDb
import DbLogger
import miscQueries
import DbSchema
import Sha1HashUtilities


def setFileStatus(db, fileHash, newStatus):
	command = "update %s set status = \"%s\" where filehash = \"%s\";" % (DbSchema.newFilesTable, newStatus, fileHash)
	db.ExecuteNonQuerySql(command)


def getParentDirs(db):
	command = "select * from %s;" % DbSchema.TempParentDirTable
	parentDirs = db.ExecuteSqlQueryReturningMultipleRows(command)
	return parentDirs


def getCountOfFilesToRemove(db):
	command = "select count(*) from %s where status = \"toRemoveCompletely\"; " \
		% (DbSchema.newFilesTable)
	count = db.ExecuteSqlQueryReturningSingleInt(command)
	return count


def getDirectoriesEndingWithString(db, searchString):
	command = "select * from %s where dirPath like \'%%%s\';" % (DbSchema.OriginalDirectoriesTable, searchString)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	return result


def getCounts(db, logger):
	count = miscQueries.numberOfRows(db, DbSchema.OriginalDirectoriesTable)
	logger.log("OriginalDirectoriesTable has %d entries" % count)

	count = miscQueries.numberOfRows(db, DbSchema.OriginalDirectoryForFileTable)
	logger.log("OriginalDirectoryForFileTable has %d entries" % count)

	count = miscQueries.numberOfRows(db, DbSchema.newFilesTable)
	logger.log("newFilesTable has %d entries" % count)

	count = miscQueries.numberOfRows(db, DbSchema.TempDirInfoTable)
	logger.log("TempDirInfoTable has %d entries" % count)

	count = miscQueries.numberOfRows(db, DbSchema.TempParentDirTable)
	logger.log("TempParentDirTable has %d entries" % count)

#	count = miscQueries.numberOfRows(db, DbSchema.TempDirectoryForFileTable)
#	logger.log("TempDirectoryForFileTable has %d entries" % count)

	count = getCountOfFilesToRemove(db)
	logger.log("files with status \"toRemoveCompletely\": %d" % count)




def getParent(db, dirhash):
	command = "select parentDirPathHash from %s where dirPathHash = \"%s\";" \
		% (DbSchema.TempParentDirTable, dirhash)
	result = db.ExecuteSqlQueryForSingleString(command)
	return result


def clearDirInfo(db, dirhash):
	command = "update %s set totalDirSize = NULL, totalDirInfo = NULL where dirPathHash = \"%s\";" % (DbSchema.TempDirInfoTable, dirhash)
	db.ExecuteNonQuerySql(command)


def clearAncestorDirInfo(sqlCommandList, dirhash, logger):
	parent = getParent(db, dirhash)
	while parent:
		logger.log("parent is:")
		logger.log(parent)
		logger.log(miscQueries.getDirectoryPath(db, parent))
		command = "update %s set totalDirSize = NULL, totalDirInfo = NULL where dirPathHash = \"%s\";" % (DbSchema.TempDirInfoTable, parent)
		sqlCommandList.append(command)
		parent = getParent(db, parent)



def getSubDirsUsingCache(parentDirsCache, dirhash):
	allDescendants = []

	subdirs = []
	for entry in parentDirsCache:
		if entry[1] == dirhash:
			subdirs.append(entry[0])

	allDescendants += subdirs
	
	for subdirhash in subdirs:
		subdirDescendants = getSubDirsUsingCache(parentDirsCache, subdirhash)
		allDescendants = allDescendants + subdirDescendants

	return allDescendants


# bit confusing, returns a dictionary mapping files to a set of directories,
# and also a dictionary mapping dirs to a set of files
def getEntireOrigDirsForFileTable(db):
	command = "select * from %s;" % DbSchema.OriginalDirectoryForFileTable
	results = db.ExecuteSqlQueryReturningMultipleRows(command)

	origDirsForFileDict = {}
	origFilesForDirDict = {}
	for row in results:
		filehash = row[0]
		filename = row[1]
		dirpathhash = row[2]
		if filehash in origDirsForFileDict:
			origDirsForFileDict[filehash].add(dirpathhash)
		else:
			origDirsForFileDict[filehash] = set([dirpathhash])

		if dirpathhash in origFilesForDirDict:
			origFilesForDirDict[dirpathhash].add(filehash)
		else:
			origFilesForDirDict[dirpathhash] = set([filehash])

	return origDirsForFileDict, origFilesForDirDict



# maybe optimize by passing in entire dirlist and do them
# all in one pass, return a dictionary
def getFilesFromDirUsingCache(origDirsForFileCache, searchHash):
	filesFromDir = set()
	for entry in origDirsForFileCache:
		filehash, filename, dirhash = entry
		if dirhash == searchHash:
			filesFromDir.add(filehash)
	return filesFromDir



def deleteDirEntries(sqlCommandList, dirPathHash):


	#command = "delete from %s where dirPathHash = '%s';" % (DbSchema.TempDirInfoTable, dirPathHash)
	#sqlCommandList.append(command)

	#command = "delete from %s where dirPathHash = '%s';" % (DbSchema.TempDirectoryForFileTable, dirPathHash)
	#sqlCommandList.append(command)

	#command = "delete from %s where dirPathHash = '%s';" % (DbSchema.TempParentDirTable, dirPathHash)
	#sqlCommandList.append(command)

	command = "delete from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, dirPathHash)
	sqlCommandList.append(command)

	command = "delete from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoriesTable, dirPathHash)
	sqlCommandList.append(command)



def removeDirAndContents(db, dirhash, dirpath):

	sqlCommandList = []

	logger.log("removing: %s" % dirpath)
	logger.log("hash: %s" % dirhash)

	# get parent, move up the chain and invalidate dirInfo
	# TODO: use parentdirs cache instead!
	clearAncestorDirInfo(sqlCommandList, dirhash, logger)

	# cache this to save repeated queries
	logger.log("get parent dirs table")
	parentDirsCache = getParentDirs(db)

	logger.log("find subdirs")
	subdirs = getSubDirsUsingCache(parentDirsCache, dirhash)

	logger.log("also adding this dir to subdirs")
	subdirs.append(dirhash)

	logger.log("got %d subdirs" % len(subdirs))

	parentDirsCache = None # hopefully can garbage collect now

	logger.log("getting origDirsForFile and origFilesForDirs")
	origDirsForFileDict, origFilesForDirDict = getEntireOrigDirsForFileTable(db)

	logger.log("getting files from subdirs")
	allFilesToDelete = set()
	for subdirhash in subdirs:
		#logger.log(miscQueries.getDirectoryPath(db, subdirhash))
		fileList = origFilesForDirDict.get(subdirhash, set())
		#logger.log(fileList)
		allFilesToDelete = allFilesToDelete | fileList


	logger.log("got %d files" % len(allFilesToDelete))

	logger.log("set file status to be removed if this is only dir")
	for i, filehash in enumerate(allFilesToDelete):
		if not i%100:
			logger.log("at file %d" % i)
		dirsForFile = origDirsForFileDict[filehash]
		if len(dirsForFile) == 1:
			#miscQueries.setFileStatus(db, filehash, "toRemoveCompletely")
			command = "update %s set status = \"%s\" where filehash = \"%s\";" % (DbSchema.newFilesTable, "toRemoveCompletely", filehash)
			sqlCommandList.append(command)

	logger.log("deleting all dir entries")
	for i, dirhash in enumerate(subdirs):
		if not i%100:
			logger.log("at dir %d" % i)
		deleteDirEntries(sqlCommandList, dirhash)

	logger.log(str(time.time()))

	logger.log("performing %d sql commands" % len(sqlCommandList))
	db.ExecuteMultipleSqlStatementsWithRollback(sqlCommandList)

	logger.log("done with %s" % dirpath)
	logger.log(str(time.time()))


# mostly same code as removeDirAndContents, so abstract out and clean up sometime
# this version skips the file remove stage as known to be a duplicate dir
# left the changed lines as commented out code to help refactoring later
def removeDuplicateDirAndContents(db, dirhash, dirpath):

	sqlCommandList = []

	logger.log("removing: %s" % dirpath)
	logger.log("hash: %s" % dirhash)

	# get parent, move up the chain and invalidate dirInfo
	# TODO: use parentdirs cache instead!
	ancestorDirs = clearAncestorDirInfo(sqlCommandList, dirhash, logger)

	# cache this to save repeated queries
	logger.log("get parent dirs table")
	parentDirsCache = getParentDirs(db)

	logger.log("find subdirs")
	subdirs = getSubDirsUsingCache(parentDirsCache, dirhash)

	logger.log("also adding this dir to subdirs")
	subdirs.append(dirhash)

	logger.log("got %d subdirs" % len(subdirs))

	parentDirsCache = None # hopefully can garbage collect now

	#MANOJTEMP logger.log("getting origDirsForFile and origFilesForDirs")
	#MANOJTEMP origDirsForFileDict, origFilesForDirDict = getEntireOrigDirsForFileTable(db)

	#MANOJTEMP logger.log("getting files from subdirs")
	allFilesToDelete = set()
	#MANOJTEMP for subdirhash in subdirs:
		#logger.log(miscQueries.getDirectoryPath(db, subdirhash))
		#MANOJTEMP fileList = origFilesForDirDict.get(subdirhash, set())
		#logger.log(fileList)
		#MANOJTEMP allFilesToDelete = allFilesToDelete | fileList


	#MANOJTEMP logger.log("got %d files" % len(allFilesToDelete))

	logger.log("set file status to be removed")
	for i, filehash in enumerate(allFilesToDelete):
		if not i%100:
			logger.log("at file %d" % i)
		#MANOJTEMP dirsForFile = origDirsForFileDict[filehash]
		#MANOJTEMP if len(dirsForFile) == 1:
			#MANOJTEMP miscQueries.setFileStatus(db, filehash, "toRemoveCompletely")
			#MANOJTEMP command = "update %s set status = \"%s\" where filehash = \"%s\";" % (DbSchema.newFilesTable, "toRemoveCompletely", filehash)
			#MANOJTEMP sqlCommandList.append(command)

	logger.log("deleting all dir entries")
	for i, dirhash in enumerate(subdirs):
		if not i%100:
			logger.log("at dir %d" % i)
		deleteDirEntries(sqlCommandList, dirhash)

	logger.log(str(time.time()))

	logger.log("performing %d sql commands" % len(sqlCommandList))
	db.ExecuteMultipleSqlStatementsWithRollback(sqlCommandList)

	logger.log("done with %s" % dirpath)
	logger.log(str(time.time()))



def getUserCreatedDirsList():
	dirs = [ 

('4AE4191F86A786EFCFCEF3511C9639339C36799A', 'J:\\m2drive\\m6\\dvds3\\strobist', 0)
	]
	return dirs


dbpath = "C:\\depotListing\\listingDb.sqlite"
#dbpath = "/Users/v724660/fmapp/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)
logger = DbLogger.dbLogger()

logger.log("time: %s" % str(time.time()))
getCounts(db, logger)
logger.log("time: %s" % str(time.time()))

dirs = getUserCreatedDirsList()

logger.log("got %d dirs to delete" % len(dirs))

for i, (dirhash, dirpath, size) in enumerate(dirs):
	logger.log("removing main dir number %d" % i)
	logger.log("time: %s" % str(time.time()))
	#removeDuplicateDirAndContents(db, dirhash, dirpath)
	removeDirAndContents(db, dirhash, dirpath)


logger.log("time: %s" % str(time.time()))
getCounts(db, logger)
logger.log("time: %s" % str(time.time()))
