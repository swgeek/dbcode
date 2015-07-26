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


def getDirectoriesEndingWithStringString(db, searchString):
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

	count = miscQueries.numberOfRows(db, DbSchema.TempDirectoryForFileTable)
	logger.log("TempDirectoryForFileTable has %d entries" % count)

	count = getCountOfFilesToRemove(db)
	logger.log("files with status toRemoveCompletely: %d" % count)



def getParent(db, dirhash):
	command = "select parentDirPathHash from %s where dirPathHash = \"%s\";" \
		% (DbSchema.TempParentDirTable, dirhash)
	result = db.ExecuteSqlQueryForSingleString(command)
	return result


def clearDirInfo(db, dirhash):
	command = "update %s set totalDirSize = NULL, totalDirInfo = NULL where dirPathHash = \"%s\";" % (DbSchema.TempDirInfoTable, dirhash)
	db.ExecuteNonQuerySql(command)


def clearAncestorDirInfo(db, dirhash, logger):
	ancestorDirs = []
	parent = getParent(db, dirhash)
	while parent:
		#logger.log(parent)
		#logger.log(miscQueries.getDirectoryPath(db, parent))
		clearDirInfo(db, parent)
		ancestorDirs.append(parent)
		parent = getParent(db, parent)

	#logger.log(ancestorDirs)
	return ancestorDirs


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


def getEntireOrigDirsForFileTable(db):
	command = "select * from %s;" % DbSchema.OriginalDirectoryForFileTable
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	return results


# maybe optimize by passing in entire dirlist and do them
# all in one pass, return a dictionary
def getFilesFromDirUsingCache(origDirsForFileCache, searchHash):
	filesFromDir = set()
	for entry in origDirsForFileCache:
		filehash, filename, dirhash = entry
		if dirhash == searchHash:
			filesFromDir.add(filehash)
	return filesFromDir


def getAllDirsForFileUsingCache(origDirsForFileCache, searchFileHash):
	dirs = set()
	for entry in origDirsForFileCache:
		filehash, filename, dirhash = entry
		if filehash == searchFileHash:
			dirs.add(dirhash)
	return dirs


def deleteDirEntries(db, dirPathHash):
	command = "delete from %s where dirPathHash = '%s';" % (DbSchema.TempDirInfoTable, dirPathHash)
	db.ExecuteNonQuerySql(command)

	command = "delete from %s where dirPathHash = '%s';" % (DbSchema.TempDirectoryForFileTable, dirPathHash)
	db.ExecuteNonQuerySql(command)

	command = "delete from %s where dirPathHash = '%s';" % (DbSchema.TempParentDirTable, dirPathHash)
	db.ExecuteNonQuerySql(command)

	command = "delete from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, dirPathHash)
	db.ExecuteNonQuerySql(command)

	command = "delete from %s where dirPathHash = '%s';" % (DbSchema.OriginalDirectoriesTable, dirPathHash)
	db.ExecuteNonQuerySql(command)


def removeDirAndContents(db, dirhash, dirpath):

	logger.log("removing: %s" % dirpath)
	logger.log("hash: %s" % dirhash)

	# get parent, move up the chain and invalidate dirInfo
	# keep list of parent dirs so can redo later
	ancestorDirs = clearAncestorDirInfo(db, dirhash, logger)

	# cache this to save repeated queries
	logger.log("get parent dirs table")
	parentDirsCache = getParentDirs(db)

	logger.log("find subdirs")
	subdirs = getSubDirsUsingCache(parentDirsCache, dirhash)

	logger.log("got %d subdirs" % len(subdirs))

	parentDirsCache = None # hopefully can garbage collect now

	logger.log("getting origDirsForFile table")
	origDirsForFileCache = getEntireOrigDirsForFileTable(db)

	logger.log("getting files from subdirs")
	allFilesToDelete = set()
	for subdirhash in subdirs:
		#logger.log(miscQueries.getDirectoryPath(db, subdirhash))
		fileList = getFilesFromDirUsingCache(origDirsForFileCache, subdirhash)	
		#logger.log(fileList)
		allFilesToDelete = allFilesToDelete | fileList


	logger.log("got %d files" % len(allFilesToDelete))

	logger.log("set file status to be removed")
	for i, filehash in enumerate(allFilesToDelete):
		if not i%100:
			logger.log("at file %d" % i)
		dirsForFile = getAllDirsForFileUsingCache(origDirsForFileCache, filehash)
		if len(dirsForFile) == 1:
			miscQueries.setFileStatus(db, filehash, "toRemoveCompletely")

	logger.log("deleting all dir entries")
	for i, dirhash in enumerate(subdirs):
		if not i%100:
			logger.log("at dir %d" % i)
		deleteDirEntries(db, dirhash)

	logger.log("done with %s" % dirpath)





db = CoreDb.CoreDb("/Users/v724660/fmapp/listingDb.sqlite")
logger = DbLogger.dbLogger()

logger.log("time: %s" % str(time.time()))
#getCounts(db, logger)
logger.log("time: %s" % str(time.time()))

dirs = getDirectoriesEndingWithStringString(db, ".svn")

logger.log("got %d dirs to delete" % len(dirs))

#dirs = [('E6ED559C42DC8E5430BDCF50E02C0B90E347FF47', 'J:\\m2drive\\m3\\tempBackupPhotoWorking\\IndiaUK_Dec2012\\lcat\\lcat Previews.lrdata')]
for entry in dirs:
	logger.log(dirs)

for i, (dirhash, dirpath) in enumerate(dirs):
	logger.log("removing main dir number %d" % i)
	logger.log("time: %s" % str(time.time()))
	removeDirAndContents(db, dirhash, dirpath)

logger.log("time: %s" % str(time.time()))

getCounts(db, logger)

logger.log("time: %s" % str(time.time()))

