import CoreDb
import DbLogger
import DbSchema
import miscQueries
import Sha1HashUtilities
import time
import ntpath


def getAllDirs(db):
	command = "select * from %s;" % DbSchema.OriginalDirectoriesTable
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	return result


def createParentDirTable(db):
	createTableCommand = "create table %s (%s);" % (DbSchema.TempParentDirTable, DbSchema.TempParentDirSchema)
	db.ExecuteNonQuerySql(createTableCommand)


def getAncestorPaths(dirpath):
	ancestorPaths = []
	partialPath = "dummy"
	while dirpath and partialPath:
		ancestorPaths.append(dirpath)
		dirpath, partialPath = ntpath.split(dirpath)

	return ancestorPaths


def addToDirHashDict(allDirPaths, pathsToAdd):
	for dirpath in pathsToAdd:
		dirhash = Sha1HashUtilities.HashString(dirpath)
		allDirPaths[dirhash] = dirpath


def addEntryToOriginalDirsTable(db, dirhash, dirpath):
	if not miscQueries.checkIfDirhashInDatabase(db, dirhash):
		logger.log(" inserting dir %s" % dirpath)
		miscQueries.insertDirHash(db, dirhash, dirpath)


def addEntryToTempDirInfoTable(db, dirhash, dirpath):
	command = "select dirPathHash from %s where dirPathHash = \"%s\";" % (DbSchema.TempDirInfoTable, dirhash)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	if len(result) > 0:
		# already in table, do not add
		return True
	else:
		print "################## should not be here ########"
		# not in table, add. If not in table, then no files, so add 0 size
		command = "insert into %s (dirPathHash, totalFileInfo, totalFileSize) values (\"%s\", \"0\", 0)" \
            % (DbSchema.TempDirInfoTable, dirhash)
		db.ExecuteNonQuerySql(command)


def insertAncestorDirsIntoDb(db, allDirsFromDb):
	allDirPaths = {}

	for i, entry in enumerate(allDirsFromDb):
		dirpath = entry[1]
		ancestorDirs = getAncestorPaths(dirpath)
		logger.log(i)
		addToDirHashDict(allDirPaths, ancestorDirs)
	logger.log(len(allDirPaths))

	for key in allDirPaths:
		dirhash = key
		dirpath = allDirPaths[key]
		#addEntryToOriginalDirsTable(db, dirhash, dirpath)
		addEntryToTempDirInfoTable(db, dirhash, dirpath)


def insertParentsIntoDb(db, allDirsFromDb, logger):
	allDirPaths = {}

	for i, entry in enumerate(allDirsFromDb):
		dirhash = entry[0]
		dirpath = entry[1]
		parent = ntpath.dirname(dirpath)
		logger.log(i)

		if parent == dirpath:
			# no parent, leave parent as null
			command = "insert into %s (dirPathHash) values (\"%s\");" \
				% (DbSchema.TempParentDirTable, dirhash)
		else:
			parentHash = Sha1HashUtilities.HashString(parent)
			command = "insert into %s (dirPathHash, parentDirPathHash) values (\"%s\", \"%s\");" \
				% (DbSchema.TempParentDirTable, dirhash, parentHash)
		db.ExecuteNonQuerySql(command)



def getParentDirs(db):
	command = "select * from %s;" % DbSchema.TempParentDirTable
	parentDirs = db.ExecuteSqlQueryReturningMultipleRows(command)
	return parentDirs


def getEntireDirInfoTable(db):
	dirInfoCache = {}
	command = "select * from %s;" % DbSchema.TempDirInfoTable
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	for entry in results:
		dirhash = entry[0]
		totalFileInfo = entry[1]
		totalFileSize = int(entry[2])
		totalDirInfo = entry[3]
		totalDirSize = entry[4]
		if not totalDirSize is None:
			totalDirSize = int(totalDirSize)

		dirInfoCache[dirhash] = (totalFileInfo, totalFileSize, totalDirInfo, totalDirInfo)

	return dirInfoCache


def getSubDirs(db, dirhash):
	command = "select dirPathHash from %s where parentDirPathHash = \"%s\";" \
		% (DbSchema.TempParentDirTable, dirhash)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	return result


def getSubDirsUsingCache(dirhash, parentDirs, logger):
	subdirs = []
	for entry in parentDirs:
		if entry[1] == dirhash:
			#logger.log(entry)
			subdirs.append(entry[0])
	return subdirs


def getDirInfo(db, dirhash):
	command = "select * from %s where dirPathHash = \"%s\";" \
		% (DbSchema.TempDirInfoTable, dirhash)
	result = db.ExecuteSqlQueryReturningSingleRow(command)
	return result


def updateDirInfo(db, dirInfoCache, dirhash, totalDirInfo, totalDirSize):
	command = "update %s set totalDirSize = \"%d\", totalDirInfo = \"%s\" where dirPathHash = \"%s\";" % (DbSchema.TempDirInfoTable, totalDirSize, totalDirInfo, dirhash)
	db.ExecuteNonQuerySql(command)
	info = dirInfoCache[dirhash]
	newInfo = (info[0], info[1], totalDirInfo, totalDirSize)
	dirInfoCache[dirhash] = newInfo


# made mistake, need to clear values. One time only! (Hopefully)
def TEMPCLEARDirInfo(db):
	command = "update %s set totalDirSize = NULL, totalDirInfo = NULL;" % DbSchema.TempDirInfoTable
	db.ExecuteNonQuerySql(command)


def updateDirInfoForLeafNode(db, dirhash, dirInfoCache):
	dirInfo = dirInfoCache[dirhash]
	totalDirSize = dirInfoCache[dirhash][3]
	if totalDirSize is None:
		totalFileInfo = dirInfo[0]
		totalFileSize = int(dirInfo[1])
		logger.log("##########inserting: %s, %d" % (totalFileInfo, totalFileSize))
		# if no subdirectories, then dirInfo and dirSize is same as fileinfo and filesize
		updateDirInfo(db, dirInfoCache, dirhash, totalFileInfo, totalFileSize)
	else:
		logger.log("already has value, skipping")




db = CoreDb.CoreDb("/Users/v724660/fmapp/listingDb.sqlite")
logger = DbLogger.dbLogger()

allDirsFromDb = getAllDirs(db)

# cache this to save repeated queries
parentDirs = getParentDirs(db)

# cache dirInfoTable as well, again to save queries
dirInfoCache = getEntireDirInfoTable(db)

# should not need to do this again, only the first time
# depends on how I handle imports.
#insertAncestorDirsIntoDb(db, allDirsFromDb)
#insertParentsIntoDb(db, allDirsFromDb, logger)

allDirsFromDb = sorted(allDirsFromDb,key=lambda x: x[1], reverse=True)

#for i in range(100):
#	entry = allDirsFromDb[i]
for i, entry in enumerate(allDirsFromDb):
	logger.log("iteration %d" % i)
	dirhash = entry[0]
	dirpath = entry[1]
	#logger.log(dirhash)
	logger.log(dirpath)

	oldTotalDirSize = dirInfoCache[dirhash][3]
	if not oldTotalDirSize is None:
		logger.log("size has value, already inserted, skipping")
		continue

	subDirs = getSubDirsUsingCache(dirhash, parentDirs, logger)
	#logger.log(subDirs)

	if subDirs:
		logger.log("has subdirs, skipping")
		continue

	logger.log("leaf dir, attempt to insert")
	updateDirInfoForLeafNode(db, dirhash, dirInfoCache)


	#for subDirEntry in subDirs:
	#	subdirhash = subDirEntry[0]
	#	subdirpath = miscQueries.getDirectoryPath(db, subdirhash)
	#	subdirname = ntpath.basename(subdirpath)
	#	logger.log("\t%s"%subdirhash)
	#	logger.log("\t%s"%subdirpath)
	#	logger.log("\t%s"%subdirname)


		# get dirsize, dirinfo, dirname
		#if null, then break out

	# check if has children
	# if no, then dirsize = totalfilesize, dirhash = fileinfo
	# if yes, then check if any child has null dirsize/info
	#      if yes, skip for now
	#      of no, calculate dirhash etc.

