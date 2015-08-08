import CoreDb
import DbLogger
import DbSchema
import miscQueries
import Sha1HashUtilities
import time
import ntpath

insertCount = 0


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


def getEntireDirInfoTable(db, logger):
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

		dirInfoCache[dirhash] = (totalFileInfo, totalFileSize, totalDirInfo, totalDirSize)

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


def updateDirInfo(db, dirInfoCache, dirhash, totalDirInfo, totalDirSize, logger):
	command = "update %s set totalDirSize = \"%d\", totalDirInfo = \"%s\" where dirPathHash = \"%s\";" % (DbSchema.TempDirInfoTable, totalDirSize, totalDirInfo, dirhash)
	#logger.log(command)
	db.ExecuteNonQuerySql(command)
	info = dirInfoCache[dirhash]
	#logger.log(info)
	newInfo = (info[0], info[1], totalDirInfo, totalDirSize)
	#logger.log(newInfo)
	dirInfoCache[dirhash] = newInfo


# made mistake, need to clear values. One time only! (Hopefully)
def TEMPCLEARDirInfo(db):
	command = "update %s set totalDirSize = NULL, totalDirInfo = NULL;" % DbSchema.TempDirInfoTable
	db.ExecuteNonQuerySql(command)


def updateDirInfoForLeafNode(db, dirhash, dirInfoCache, logger):
	#logger.log(dirhash)
	global insertCount
	dirInfo = dirInfoCache[dirhash]
	#logger.log(dirInfo)

	totalDirSize = dirInfoCache[dirhash][3]
	if totalDirSize is None:
		totalFileInfo = dirInfo[0]
		totalFileSize = int(dirInfo[1])
		logger.log("##########inserting leaf: %s, %d" % (totalFileInfo, totalFileSize))
		insertCount += 1
		# if no subdirectories, then dirInfo and dirSize is same as fileinfo and filesize
		updateDirInfo(db, dirInfoCache, dirhash, totalFileInfo, totalFileSize, logger)
	else:
		logger.log("already has value, skipping")


def updateDirInfoGeneralCase(db, dirhash, subDirs, dirInfoCache, dirpathDict):
	global insertCount
	dirEntry = dirInfoCache[dirhash]
	totalDirSize = dirEntry[3]
	if totalDirSize is not None:
		#logger.log("already has value, skipping")
		return

	totalFileInfo = dirEntry[0]
	totalFileSize = int(dirEntry[1])

	totalDirSize = totalFileSize

	subdirPathList = []

	for subdirhash in subDirs:
		#logger.log("subdir %s:" % subdirhash)
		subdirEntry = dirInfoCache[subdirhash]
		#logger.log(subdirEntry)
		subdirSize = subdirEntry[3]
		if subdirSize is None:
			logger.log("a subdir does not have size, skipping")
			return
		totalDirSize += int(subdirSize)

		subdirpath = dirpathDict[subdirhash]
		subdirPathList.append(subdirpath)

	#logger.log("totalDirSize: %d" % totalDirSize)

	# need list of dirs in format <dirinfo><dir basename> 

	subdirPathList.sort()
	singlestring = ""

	for subdirpath in subdirPathList:
		subdirhash = Sha1HashUtilities.HashString(subdirpath)
		subdirEntry = dirInfoCache[subdirhash]
		subdirInfo = subdirEntry[2]
		subdirname = ntpath.basename(subdirpath)
		singlestring += subdirInfo + subdirname

	logger.log(singlestring)

	totalDirInfo = Sha1HashUtilities.HashString(singlestring)

	logger.log("inserting: size %d" % totalDirSize)
	insertCount += 1
	updateDirInfo(db, dirInfoCache, dirhash, totalDirInfo, totalDirSize, logger)

		
#--------------------- refactored code starts here ----------

# cache all directories, need both for cache and to iterate through dirs to do work
def getCacheOfAllDirs(db):
	command = "select * from %s;" % DbSchema.OriginalDirectoriesTable
	allDirsFromDb = db.ExecuteSqlQueryReturningMultipleRows(command)

	dirpathDict = dict(allDirsFromDb)
	# why are we sorting here? Look into, remove if not needed
	allDirsFromDb = sorted(allDirsFromDb,key=lambda x: x[1], reverse=True)
	return allDirsFromDb, dirpathDict


def getCacheOfAllDirsAsDict(db):
	command = "select * from %s;" % DbSchema.OriginalDirectoriesTable
	allDirsFromDb = db.ExecuteSqlQueryReturningMultipleRows(command)
	dirpathDict = dict(allDirsFromDb)
	return dirpathDict



# the meat of the code, fills in dirinfo and size for all dirs
def populateDirInfoFields(db, logger):
	allDirsFromDb, dirpathDict = getCacheOfAllDirs(db)

	# cache this to save repeated queries
	parentDirs = getParentDirs(db)

	# cache dirInfoTable as well, again to save queries
	dirInfoCache = getEntireDirInfoTable(db, logger)

	# should not need to do this again, only the first time
	# depends on how I handle imports.
	#insertAncestorDirsIntoDb(db, allDirsFromDb)
	#insertParentsIntoDb(db, allDirsFromDb, logger)


	#for i in range(10):
	#	entry = allDirsFromDb[i]

	logger.log("have %d dirs" % len(allDirsFromDb))

	for i, entry in enumerate(allDirsFromDb):
		dirhash = entry[0]
		dirpath = entry[1]
		logger.log(dirhash)
		logger.log("%d:%s" % (i,dirpath))


		oldTotalDirSize = dirInfoCache[dirhash][3]
		#logger.log(oldTotalDirSize)
		if not oldTotalDirSize is None:
			logger.log("size has value, already inserted, skipping")
			continue

		#logger.log( dirInfoCache[dirhash])
		#logger.log("why are we here?")
		subDirs = getSubDirsUsingCache(dirhash, parentDirs, logger)
		#logger.log(subDirs)

		if subDirs:
			#logger.log("has subdirs, attempting to insert")
			updateDirInfoGeneralCase(db, dirhash, subDirs, dirInfoCache, dirpathDict)
		else:
			#logger.log("leaf dir, attempt to insert")
			updateDirInfoForLeafNode(db, dirhash, dirInfoCache, logger)

	logger.log("inserted: %d" % insertCount)



def checkIfParentDirAlreadyInList(dirpath, dupsList, logger):
	for row in dupsList:
		if dirpath.startswith(row[1]):
			#logger.log("found match")
			#logger.log(row[1])
			#logger.log(dirpath)
			return True

	#logger.log("NOT A MATCH")
	return False




def listDuplicateDirsBySize(db, logger):

	dirpathDict = getCacheOfAllDirsAsDict(db)

	dirInfoCache = getEntireDirInfoTable(db, logger)
	dirInfoList = [(dirhash, v[2], v[3]) for dirhash, v in dirInfoCache.iteritems()]

	#dirInfoList is now list of tuples (dirhash, dirValue, dirSize)

	# now sort by size
	dirInfoList = sorted(dirInfoList, key=lambda x:x[2], reverse=True)

	#for entry in dirInfoList:
	#	logger.log(entry)


	dupsList = []

	# find largest duplicate dir tree
	for i in range(1, len(dirInfoList)):
		currentDirHash = dirInfoList[i][0]
		currentDirValue = dirInfoList[i][1]
		currentDirSize = dirInfoList[i][2]
		previousDirHash = dirInfoList[i-1][0]
		previousDirValue = dirInfoList[i-1][1]
		previousDirSize = dirInfoList[i-1][2]

		if currentDirSize is None:
			continue

		if currentDirValue is None:
			continue

		if previousDirSize is None:
			continue

		if previousDirValue is None:
			continue

		if currentDirSize == previousDirSize and \
			currentDirValue == previousDirValue:

			currentDirPath = dirpathDict[currentDirHash]
			previousDirPath = dirpathDict[previousDirHash]

			# if either path is subdir of path already in our list, then skip
			# subdirs of dups are obviously going to match as dups
			# we will miss some real dups as going overboard with matching, but that's ok for now
			if checkIfParentDirAlreadyInList(currentDirPath, dupsList, logger):
				continue
			if checkIfParentDirAlreadyInList(previousDirPath, dupsList, logger):
				continue

			dupsList.append((previousDirHash, previousDirPath, previousDirSize))
			dupsList.append((currentDirHash, currentDirPath, currentDirSize))

			print(len(dupsList))

			#if len(dupsList) > 100:
			#	break
	

	numOfDupPairs = len(dupsList)/2
	logger.log("found %d sets of dups" % numOfDupPairs)

	logger.log("DUPS: ")

	for i in range(0, len(dupsList)-1, 2):
		entry1 = str(dupsList[i])
		path1 = dupsList[i][1]
		entry2 = str(dupsList[i + 1])
		path2 = dupsList[i+1][1]

		#if "cd" in path1.lower():
		#	continue
		#if "cd" in path2.lower():
		#	continue
		#if "dvd" in path1.lower():
		#	continue
		#if "dvd" in path2.lower():
		#	continue
		#if "disk" in path1.lower():
		#	continue
		#if "disk" in path2.lower():
		#	continue
		#if "disc" in path1.lower():
		#	continue
		#if "disc" in path2.lower():
		#	continue
		#if "resources" in path1.lower():
		#	continue
		#if "resources" in path2.lower():
		#	continue

		s = "G:\\2004b\\20040613_robert\\jpg_cd\\".lower()
		if s in path1.lower():
			continue
		if s in path2.lower():
			continue

		#s = "carbonite".lower()
		#if s in path1.lower():
		#	continue
		#if s in path2.lower():
		#	continue




		# if cd or dvd in one pathname do not comment it out:
		if ("\\cd\\" in entry1.lower()) and ("\\cd\\" not in entry2.lower()):
			logger.log("#%s, " % entry1)
			logger.log("%s, " % entry2)
		elif ("\\cd\\" in entry2.lower()) and ("\\cd\\" not in entry1.lower()):
			logger.log("%s, " % entry1)
			logger.log("#%s, " % entry2)
		elif ("\\dvd\\" in entry1.lower()) and ("\\dvd\\" not in entry2.lower()):
			logger.log("#%s, " % entry1)
			logger.log("%s, " % entry2)
		elif ("\\dvd\\" in entry2.lower()) and ("\\dvd\\" not in entry1.lower()):
			logger.log("%s, " % entry1)
			logger.log("#%s, " % entry2)
		elif ("carbonite" in entry1.lower()) and ("carbonite" not in entry2.lower()):
			logger.log("#%s, " % entry1)
			logger.log("%s, " % entry2)
		elif ("carbonite" in entry2.lower()) and ("carbonite" not in entry1.lower()):
			logger.log("%s, " % entry1)
			logger.log("#%s, " % entry2)

		# otherwise comment out the shorter entry
		elif len(entry1) > len(entry2):
			logger.log("%s, " % entry1)
			logger.log("#%s, " % entry2)
		else:
			logger.log("#%s, " % entry1)
			logger.log("%s, " % entry2)





def listDirsBySize(db, logger):

	dirpathDict = getCacheOfAllDirsAsDict(db)

	dirInfoCache = getEntireDirInfoTable(db, logger)
	dirInfoList = [(dirhash, v[2], v[3]) for dirhash, v in dirInfoCache.iteritems()]

	#dirInfoList is now list of tuples (dirhash, dirValue, dirSize)

	# now sort by size
	dirInfoList = sorted(dirInfoList, key=lambda x:x[2], reverse=True)

	dirListBySize = []
	for i in range(10000):
		dirhash = dirInfoList[i][0]
		dirsize = dirInfoList[i][2]
		dirpath = dirpathDict[dirhash]

		#if not "Windows 8 Release Preview Metro style app samples" in dirpath:
		#	continue
		dirListBySize.append((dirhash, dirpath, dirsize))

	for entry in dirListBySize:
		logger.log("#%s, " % str(entry))




db = CoreDb.CoreDb("/Users/v724660/fmapp/listingDb.sqlite")
logger = DbLogger.dbLogger()

#populateDirInfoFields(db, logger)

listDuplicateDirsBySize(db, logger)

#listDirsBySize(db, logger)