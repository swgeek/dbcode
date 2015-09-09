
#
# trying for some stuff from mainDirInfo.
# this one will only get largest directory, not duplicates.
# Trying to do everything in memory instead of creating temp dirs (dirinfo, parent, etc)
#
# have to run mainAddAllAncestorsFirst!
#

import CoreDb
import DbLogger
import DbSchema
import miscQueries
import Sha1HashUtilities
import time
import ntpath

logger = None

'''

1: create parentdir mapping
2: create total filesizes for each directory
3: create total dirsize for each directory
4: sort by dirsize, print largest
'''
insertCount = 0


# cache all directories, need both for cache and to iterate through dirs to do work
def getAllDirs(db):
	command = "select * from %s;" % DbSchema.OriginalDirectoriesTable
	allDirsList = db.ExecuteSqlQueryReturningMultipleRows(command)
	dirpathDict = dict(allDirsList)
	return allDirsList, dirpathDict


def getParentDirs(dirpathDict, logger):
	parentDirs = {}

	for dirhash in dirpathDict:
		dirpath = dirpathDict[dirhash]
		parent = ntpath.dirname(dirpath)

		if parent == dirpath:
			# no parent, set parent to null (or can not put in dict, but this makes it more obvious)
			parentHash = None
		else:
			parentHash = Sha1HashUtilities.HashString(parent)

		parentDirs[dirhash] = parentHash

	return parentDirs


def getFileSizes(db):
	command = "select filehash, filesize from %s;" % DbSchema.newFilesTable
	filesizeList = db.ExecuteSqlQueryReturningMultipleRows(command)
	filesizeDict = dict(filesizeList)
	return filesizeDict


def getFileDirectories(db):
	command = "select filehash, dirPathHash from %s;" % DbSchema.OriginalDirectoryForFileTable
	fileDirectoryList = db.ExecuteSqlQueryReturningMultipleRows(command)
	return fileDirectoryList


def getTotalSizeOfFilesInDirectoryOnly(dirhash, filesizeDict, fileDirectoryList):
	filesInDirectory = [x[0] for x in fileDirectoryList if (x[1] == dirhash)  ]
	totalSize = 0
	for filehash in filesInDirectory:
		totalSize += filesizeDict[filehash]
	return totalSize


def getTotalDirSize(dirhash, parentDirs, totalDirSizeDict, filesizeDict, fileDirectoryList):
	totalDirSize = 0

	subDirList = [key for key in parentDirs if (parentDirs[key] == dirhash) ]
	for subdirhash in subDirList:
		if totalDirSizeDict.get(subdirhash) is None:
			# if subdir does not have totalsize, so cannot calculate for this dir
			return None
		totalDirSize += totalDirSizeDict[subdirhash]

	totalFileSize = getTotalSizeOfFilesInDirectoryOnly(dirhash, filesizeDict, fileDirectoryList)
	totalDirSize += totalFileSize
	return totalDirSize



def logTimestamp(logger):
	logger.log("time: %s" % time.strftime("%H:%M:%S"))


#dbpath = "C:\\depotListing\\listingDb.sqlite"
dbpath = "/Users/v724660/fmapp/listingDb.sqlite"
db = CoreDb.CoreDb(dbpath)
logger = DbLogger.dbLogger()

logTimestamp(logger)

logger.log("getting all dirs")
allDirsList, dirpathDict = getAllDirs(db)
logger.log("Have %d dirs" % len(dirpathDict))

logTimestamp(logger)
logger.log("getting parent dirs")

parentDirs = getParentDirs(dirpathDict, logger)

logTimestamp(logger)
logger.log("getting file directory mapping")

filesizeDict = getFileSizes(db)
fileDirectoryList = getFileDirectories(db)

logTimestamp(logger)

totalDirSizeDict = {}

# sort dirs list in reverse order so size calculation always done from leaf to ancester up the tree
allDirsList = sorted(allDirsList,key=lambda x: x[1], reverse=True)

for i, entry in enumerate(allDirsList):
	print "%d, " % i, 
	dirhash = entry[0]
	dirSize = getTotalDirSize(dirhash, parentDirs, totalDirSizeDict, filesizeDict, fileDirectoryList)
	if dirSize is not None:
		totalDirSizeDict[dirhash] = dirSize
	else:
		logger.log("had no result for %s" % str(entry))









'''



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

		s = "carbonite".lower()
		if s in path1.lower():
			continue
		if s in path2.lower():
			continue




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
	for i in range(20000):
		dirhash = dirInfoList[i][0]
		dirsize = dirInfoList[i][2]
		dirpath = dirpathDict[dirhash]

		#if not "Windows 8 Release Preview Metro style app samples" in dirpath:
		#	continue
		dirListBySize.append((dirhash, dirpath, dirsize))

	for entry in dirListBySize:
		logger.log("#%s, " % str(entry))



dbpath = "C:\\depotListing\\listingDb.sqlite"

db = CoreDb.CoreDb(dbpath)
logger = DbLogger.dbLogger()

#populateDirInfoFields(db, logger)

#listDuplicateDirsBySize(db, logger)

listDirsBySize(db, logger)


import CoreDb
import DbSchema
import DbLogger
import os
import miscQueries


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


#def DeleteFileFromDepot(depotRootPath, filehash):
# moved to CopyFilesEtc.py


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

#rootDir = "I:\\m\\fromUsbProbDups\\moved\\fromMac20130729\\moveToUsb\\opengles-book"
#logger.log(miscQueries.numberOfRows(db, DbSchema.OriginalDirectoryForFileTable))
#logger.log(miscQueries.numberOfRows(db, DbSchema.OriginalDirectoriesTable))
#dirs = getDirectoriesWithSearchString(db, rootDir)
#logger.log(len(dirs))
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



##############################
largestRow = miscQueries.getLargestFile(db)
#largestRow = (u'E3ED49B8581DE969A2FF2FC0B5DBB07B6FEBBB7C', 3954050048L, 1, None)


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
	logger.log("(\"%s\",\"%s\",0)," % (dirhash, dirpath))
	logger.log("")
	continue


if len(dirs) == 1:
	logger.log("only one dir, yay!")
	dirhash = dirs[0][2]
	logger.log("contents of dir:")
	# this part is manual, e.g. here assuming only one filename and one directory
	filesInDir = getFilesFromDir(db, dirhash)
	for fileinfo in filesInDir:
		logger.log(fileinfo)

	exit(1)
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

'''