import CoreDb
import DbLogger
import DbSchema
import miscQueries
import Sha1HashUtilities
import time


def stringToAscii(s):
	#logger.log("entering")
	#logger.log(s)
	#logger.log(type(s))
	#logger.log("decoding to unicode")
	try:
		sUnicode = s.decode('utf-8', errors="replace")
	except:
		logger.log("utf-8 did not work, trying 16")
		#sUnicode = s.decode('utf-16')
		sUnicode = s
	#logger.log(sUnicode)
	#logger.log("encoding to ascii")
	sAscii = sUnicode.encode('ascii', 'replace')
	#logger.log(sAscii)
	#singlestringUTF8 = singlestring.encode('utf-8')
	return sAscii


def portDataIntoTempDirForFileTable(db):

	dropTableCommand = "drop table %s;" % DbSchema.TempDirectoryForFileTable
	db.ExecuteNonQuerySql(dropTableCommand)
	createTableCommand = "create table %s (%s);" % (DbSchema.TempDirectoryForFileTable, DbSchema.TempDirectoryForFileSchema)
	db.ExecuteNonQuerySql(createTableCommand)

	command = "insert into %s (filehash, filename, dirPathHash, filesize) " % DbSchema.TempDirectoryForFileTable +\
		"select filehash, filename, dirPathHash, filesize from %s " % DbSchema.OriginalDirectoryForFileTable +\
		"join %s using (filehash) " % DbSchema.newFilesTable
	db.ExecuteNonQuerySql(command)


def getTempFileDirInfoStartingWithChar(db, startChar):
	command = "select dirPathHash, filehash, filename, filesize from %s " % DbSchema.TempDirectoryForFileTable + \
		"where dirPathHash like '%s%%';" % startChar
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	return result


def getListOfToDoDirsStartingWithChar(db, startChar, number):
	command = "select dirPathHash from %s " % DbSchema.TempDirInfoTable + \
		"where dirPathHash like '%s%%' and totalFileSize is null limit %s;" % (startChar,number)
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	return result


def getTempDirEntriesNotInOriginalDir(db):
	command = "select filehash, filename, dirPathHash from %s " % DbSchema.OriginalDirectoryForFileTable +\
				"where filehash not in (select filehash from %s); " % DbSchema.TempDirectoryForFileTable
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	return result


def createDirInfoTable(db):
	dropTableCommand = "drop table %s;" % DbSchema.TempDirInfoTable
	db.ExecuteNonQuerySql(dropTableCommand)
	createTableCommand = "create table %s (%s);" % (DbSchema.TempDirInfoTable, DbSchema.TempDirInfoSchema)
	db.ExecuteNonQuerySql(createTableCommand)


def copyDirHashValues(db):
	command = "INSERT INTO %s (dirPathHash) SELECT dirPathHash FROM %s;" % (DbSchema.TempDirInfoTable, DbSchema.OriginalDirectoriesTable)
	db.ExecuteNonQuerySql(command)



def getNumberOfEntries(db, tableName):
	command = "select count(*) from %s" % tableName
	return db.ExecuteSqlQueryReturningSingleInt(command)


def getDirHashWithFileInfoTodo(db):
	command = "select dirPathHash from %s where totalFileSize is null limit 1" % DbSchema.TempDirInfoTable
	#command = "select * from %s where totalFileSize > 1000000 limit 1" % DbSchema.TempDirInfoTable
	#values =  db.ExecuteSqlQueryReturningSingleRow(command)
	#logger.log(values)
	#command = "select dirPathHash from %s where totalFileSize > 1000000 limit 1" % DbSchema.TempDirInfoTable
	values =  db.ExecuteSqlQueryReturningSingleRow(command)
	if values != None:
		return values[0]


def updateFileInfoForDir(db, dirPathHash, totalFileSize, totalFileInfo = "0"):
	command = "update %s set totalFileSize = \"%d\", totalFileInfo = \"%s\" where dirPathHash = \"%s\";" % (DbSchema.TempDirInfoTable, totalFileSize, totalFileInfo, dirPathHash)
	db.ExecuteNonQuerySql(command)


def getFileSize(db, filehash):
	command = "select filesize from %s where filehash = \"%s\";" % (DbSchema.newFilesTable, filehash)
	return db.ExecuteSqlQueryReturningSingleInt(command)


def getFileEntry(db, filehash):
	command = "select * from %s where filehash = \"%s\";" % (DbSchema.newFilesTable, filehash)
	return db.ExecuteSqlQueryReturningSingleRow(command)


def getInfoForAllFilesFromDir(db, dirPathHash):
	command = "select filehash, filename, filesize from %s " % DbSchema.TempDirectoryForFileTable +\
				"where dirPathHash = \"%s\";" % dirPathHash
	result = db.ExecuteSqlQueryReturningMultipleRows(command)
	return result


def getInfoForAllFilesFromDirUsingList(tempDirTableContents, dirPathHash):
	result = []

	for entry in tempDirTableContents:
		if entry[0] == dirPathHash:
			result.append(entry[1:])

	print "FOUND: %d entries" % len(result)
	return result


def doTotalFileInfoForDir(db, dirPathHash, tempDirTableContents, logger):

	totalFileSize = 0

	logger.log("doing dirPathHash %s" % dirPathHash)	

	dirpath = miscQueries.getDirectoryPath(db, dirPathHash)
	logger.log("original path: %s" % dirpath)

	# get list of files in this dir
	#logger.log("files:")
	filelist = getInfoForAllFilesFromDirUsingList(tempDirTableContents, dirPathHash)
	logger.log("\t %d files" % len(filelist))
	#for entry in filelist:
	#	logger.log(entry)

	if not filelist:
		updateFileInfoForDir(db, dirPathHash, 0)
		return

	filelist.sort()

	#logger.log("sorted and with filesize")

	for entry in filelist:
		#logger.log(entry)
		filehash = entry[0]
		filesize = entry[2]
		#logger.log(filesize)
		if filesize is None:
			logger.log("###################NO FILESIZE FOR %s" % filehash)
			
			logger.log(getFileEntry(db, filehash))
			exit(1)

		filename = entry[1]

		try:
			asciiFilename = stringToAscii(filename)
			#filelist.append(asciiFilename)
			#logger.log(filename)
			#logger.log(asciiFilename)
		except:
			logger.log("exception, skipping")
			logger.log(entry)

		totalFileSize += filesize

	# string is list of files in format <filehash><filename>
	singlestring = "".join(x[0] + x[1] for x in filelist)
	singlestringAscii = stringToAscii(singlestring)
	logger.log(singlestringAscii)

	totalFileInfo = Sha1HashUtilities.HashString(singlestringAscii)
	logger.log("totalFileInfo: " + totalFileInfo)
	logger.log("totalFileSize: %s" % totalFileSize)
	logger.log(time.time())

	updateFileInfoForDir(db, dirPathHash, totalFileSize, totalFileInfo)


def getOriginalDirectoriesForFile(db, filehash):
	command = "select * from %s where filehash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, filehash)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	return results


db = CoreDb.CoreDb("/Users/v724660/fmapp/listingDb.sqlite")
logger = DbLogger.dbLogger()

#logger.log(time.time())

#logger.log("porting data into TempDirectoryForFileTable")
# takes about 6.5 seconds
#portDataIntoTempDirForFileTable(db)

# check if finished
dirhash = getDirHashWithFileInfoTodo(db)
print dirhash
exit(1)

startTime = time.time()
logger.log(startTime)
logger.log("calling getListOfToDoDirsStartingWithChar")
dirList = getListOfToDoDirsStartingWithChar(db, "F", 100000)
endTime = time.time()
logger.log(endTime)
timeTaken = endTime - startTime
logger.log("took %d seconds" % timeTaken)
logger.log(len(dirList))

dirFileListString = {}
dirTotalFileSize = {}

startTime = time.time()
logger.log(startTime)
logger.log("calling getTempFileDirInfoStartingWithChar")
tempDirTableContents = getTempFileDirInfoStartingWithChar(db, "F")
endTime = time.time()
logger.log(endTime) 
timeTaken = endTime - startTime
logger.log("took %d seconds" % timeTaken)

logger.log(len(tempDirTableContents))

#createDirInfoTable(db)
#copyDirHashValues(db)

for i, entry in enumerate(dirList):
	logger.log("entry %d" % i)
	dirPathHash = entry[0]
	doTotalFileInfoForDir(db, dirPathHash, tempDirTableContents, logger)
