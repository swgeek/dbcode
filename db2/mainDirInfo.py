import CoreDb
import DbLogger
import DbSchema
import miscQueries
import Sha1HashUtilities


def createDirInfoTable(db):
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
	values = db.ExecuteSqlQueryReturningSingleRow(command)
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


def doTotalFileInfoForDir(db, dirPathHash, logger):

	totalFileSize = 0

	logger.log("doing dirPathHash %s" % dirPathHash)	

	dirpath = miscQueries.getDirectoryPath(db, dirPathHash)
	logger.log("original path: %s" % dirpath)

	# get list of files in this dir
	logger.log("files:")
	filelist = miscQueries.getAllFilesFromDir(db, dirPathHash)
	for entry in filelist:
		logger.log(entry)

	if not filelist:
		updateFileInfoForDir(db, dirPathHash, 0)
		return

	filelist.sort()

	logger.log("sorted and with filesize")

	for entry in filelist:
		logger.log(entry)
		filehash = entry[0]
		filesize = getFileSize(db, filehash)
		logger.log(filesize)
		if filesize is None:
			logger.log("###################NO FILESIZE FOR %s" % filehash)
			
			logger.log(getFileEntry(db, filehash))
			exit(1)


		totalFileSize += filesize

	logger.log("joined into single string")

	singlestring = "".join("".join(x) for x in filelist)
	logger.log(singlestring)
	singlestringUTF8 = singlestring.encode('utf-8')
	logger.log(singlestring)

	logger.log("singlestring hash")

	totalFileInfo = Sha1HashUtilities.HashString(singlestringUTF8)
	logger.log(totalFileInfo)
	logger.log(totalFileSize)

	updateFileInfoForDir(db, dirPathHash, totalFileSize, totalFileInfo)




db = CoreDb.CoreDb("C:\\depotListing\\listingDb.sqlite")
logger = DbLogger.dbLogger()

#createDirInfoTable(db)
#copyDirHashValues(db)

#count = getNumberOfEntries(db, DbSchema.TempDirInfoTable)
#logger.log(count)

#count = getNumberOfEntries(db, DbSchema.OriginalDirectoriesTable)
#logger.log(count)


for i in range(300):
	dirPathHash = getDirHashWithFileInfoTodo(db)
	logger.log("doing dirPathHash %s" % dirPathHash)
	doTotalFileInfoForDir(db, dirPathHash, logger)


