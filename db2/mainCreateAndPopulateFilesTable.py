import os

import CoreDb
import DbLogger
import DbSchema
import miscQueries

def createFilesTable(db):
	createTableCommand = "create table %s (%s);" % (DbSchema.newFilesTable, DbSchema.newFilesSchema)
	db.ExecuteNonQuerySql(createTableCommand)

def dropFilesTable(db):
	createTableCommand = "drop table %s;" % DbSchema.newFilesTable
	db.ExecuteNonQuerySql(createTableCommand)

def getNumberOfFileEntries(db):
	command = "select count(*) from %s;" % DbSchema.newFilesTable
	count = db.ExecuteSqlQueryReturningSingleInt(command)
	return count

db = CoreDb.CoreDb("C:\\depotListing\\listingDb.sqlite")
logger = DbLogger.dbLogger()

depotRoot = "I:\\objectstore1"
depotId = 1				# cheating, hardcoding instead of getting from db
logger.log("depot id: %d,  root: %s" % (depotId, depotRoot))

count = getNumberOfFileEntries(db)
logger.log("%d files at start" % count)

#createFilesTable(db)

for dirName in os.listdir(depotRoot):
	logger.log( "adding %s" % dirName )
	subdirPath = os.path.join(depotRoot, dirName)

	files = os.listdir(subdirPath)
	logger.log("\t%d files in %s" % (len(files), dirName))

	fileInfoList = []
	for filename in files:
		if len(filename) != 40:
			logger.log("error: filename %s is wrong length" % filename)
			exit(1)
		# check filename format fits
		filepath = os.path.join(subdirPath, filename)
		filesize = os.path.getsize(filepath)
		
		fileInfoList.append((filename, filesize, depotId))

	logger.log("trying to make %d entries" % len(fileInfoList))
	# should first check for duplicate entries, assuming none for now as first time
	miscQueries.insertMultipleFileEntries(db, fileInfoList)
	logger.log( "finished %s\n" % dirName )

count = getNumberOfFileEntries(db)
logger.log("%d files at end" % count)

