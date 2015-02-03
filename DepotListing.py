
import DbInterface
import os.path
import SHA1HashUtilities

objectStoresTable = "objectStores"
objectStoresSchema = "depotId INTEGER PRIMARY KEY AUTOINCREMENT, description varchar(500), path varchar(500)"

FileListingTable = "fileListing"
FileListingSchema = "filehash char(40), depotId INTEGER, filesize int, FOREIGN KEY (depotId) REFERENCES objectStores(depotId), PRIMARY KEY (filehash, depotId)"

LocationCountTable = "locationCount"
LocationCountSchema = "filehash char(40) PRIMARY KEY, locations INTEGER"

def createListingDb(dbFilePath):

	directory, filename = os.path.split(dbFilePath)
	if not os.path.isdir(directory):
		raise Exception("dir %s does not exist" % directory)

	if os.path.isfile(dbFilePath):
		raise Exception("file %s already exists" % dbFilePath)

	DbInterface.DbInterface.CreateEmptyDbFile(dbFilePath)
	newDb = DbInterface.DbInterface(dbFilePath)

	newDb.OpenConnection()

	createTableCommand = "create table %s (%s);" % (objectStoresTable, objectStoresSchema)
	newDb.ExecuteNonQuerySql(createTableCommand)

	createTableCommand = "create table %s (%s);" % (FileListingTable, FileListingSchema)
	newDb.ExecuteNonQuerySql(createTableCommand)

	newDb.CloseConnection()


def createLocationCountTable(db):
	dropTableCommand = "drop table %s" % LocationCountTable
	db.ExecuteNonQuerySql(dropTableCommand)

	createTableCommand = "create table %s (%s);" % (LocationCountTable, LocationCountSchema)
	db.ExecuteNonQuerySql(createTableCommand)


def initializeLocationCounts(db):
	command = "insert into %s (filehash, locations) select filehash, count(filehash) from %s group by filehash;" % (LocationCountTable, FileListingTable)
	db.ExecuteNonQuerySql(command)


def getCountOfLocationCounts(db):
	command = "select locations, count(locations) from %s group by locations;" % LocationCountTable
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	for row in results:
		print row

def incrementLocationCountWithinTransaction(db, filehash):
	command = "select locations from %s where filehash = \"%s\"" % (LocationCountTable, filehash)
	oldCount = db.ExecuteSqlQueryReturningSingleInt(command)
	newCount = oldCount + 1
	command = "update %s set locations = %d where filehash = \"%s\";" % (LocationCountTable, newCount, filehash)
	db.ExecuteNonQuerySqlWithinTransaction(command)


def newDepotEntry(db, description, path):
	db.OpenConnection()

	command = "insert into %s (description, path) values (\"%s\", \"%s\");" % (objectStoresTable, description, path)
	db.ExecuteNonQuerySql(command)

	command = "select depotId from %s where description = \"%s\" and path = \"%s\" " % (objectStoresTable, description, path)
	depotId = db.ExecuteSqlQueryReturningSingleInt(command)

	db.CloseConnection()

	return depotId


def newFileListingEntry(db, filehash, depotId, filesize):
	db.OpenConnection()
	command = "insert into %s (filehash, depotId, filesize) values (\"%s\", \"%s\", %d);" % (FileListingTable, filehash, depotId, filesize)
	db.ExecuteNonQuerySql(command)
	db.CloseConnection()


def newFileListingEntryAsPartOfTransaction(db, filehash, depotId, filesize):
	command = "insert into %s (filehash, depotId, filesize) values (\"%s\", \"%s\", %d);" % (FileListingTable, filehash, depotId, filesize)
	db.ExecuteNonQuerySqlWithinTransaction(command)


def getCountForEachDepot(db):
	command = "select depotId from %s" % objectStoresTable
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	print "depotList"
	for row in results:
		depotId = row[0]
		command = "select count(filehash) from %s where depotId = %d" % (FileListingTable, depotId)
		count = db.ExecuteSqlQueryReturningSingleInt(command)
		print "%d: %d" % (depotId, count)





databaseFilePathName = "C:\\depotListing\\listingDb.sqlite"

#createListingDb("C:\\depotListing\\listingDb.sqlite")

db = DbInterface.DbInterface(databaseFilePathName)


# depot 1 in db
#depotPath = "G:\\objectstore1"
#depotId =  newDepotEntry(db, "main store, 4tb drive", depotPath)

#depot 2 in db
#depotPath = "H:\objectstore2"
#depotId =  newDepotEntry(db, "newest store, 750gb drive", depotPath)

#depot 4 in db
#depotPath = "H:\\backup1a"
#depotId =  newDepotEntry(db, "backup1a, 1GB drive", depotPath)

#depot 5 in db
#depotPath = "H:\\backup1b"
#depotId =  newDepotEntry(db, "backup1b, 1TB drive", depotPath)

#depot 6 in db
#depotPath = "H:\\backup1c"
#depotId =  newDepotEntry(db, "backup1c, 1TB drive", depotPath)

#depot 7 in db
#depotPath = "H:\\deletedFiles"
#depotId =  newDepotEntry(db, "deletedFiles, 2TB drive", depotPath)

#depot 8 in db
#depotPath = "H:\\deletedFilesBackup1"
#depotId =  newDepotEntry(db, "deletedFiles backup, 2TB drive", depotPath)

#depot 9 in db
#depotPath = "H:\\objectStore1Backup"
#depotId =  newDepotEntry(db, "deletedFiles backup, 2TB drive", depotPath)

#depot 10 in db
#depotPath = "F:\objectstore2Backup"
#depotId =  newDepotEntry(db, "new store backup, 750G drive", depotPath)

#depot 11 in db


depotPath = "F:\\moreBackup"
depotId =  newDepotEntry(db, "new store backup, 750G drive", depotPath)
print "depotid is %d" % depotId

exit(1)

db.startTransaction()

for dirName in os.listdir(depotPath):
	dirPath = os.path.join(depotPath, dirName)
	print "processing %s" % dirPath
	for fileName in os.listdir(dirPath):
		partialFileName, fileExtension = os.path.splitext(fileName)
		if fileExtension != "":
			print "Error: file %s has extension %s" % (fileName, fileExtension)
			exit(1)
		# get filesize
		# add into database

		filesize = os.path.getsize(os.path.join(dirPath, fileName))
		newFileListingEntryAsPartOfTransaction(db, fileName, depotId, filesize)
		#incrementLocationCountWithinTransaction(db, fileName)

db.EndTransaction()

getCountForEachDepot(db)

getCountOfLocationCounts(db)

createLocationCountTable(db)
initializeLocationCounts(db)
getCountOfLocationCounts(db)
