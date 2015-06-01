# ad hoc stuff for now.
import sortLater
import DbSchema
import CoreDb
import DepotInterface
import os
import DbHelper
#backupSingleLocationFilesInDepot(db, 1, 11)

#oldDatabaseFilePathName = "C:\\depot\\db.sqlite"

databaseFilePathName = "C:\\depotListing\\listingDb.sqlite"
db = CoreDb.CoreDb(databaseFilePathName)


databaseFilePathName2 = "C:\\depotListing\\backup3\\listingDb.sqlite"
db2 = CoreDb.CoreDb(databaseFilePathName2)
#DepotInterface.updateDepotPath(db, 2, "E:\\objectstore2")

#DepotInterface.ReInitializeLocationCounts(db)

ToDeleteTable = "toDelete"

depotInfo = DepotInterface.getDepotInfo(db)
for row in depotInfo:
	print row

#moveFilesFromOneDepotToAnother(db, 7, 2)

#countOverlapBetweenTwoDepots(db, 2, 7)
#getCountOfFilesInDepot(db, 10)
#printNumOfCopiesOfEachFileInDepot(db, 10)

#removeDepotAndAllReferences(db, 7)

#DepotInterface.createFilenameCountTable(db)
#DepotInterface.initializeFilenameCounts(db)
#DepotInterface.getCountOfFilenameCounts(db)

#NotFoundFilesTable = "notFoundV2"
#NotFoundFilesSchema = "filehash char(40) PRIMARY KEY, oldStatus varchar(60), filesize int, filenames varchar(500), directories varchar(500)"

#ToDeleteSchema = "filehash char(40) PRIMARY KEY, oldStatus varchar(60), filesize int, filenames varchar(500), directories varchar(500)"
#FilesSchema = "filehash char(40) PRIMARY KEY, filesize int, status varchar(60)"

dbhelper = DbHelper.DbHelper(databaseFilePathName)
command = "select filehash from %s;" % DbSchema.ToDeleteTable
results =  db2.ExecuteSqlQueryReturningMultipleRows(command)
for row in results:
	filehash = row[0]
	#print filehash
	status = dbhelper.getFileStatus(filehash)
	print status
	if status == "None":
		dbhelper.setFileStatus(filehash, "deleted")

exit(1)

#command = "insert into %s select * from %s where filehash not in (select filehash from %s)" % (DbSchema.NotFoundFilesTable, DbSchema.ToDeleteTable, DbSchema.FileListingTable)

def getPathOfHashFile(depotRootPath, filehash):
	subdir = filehash[0:2]
	return os.path.join(depotRootPath, subdir, filehash)

def getDepotPaths(db):
	depotPaths = {}
	depotInfo = DepotInterface.getDepotInfo(db)
	for row in depotInfo:
		depotPaths[row[0]] = row[2]
	return depotPaths


def removeFilesInToDeleteTable(db):
	depotPaths = getDepotPaths(db)
	command = "select filehash, depotId from %s join %s using (filehash);" % (DbSchema.ToDeleteTable, DbSchema.FileListingTable)
	results =  db.ExecuteSqlQueryReturningMultipleRows(command)
	for row in results:
		filehash = row[0]
		depotId = row[1]
		print filehash
		print depotId
		rootPath = depotPaths[depotId]
		hashedFilePath = getPathOfHashFile(rootPath, filehash)
		print hashedFilePath
		if os.path.isfile(hashedFilePath):
			os.remove(hashedFilePath)
			command = "delete from %s where filehash = '%s' and depotId = %d;" % (DbSchema.FileListingTable, filehash, depotId )
			db.ExecuteNonQuerySql(command)
			# remove from locations
		#shutil.copyfile(filepath, newFilePath)

# remove from FileListingTable!
#BF81EF31D12A0869C88F47C82E20BAD906A8CF87 2
command = "delete  from %s;" % DbSchema.ToDeleteTable
db.ExecuteNonQuerySql(command)
command = "select * from %s;" % DbSchema.ToDeleteTable
print db.ExecuteSqlQueryReturningMultipleRows(command)

#removeFilesInToDeleteTable(db)

exit(1)

#command = "select count(*) from %s join %s using (filehash);" % (DbSchema.ToDeleteTable, DbSchema.FilesTable)
#command = "select count(*) from %s;" % (DbSchema.ToDeleteTable)

command = "select * from %s;" % (DbSchema.ToDeleteTable)
results =  db.ExecuteSqlQueryReturningMultipleRows(command)
for row in results:
	filehash = row[0]
	oldStatus = row[1]
	filesize = row[2]
	filename = row[3]
	dirString = row[4]
	dirs = dirString.split()
	print "found:", filehash, oldStatus, filesize, filename
	command = "select * from %s where filehash = '%s';" % (DbSchema.OriginalDirectoryForFileTable, filehash)
	print db.ExecuteSqlQueryReturningMultipleRows(command)
	for dirPathHash in dirs:
		print "dirpathHash is %s" % dirPathHash
		#command = "select * from %s where filehash = %s and filename = %s and dirPathHash = %s;" % (DbSchema.OriginalDirectoryForFileTable, filehash, filename, dirPathHash)
		command = "insert or replace into %s (filehash, filename, dirPathHash) values (\"%s\", \"%s\", \"%s\");" % (DbSchema.OriginalDirectoryForFileTable, filehash, filename, dirPathHash)
		#db.ExecuteNonQuerySql(command)
		#print command

#NotFoundFilesTable = "notFoundV2"
#NotFoundFilesSchema = "filehash char(40) PRIMARY KEY, oldStatus varchar(60), filesize int, filenames varchar(500), directories varchar(500)"
#OriginalDirectoryForFileTable = "originalDirectoryForFile"
#OriginalDirectoryForFileSchema = "filehash char(40), filename varchar(500), dirPathHash char(40), PRIMARY KEY (filehash, filename, dirPathHash)"


#OldStatusTable = "oldStatus"
#OldStatusSchema = "filehash char(40) PRIMARY KEY, oldStatus varchar(60)"
