# misc functions to port data from old database to new version

import DbInterface
import DepotInterface


objectStoresTable = "objectStores"
FileListingTable = "fileListing"
LocationCountTable = "locationCount"
oldDbFilesTable = "FilesV2"



# get filenames from old db, put in new
def portFilenames(oldDb, db):
	count = 0

	command = "select filehash, filename from OriginalDirectoriesForFileV5" 
	filehashReader = oldDb.GetDataReaderForSqlQuery(command)
	result = filehashReader.next()
	while result is not None:
		filehash, filename = result
		DepotInterface.addFilename(db, filehash, filename)
		count += 1
		if count % 1000 == 0:
			print count
		result = filehashReader.next()


# get status from old db, put in new
def portFileStatus(oldDb, db):
#	DepotInterface.createOldStatusTable(db)
	db.startTransaction()
	count = 0

	command = "select filehash, status from FilesV2" 
	reader = oldDb.GetDataReaderForSqlQuery(command)
	result = reader.next()
	while result is not None:
		filehash, status = result
		DepotInterface.addOldStatusAsPartOfTransaction(db, filehash, status)
		count += 1
		if count % 1000 == 0:
			print count
		result = reader.next()

	db.EndTransaction()



def portFileLink(oldDb, db):
	DepotInterface.createOldFileLinkTable(db)
	db.startTransaction()
	count = 0

	command = "select filehash, linkFileHash from FileLink" 
	reader = oldDb.GetDataReaderForSqlQuery(command)
	result = reader.next()
	while result is not None:
		filehash, link = result
		DepotInterface.addOldFileLinkAsPartOfTransaction(db, filehash, link)
		count += 1
		if count % 1000 == 0:
			print count
		result = reader.next()

	db.EndTransaction()



def portFiles(oldDb, db):
	db.startTransaction()
	count = 0

	command = "select filehash, filesize from %s" % oldDbFilesTable
	filehashReader = oldDb.GetDataReaderForSqlQuery(command)
	result = filehashReader.next()
	while result is not None:
		filehash, filesize = result
		DepotInterface.newFileEntryWithinTransaction(db, filehash, filesize)
		count += 1
		if count % 1000 == 0:
			print count
		result = filehashReader.next()

	db.EndTransaction()


# TODO: this code is the same for all port stuff, if use again then figure
# out a way to abstract it out
def portOriginalDirectories(oldDb, db):

	#DepotInterface.createOriginalDirectoriesTable(db)

	db.startTransaction()
	count = 0

	command = "select dirPathHash, dirPath from originalDirectoriesV2" 
	reader = oldDb.GetDataReaderForSqlQuery(command)
	result = reader.next()
	while result is not None:
		dirpathHash, dirpath = result
		DepotInterface.addDirPathAsPartOfTransaction(db, dirpathHash, dirpath)
		count += 1
		if count % 1000 == 0:
			print count
		result = reader.next()

	db.EndTransaction()


def portOldOriginalRootDirectoryTable(oldDb, db):

	DepotInterface.createOldOriginalRootDirectoryTable(db)

	db.startTransaction()
	count = 0

	command = "select rootdir from originalRootDirectories" 
	reader = oldDb.GetDataReaderForSqlQuery(command)
	result = reader.next()
	while result is not None:
		rootdir = result
		DepotInterface.addOldOriginalRootDirectoryAsPartOfTransaction(db, rootdir)
		count += 1
		if count % 1000 == 0:
			print count
		result = reader.next()

	db.EndTransaction()





def portOriginalDirectoriesForFile(oldDb, db):

	#DepotInterface.createOriginalDirectoryForFileTable(db)

	db.startTransaction()
	count = 0

	command = "select filehash, filename, dirPathHash from OriginalDirectoriesForFileV5" 
	reader = oldDb.GetDataReaderForSqlQuery(command)
	result = reader.next()
	while result is not None:
		filehash, filename, dirPathHash = result
		DepotInterface.addOriginalDirectoryForFileAsPartOfTransaction(db, filehash, filename, dirPathHash)
		count += 1
		if count % 1000 == 0:
			print count
		result = reader.next()

	db.EndTransaction()



def portSubDirListing(oldDb, db):

	#DepotInterface.createDirSubDirTable(db)

	db.startTransaction()
	count = 0

	command = "select dirPathHash, subdirs from SubdirListingForDir" 
	reader = oldDb.GetDataReaderForSqlQuery(command)
	result = reader.next()
	while result is not None:
		dirPathHash, subdirsString = result
		subdirsList = list(set(subdirsString.split(';')))
		for subDirPathHash in subdirsList:
			DepotInterface.addDirSubDirAsPartOfTransaction(db, dirPathHash, subDirPathHash)

		count += 1
		if count % 1000 == 0:
			print count

		result = reader.next()

	db.EndTransaction()



def tempCheckInBoth(oldDb, newDb):
	command = "select filehash from %s" % oldDbFilesTable
	reader = oldDb.ExecuteSqlQueryReturningReader(command)

	count0 = 0
	count1 = 0
	countOther = 0

	row = reader.next()
	while row:
		filehash = row[0]
		command2 = "select count(*) from %s where filehash = '%s';" % (FileListingTable, filehash)
		count = newDb.ExecuteSqlQueryReturningSingleInt(command2)
		if count == 0:
			count0 += 1
			print "found 0"

		elif count == 1:
			count1 += 1

		else:
			countOther += 1
			if (countOther) % 1000 == 0:
				print "countOther = %d" % countOther
		row = reader.next()

	print count0
	print count1
	print countOther


# copied from depotListing. Obviously TODO: put in common file.

oldDatabaseFilePathName = "C:\\depot\\db.sqlite"
oldDb = DbInterface.DbInterface(oldDatabaseFilePathName)
databaseFilePathName = "C:\\depotListing\\listingDb.sqlite"
db = DbInterface.DbInterface(databaseFilePathName)

#tempCheckInBoth(oldDb, db)
#portFiles(oldDb, db)
#portFilenames(oldDb, db)

#portFileStatus(oldDb, db)
#portFileLink(oldDb, db)

#portOriginalDirectories(oldDb, db)

#portOriginalDirectoriesForFile(oldDb, db)
#portOldOriginalRootDirectoryTable(oldDb, db)
#DepotInterface.printAllOldRootDirs(db)

#portSubDirListing(oldDb, db)
