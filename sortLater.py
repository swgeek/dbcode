import DbInterface
import DepotInterface
import os
import ctypes
import shutil
import DbSchema


# get number of files with n other copies in a depot
def getCountOfFilesWithNCopiesInDepot(db, depotId, n):
	print "depot %d" % depotId
	command = "select count(filehash) from %s join %s using (filehash) where locations = %d and depotId = %d" % (DbSchema.FileListingTable, DbSchema.LocationCountTable, n, depotId)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	print "with %d" % n
	print results


def getCountOfFilesInDepot(db, depotId):
	print "depot %d" % depotId
	command = "select count(filehash) from %s where depotId = %d" % (DbSchema.FileListingTable, depotId)
	return db.ExecuteSqlQueryReturningSingleInt(command)


def windowsSpecificGetFreeSpace(drive):
	freeSpace = ctypes.c_ulonglong(0)
	ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(drive), None, None, ctypes.pointer(freeSpace))
	return freeSpace.value


# will only do 10,000 files. Stupid sqlite will not let me update listing while reader is open.
# will try removing the limit after moving to postgres
def backupSingleLocationFilesInDepot(db, depotId, backupDepotId):
	command = "select path from objectStores where depotId = %d" % depotId
	depotPath = db.ExecuteSqlQueryForSingleString(command)
	print "depot: %s" % depotPath

	command = "select path from objectStores where depotId = %d" % backupDepotId
	backupDepotPath = db.ExecuteSqlQueryForSingleString(command)
	print "backup depot: %s" % backupDepotPath

	drive, path = os.path.splitdrive(backupDepotPath)
	print "drive is %s" % drive

	command = "select filehash, filesize from %s join %s using (filehash) where locations = 1 and depotId = %d limit 10000" % (DepotInterface.FileListingTable, DepotInterface.LocationCountTable, depotId)
	count = 0

	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	for row in results:
		filehash = row[0]
		filesize = row[1]

		# first check have space
		remainingSpace = windowsSpecificGetFreeSpace(drive)
		tenGig = 10000000000
		if filesize > (remainingSpace - tenGig):
			print "cannot copy file, nearly out of space"
			break

		subdir = filehash[0:2]
		filepath = os.path.join(depotPath, subdir, filehash)
		newFilePath = os.path.join(backupDepotPath, subdir, filehash)

		print "copying %s to %s" % (filepath, newFilePath)

		parentDir = os.path.dirname(newFilePath)
		if not os.path.isdir(parentDir):
			os.mkdir(parentDir)

		shutil.copyfile(filepath, newFilePath)

		print count
		count += 1
		row = reader.next()

		DepotInterface.newFileListingEntry(db, filehash, backupDepotId, filesize)

	print "copied %d files" % count


def printNumOfCopiesOfEachFileInDepot(db, depotId):
	getCountOfFilesWithNCopiesInDepot(db, depotId, 1)
	getCountOfFilesWithNCopiesInDepot(db, depotId, 2)
	getCountOfFilesWithNCopiesInDepot(db, depotId, 3)
	getCountOfFilesWithNCopiesInDepot(db, depotId, 4)
	getCountOfFilesWithNCopiesInDepot(db, depotId, 5)
	getCountOfFilesWithNCopiesInDepot(db, depotId, 6)
	getCountOfFilesWithNCopiesInDepot(db, depotId, 7)
	getCountOfFilesWithNCopiesInDepot(db, depotId, 8)


def countOverlapBetweenTwoDepots(db, depotId1, depotId2):
	tableName = DepotInterface.FileListingTable
	command = "select count(*) from %s as a join %s as b using (filehash)  where a.depotId = %d and b.depotId = %d;" % (tableName, tableName, depotId1, depotId2)
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	print results


# this assumes on same drive, so don't have to worry about disk space
def moveFilesFromOneDepotToAnother(db, sourceDepotId, destDepotId):
	command = "select path from objectStores where depotId = %d" % sourceDepotId
	sourceDepotPath = db.ExecuteSqlQueryForSingleString(command)
	print "source depot: %s" % sourceDepotPath
	if not os.path.isdir(sourceDepotPath):
		exit(1)

	command = "select path from objectStores where depotId = %d" % destDepotId
	destDepotPath = db.ExecuteSqlQueryForSingleString(command)
	print "source depot: %s" % destDepotPath
	if not os.path.isdir(destDepotPath):
		exit(1)

	print "got this far"

	# get path1, check exists
	# get path2, check does not exist
	# physically move file
	# update locations table


	for dirName in os.listdir(sourceDepotPath):

		db.startTransaction()
		sourceDirPath = os.path.join(sourceDepotPath, dirName)
		print "processing %s" % sourceDirPath
		destDirPath = os.path.join(destDepotPath, dirName)
		for fileName in os.listdir(sourceDirPath):
			partialFileName, fileExtension = os.path.splitext(fileName)
			if fileExtension != "":
				print "Error: file %s has extension %s" % (fileName, fileExtension)
				exit(1)
			# move file
			sourceFile = os.path.join(sourceDirPath, partialFileName)
			shutil.move(sourceFile, destDirPath)
			DepotInterface.updateFileListingEntryAsPartOfTransaction(db, partialFileName, sourceDepotId, destDepotId)

		db.EndTransaction()

 
def removeDepotAndAllReferences(db, depotId):
	command = "delete from %s where depotId = %d" %( DepotInterface.FileListingTable,  depotId) 
	db.ExecuteNonQuerySql(command)

	command = "delete from %s where depotId = %d" %( DepotInterface.objectStoresTable,  depotId) 
	db.ExecuteNonQuerySql(command)

def printFileInfo(db, filehash):
	print "file %s" % filehash
	dirList =  DepotInterface.getFilenamesAndDirectories(db, filehash)
	for row in dirList:
		print row
		filename, dirhash = row
		print "file %s dir %s" % (filename, dirhash)
		print DepotInterface.getDirectoryPath(db, dirhash)