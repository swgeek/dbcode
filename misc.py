# ad hoc stuff for now.
import DbInterface
import os
import ctypes



# copied from depotListing. Obviously TODO: put in common file.
objectStoresTable = "objectStores"
FileListingTable = "fileListing"
LocationCountTable = "locationCount"

# get number of files without backup copy for each depot
def getCountOfNonBackedUpForEachDepot(db):
	command = "select depotId from %s" % objectStoresTable
	results = db.ExecuteSqlQueryReturningMultipleRows(command)
	print "depotList"
	print results

	for row in results:
		depotId = row[0]
		print "depot %d" % depotId

		command = "select filehash from %s join %s using (filehash) where locations = 1 and depotId = %d limit 10" % (FileListingTable, LocationCountTable, depotId)

		results = db.ExecuteSqlQueryReturningMultipleRows(command)
		print "with 1"
		print results


def windowsSpecificGetFreeSpace(drive):
	freeSpace = ctypes.c_ulonglong(0)
	ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(drive), None, None, ctypes.pointer(freeSpace))
	return freeSpace


def backupSingleLocationFilesInDepot(db, depotId, backupDepotId):
	command = "select path from objectStores where depotId = %d" % depotId
	depotPath = db.ExecuteSqlQueryForSingleString(command)
	print "depot: %s" % depotPath

	command = "select path from objectStores where depotId = %d" % backupDepotId
	backupDepotPath = db.ExecuteSqlQueryForSingleString(command)
	print "backup depot: %s" % backupDepotPath

	drive, path = os.path.splitdrive(backupDepotPath)
	print "drive is %s" % drive

	print windowsSpecificGetFreeSpace(drive)
	exit(1)

	command = "select filehash, filesize from %s join %s using (filehash) where locations = 1 and depotId = %d" % (FileListingTable, LocationCountTable, depotId)
	count = 0

	reader = db.ExecuteSqlQueryReturningReader(command)
	row = reader.next()
	while row:
		filehash = row[0]
		filesize = row[1]
		filepath = os.path.join(depotPath, filehash)
		newFilePath = os.path.join(backupDepotPath, filehash)

		# check if have space first!
		print "copying %s to %s" % (filepath, newFilePath)
		print "size = %d" % filesize
		print count
		count += 1
		row = reader.next()




databaseFilePathName = "C:\\depotListing\\listingDb.sqlite"
db = DbInterface.DbInterface(databaseFilePathName)

backupSingleLocationFilesInDepot(db, 1, 11)

'''
	for row in results:
		depotId = row[0]
		command = "select count(filehash) from %s where depotId = %d" % (FileListingTable, depotId)
		count = db.ExecuteSqlQueryReturningSingleInt(command)
		print "%d: %d" % (depotId, count)
'''

