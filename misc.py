# ad hoc stuff for now.
import DbInterface
import DepotInterface
import os
import ctypes
import shutil



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
	return freeSpace.value


def backupSingleLocationFilesInDepot(db, depotId, backupDepotId):
	command = "select path from objectStores where depotId = %d" % depotId
	depotPath = db.ExecuteSqlQueryForSingleString(command)
	print "depot: %s" % depotPath

	command = "select path from objectStores where depotId = %d" % backupDepotId
	backupDepotPath = db.ExecuteSqlQueryForSingleString(command)
	print "backup depot: %s" % backupDepotPath

	drive, path = os.path.splitdrive(backupDepotPath)
	print "drive is %s" % drive

	command = "select filehash, filesize from %s join %s using (filehash) where locations = 1 and depotId = %d" % (FileListingTable, LocationCountTable, depotId)
	count = 0

	reader = db.ExecuteSqlQueryReturningReader(command)
	row = reader.next()
	while row:
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

	print "copied %d files" % count







#backupSingleLocationFilesInDepot(db, 1, 11)

oldDatabaseFilePathName = "C:\\depot\\db.sqlite"









#DepotInterface.createFilenameCountTable(db)
#DepotInterface.initializeFilenameCounts(db)
#DepotInterface.getCountOfFilenameCounts(db)

