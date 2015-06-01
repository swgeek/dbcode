import DbInterface
import DepotInterface
import sortLater

# all files in primary depots, i.e. 1 and 2. Everything better be in here.
TempPrimaryFilesTable = "primaryFiles"
TempPrimaryFilesSchema = "filehash char(40) PRIMARY KEY"


def quickTest(db):

	#DepotInterface.dropTable(db, DepotInterface.CurrentBackupTable)
	#DepotInterface.createCurrentBackupTable(db)
	print DepotInterface.checkIfFileBackedUpInCurrentPass(db, "000")
	DepotInterface.AddFileBackedUpInCurrentPass(db, "000")
	print DepotInterface.checkIfFileBackedUpInCurrentPass(db, "000")
	# should remove
	print DepotInterface.checkIfFileBackedUpInCurrentPass(db, "000")



def windowsSpecificGetFreeSpace(drive):
	freeSpace = ctypes.c_ulonglong(0)
	ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(drive), None, None, ctypes.pointer(freeSpace))
	return freeSpace.value


'''
def CopyFile(sourceFilePath, destRootDir):

		drive, path = os.path.splitdrive(backupDepotPath)
		print "drive is %s" % drive

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
'''

def AddAllPrimaryFilesToTable(db, primaryDepots):
	DepotInterface.createTable(db, TempPrimaryFilesTable, TempPrimaryFilesSchema)
	for depotId in primaryDepots:
		command = "insert into %s (filehash) select filehash from %s where depotId = %d;" % (TempPrimaryFilesTable, DepotInterface.FileListingTable, depotId)
		db.ExecuteNonQuerySql(command)

	command = "select count(*) from %s;" % TempPrimaryFilesTable
	print "primary files table has:"
	print db.ExecuteSqlQueryReturningSingleRow(command)
	print ""
	sum = 0
	for depotId in primaryDepots:
		c = sortLater.getCountOfFilesInDepot(db, depotId)
		print "depot %d has %d entries" % (depotId, c)
		sum += c
	print "total is %d " % sum


def countFilesInDepotNotInPrimaryFilesTable(db, depotId):
	t1 = DepotInterface.FileListingTable
	t2 = TempPrimaryFilesTable
	command = "select count(*) from %s where depotId = %d and not exists (select filehash from %s where %s.filehash = %s.filehash); " % (t1, depotId, t2, t1, t2)
	return db.ExecuteSqlQueryReturningSingleInt(command)


databaseFilePathName = "C:\\depotListing\\listingDb.sqlite"
db = DbInterface.DbInterface(databaseFilePathName)

sortLater.printDepotInfo(db)

#sortLater.backupSingleLocationFilesInDepot(db, 8, 2)

#DepotInterface.ReInitializeLocationCounts(db)

#countFilesInDepotNotInPrimaryFilesTable(db, 8)

#command = "select * from %s where filehash = '%s'; " % (t1, "00000AA1E899DA256B7B6BC0C08AFCFEA9D78FEF")
command = "select * from %s where filehash = '%s'; " % (DepotInterface.LocationCountTable, "00000AA1E899DA256B7B6BC0C08AFCFEA9D78FEF")
command = "select * from %s where filehash = '%s'; " % (DepotInterface.OriginalDirectoryForFileTable, "00000344C90A5385900B134AB60AB4654B74FC27")
command = "select * from %s where dirPathHash = '%s'; " % (DepotInterface.OriginalDirectoriesTable, "E62AB7970BA6F5204DE66B640DD9A1A3AA46EC42")

print db.ExecuteSqlQueryReturningMultipleRows(command)


# NOTE: These two files don't exist on drive, but do exist in depot 8 listing. Get rid of it from there and also from rest of depot
#[(u'00000344C90A5385900B134AB60AB4654B74FC27', 8, 324), (u'00000AA1E899DA256B7B6BC0C08AFCFEA9D78FEF', 8, 258)]

#for depotId in [1, 2, 4, 5, 6, 8, 9, 10, 11, 12]:
#	print "%d: %d " % (depotId, countFilesInDepotNotInPrimaryFilesTable(db, depotId))
#AddAllPrimaryFilesToTable(db, [1, 2])

#quickTest(db)
#DepotInterface.dropTable(db, DepotInterface.CurrentBackupTable)
#DepotInterface.createCurrentBackupTable(db)

#DepotInterface.updateDepotPath(db, 8, "F:\\deletedFiles")
