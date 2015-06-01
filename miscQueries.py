# queries I use to sort, e.g. get largest files

import DbInterface
import DepotInterface
import os
import unicodedata

databaseFilePathName = "C:\\depotListing\\listingDb.sqlite"
db = DbInterface.DbInterface(databaseFilePathName)

#reader = DepotInterface.getReaderForFilesWithFilenameStartingWithDotUnderscore(db)


DepotInterface.createLargestFilesTable(db)
DepotInterface.cacheLargestFilesInfo(db)
#DepotInterface.createToDeleteTable(db)
#DepotInterface.createToKeepForNowTable(db)

# think I screwed up, look at all "todelete" files again. Some need to go into notfound
# easiest way: delete all entries from todelete, start over
#DepotInterface.dropTable(db, "largestFiles")
#DepotInterface.createLargestFilesTable(db)
#DepotInterface.cacheLargestFilesInfo(db)


# have stuff in "tokeepfornow, handle that"


def handleToDelete(filehash, filesize, filenames, directories):
	#print "%s %d %s %s" % (filehash, filesize, filenames, directories)
	#print DepotInterface.getToDeleteFileInfo(db, filehash)
	print "todelete!"
	DepotInterface.addToDeleteEntry(db, filehash, filesize, filenames, directories)
	DepotInterface.deleteFileInfoFromTodoTables(db, filehash)
	DepotInterface.removeFromLargestFilesCache(db, filehash)
	print "removed from regular tables"
	print "info from todelete table"
	print DepotInterface.getToDeleteFileInfo(db, filehash)

def handleNotFound(filehash, filesize, filenames, directories):
	print "not found"
	DepotInterface.addNotFoundEntry(db, filehash, filesize, filenames, directories)
	DepotInterface.deleteFileInfoFromTodoTables(db, filehash)
	DepotInterface.removeFromLargestFilesCache(db, filehash)
	print "removed from regular tables"
	print "info from NotFound table"
	print DepotInterface.getNotFoundFileInfo(db, filehash)

def extractAllFilesDirAsFileUsingHashnames(dirhash, maxNum=None):
	dirpath = DepotInterface.getDirectoryPath(db, dirhash)
	print "extracting from %s" % dirpath
	print "get files from directory %s" % dirpath
	results =  DepotInterface.getFilesInDirectory(db, dirhash)
	print "extracting %d files" % len(results)
	count = 0
	for row in results:
		fileToExtract = row[0]
		print "hashfile is %s " % fileToExtract
		filename = row[1]
		filename = unicodedata.normalize('NFKD', filename).encode('ascii','ignore')
		print "extracting:"
		print filename
		extension = os.path.splitext(filename)[1]
		destinationDir = "E:\\tempExtract"
		newFilename = fileToExtract + extension
		path1 = os.path.join(destinationDir, "keep", newFilename)
		path2 = os.path.join(destinationDir, "delete", newFilename)
		path3 = os.path.join(destinationDir, newFilename)
		if os.path.isfile(path1) or os.path.isfile(path2) or os.path.isfile(path3):
			print "already extracted"
			continue

		if DepotInterface.extractFile(db, fileToExtract, destinationDir, newFilename):
			count += 1
		else:
			print "not found"
		if (not maxNum is None) and (count > maxNum):
			break



def setHashnamedFilesFromDirToDelete(dirname):
	everything = os.listdir(dirname)
	print everything
	for f in everything:
		filepath = os.path.join(dirname, f)
		if os.path.isfile(filepath):
			filehash = os.path.splitext(f)[0]
			print filehash
			info = DepotInterface.getAllFileInfo(db, filehash)
			filehash, filesize, status, filenames, directories, depotIds = info
			if filesize is None:
				# not found, maybe already deleted. 
				if DepotInterface.getToDeleteFileInfo(db, filehash) is None:
					print "*********** not found in deletes either!"
					exit(1)
			
			else:
				handleToDelete(filehash, filesize, filenames, directories)


#DepotInterface.updateDepotPath(db, 7, "E:\\deletedFiles")


#setHashnamedFilesFromDirToDelete("E:\\tempExtract\\delete")

depotInfo = DepotInterface.getDepotInfo(db)
for row in depotInfo:
	print row


reader = DepotInterface.getReaderForLargestFilesFromCache(db)
tlist = []
for i in range(1):
	result = reader.next()
	tlist.append(result)
reader.close()

for result in tlist:
	if result is None:
		break
	filehash, filesize, status, filenames, directories, depotIds = result
	print filehash
	print filenames
	dlist = directories.split()
	for d in dlist:
		print DepotInterface.getDirectoryPath(db, d)

	#not found
	if depotIds == "":
		handleNotFound(filehash, filesize, filenames, directories)

	#delete
	#elif filehash == "3CBAC3E29491DD9529F0B2C1DA9A98C69225ECF4":
		#handleToDelete(filehash, filesize, filenames, directories)

	#keep for now
	#elif filehash == "2AE5BC9220FD39AC4B66DC29D100321DBCEAEFF6":
		#print "ToKeepForNow!"
		#DepotInterface.addToKeepForNowEntry(db, filehash)
		#DepotInterface.removeFromLargestFilesCache(db, filehash)

	#extract everything from same directory
	#elif filehash == "":
		# for now, first directory only!
		#dirhash = dlist[0]
		#extractAllFilesDirAsFileUsingHashnames(dirhash)

	else:
		print "depotids:"
		print depotIds
		depotList = depotIds.split()
		for depotId in depotList:
			print DepotInterface.getDepotPath(db, int(depotId))


