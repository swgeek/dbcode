#
# Yes, lousy name. Will have to come up with a better bucket.
#
# misc file utilities, copy, move, etc.
#
import os
import shutil
import miscQueries


def CopyFileFromDepot(db, depotRootPath, destinationDirPath, filehash, newFilename):

		subdir = filehash[0:2]
		sourcePath = os.path.join(depotRootPath, subdir, filehash)
		destinationPath = os.path.join(destinationDirPath, newFilename)

		if not os.path.isfile(sourcePath):
			return False

		print "copying %s to %s" % (sourcePath, destinationPath)

		shutil.copyfile(sourcePath, destinationPath)

		return True



def extractAllFilesFromDirectory(db, dirHash, destinationDir):
	fileInfo = miscQueries.getAllFilesFromDir(db, dirHash)

	# hardcoding depot root dirs for now, but should get these from database
	depotRoot = "I:\\objectstore1"
	depotRoot2 = "H:\\objectstore2"

	for line in fileInfo:
		filehash = line[0]
		filename = line[1]
		if not CopyFileFromDepot(db, depotRoot, destinationDir, filehash, filename):
			if not CopyFileFromDepot(db, depotRoot2, destinationDir, filehash, filename):
				print "ERROR, file not found"
				exit(1)

