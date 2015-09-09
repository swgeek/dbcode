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


def CopyFileIntoDepot(depotRootPath, sourceFilePath, filehash, logger):

		subdir = filehash[0:2]
		destinationDirPath = os.path.join(depotRootPath, subdir)
		destinationFilePath = os.path.join(depotRootPath, subdir, filehash)

		if not os.path.isdir(destinationDirPath):
			os.mkdir(destinationDirPath)

		if os.path.isfile(destinationFilePath):
			filesize = os.path.getsize(destinationFilePath)
			sourceFilesize = os.path.getsize(sourceFilePath)

			if filesize != sourceFilesize:
				logger.log( "ERROR: filesizes do not match for %s and %s" % (sourceFilePath, filehash) )
				print "ERROR: filesizes do not match for %s and %s" % (sourceFilePath, filehash) 
				exit(1)
			else:
				logger.log("did not copy %s, %s already exists" % (sourceFilePath, filehash))

		else:
			logger.log("copying %s to %s" % (sourceFilePath, destinationFilePath))
			shutil.copyfile(sourceFilePath, destinationFilePath)


# only does disk stuff, assumes db data handled elsewhere
def DeleteFileFromDepot(depotRootPath, filehash):
		subdir = filehash[0:2]
		filepath = os.path.join(depotRootPath, subdir, filehash)

		if not os.path.isfile(filepath):
			return False

		os.remove(filepath)
		return True
