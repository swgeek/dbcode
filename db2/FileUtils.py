#
# Yes, lousy name. Will have to come up with a better bucket.
#
# misc file utilities, copy, move, etc.
#
import os
import shutil
import miscQueries
import ctypes


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


def windowsSpecificGetFreeSpace(drive):
	freeSpace = ctypes.c_ulonglong(0)
	ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(drive), None, None, ctypes.pointer(freeSpace))
	return freeSpace.value


def getFreeSpaceOnDepotDrive(depotRoot):
	# get size on disk of depots if possible, but for now just get free space
	drive, path = os.path.splitdrive(depotRoot)
	space = windowsSpecificGetFreeSpace(drive)
	return space


# nothing to do with database, simply copies depot from one drive to another.
# this will only work for depots, as make assumption that only directories at top level
# and only files at bottom level. May generalize, may not.
# assumes enough space available
def copyDepot(sourceDepotRoot, destinationDepotRoot):

	if not os.path.exists(sourceDepotRoot):
		raise Exception("%s does not exist" % sourceDepotRoot)

	if not os.path.exists(destinationDepotRoot):
		raise Exception("%s does not exist" % destinationDepotRoot)

	for dirName in os.listdir(sourceDepotRoot):
		print "copying %s" % dirName
		sourceDirPath = os.path.join(sourceDepotRoot, dirName)
		destDirPath = os.path.join(destinationDepotRoot, dirName)

		#if not os.path.exists(destDirPath):
		#	os.mkdir(destDirPath)
		shutil.copytree(sourceDirPath, destDirPath)



# similar to copyFileFromDepot, but moves instead of copying. 
# useful for moving corrupted files out of the depot.
def MoveFileFromDepot(db, depotRootPath, destinationDirPath, filehash, newFilename):

		subdir = filehash[0:2]
		sourcePath = os.path.join(depotRootPath, subdir, filehash)
		destinationPath = os.path.join(destinationDirPath, newFilename)

		if not os.path.isfile(sourcePath):
			return False

		print "moving %s to %s" % (sourcePath, destinationPath)

		shutil.move(sourcePath, destinationPath)

		return True


def getPathOfFileInDepot(depotRootPath, filehash):
		subdir = filehash[0:2].upper()
		filepath = os.path.join(depotRootPath, subdir, filehash)

		if os.path.isfile(filepath):
			return filepath
		else:
			return None


def getListOfAllFilesInDir(rootDirPath):
	allfiles = []

	for dirpath, subDirList, subdirFiles in os.walk(rootDirPath):
		for filename in subdirFiles:
			allfiles.append((dirpath, filename))

	return allfiles
