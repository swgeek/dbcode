# checks a depot for corrupt files: i.e. checks hash of every file is same is filename
# moves files to specified dir if corrupt. Does not update database!

import os
import shutil

import FileUtils
import DbLogger
import Sha1HashUtilities


logger = DbLogger.dbLogger()

sourceDepotRoot = "I:\\objectstore1e"
corruptFileDir = "I:\\corruptFiles"

filecount = 0
corruptCount = 0
dircount = 0

for dirname in os.listdir(sourceDepotRoot):
	dirpath = os.path.join(sourceDepotRoot, dirname)
	if os.path.isdir(dirpath):

		logger.log("checking %d: %s" % (dircount, dirpath))
		dircount += 1
		filelist = os.listdir(dirpath)
		for filename in filelist:
			filepath = os.path.join(dirpath, filename)
			if os.path.isfile(filepath):
				#logger.log("checking %s" % filepath)
				filecount += 1
				filehash = Sha1HashUtilities.HashFile(filepath)
				filehash = filehash.upper()
				if filehash != filename: 
					logger.log("corrupt file: %s" % filepath)
					destinationPath = os.path.join(corruptFileDir, filename)
					logger.log("moving %s to %s" % (filepath, destinationPath))
					shutil.move(filepath, destinationPath)
					corruptCount += 1


logger.log("number of files: %d" % filecount)
logger.log("corrupt files: %d" % corruptCount)




# get list of dirs
#for dir in sourceDepotRoot:
	# get list of FileUtils
	# for file in FileUtils:
		# hash file
		# if filehsah != filename:
			# move file to corrupt dir