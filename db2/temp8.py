
# copy regular directory (not depot) from one drive to another
# not doing subdirectories yet, will add that next time

# copy a dir and log files that could not be copied (corrupt drive etc)

import os
import shutil

import DbLogger

logger = DbLogger.dbLogger()

sourceDir = "H:\\books"
destinationDir = "E:\\books"

if not os.path.isdir(destinationDir):
	os.mkdir(destinationDir)

filelist = 	os.listdir(sourceDir)

for filename in filelist:
	sourcePath = os.path.join(sourceDir, filename)
	destPath = os.path.join(destinationDir, filename)
	try:
		shutil.copyfile(sourcePath, destPath)
	except:
		logger.log("cannot copy %s" % filename)
		