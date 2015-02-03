import DbHelper
import DbInterface
import os.path
import SHA1HashUtilities

if os.path.isfile("trythis.sqlite"):
	os.remove("trythis.sqlite")

DbHelper.DbHelper.CreateDb("trythis.sqlite")

db = DbHelper.DbHelper("trythis.sqlite")
db.OpenConnection()
db.AddFile("0a4d55a8d778e5022fab701977c5d840bbc486d0", 1045)
db.AddFile("0000000000001111111111111111111111111110", -1)

dbInterface = DbInterface.DbInterface("trythis.sqlite")
query = "select * from FilesV2;"
print dbInterface.ExecuteSqlQueryReturningMultipleRows(query)
print ""

print "check if file 0a4d55a8d778e5022fab701977c5d840bbc486d0 found: should get True"
print db.FileAlreadyInDatabase("0a4d55a8d778e5022fab701977c5d840bbc486d0", 1045)
print ""

print "check if file 00001111 found: should get False"
print db.FileAlreadyInDatabase("00001111", 1045)
print ""

print "check if file 0a4d55a8d778e5022fab701977c5d840bbc486d0 with wrong filesize found: should get exception"
try:
	print db.FileAlreadyInDatabase("0a4d55a8d778e5022fab701977c5d840bbc486d0", 1046)
except:
	print "correctly caught exception"
print ""

print "check if file 0000000000001111111111111111111111111110 found and size updated, should get true, the contents should have updated size"
print db.FileAlreadyInDatabase("0000000000001111111111111111111111111110", 1001000)
query = "select * from FilesV2;"
print dbInterface.ExecuteSqlQueryReturningMultipleRows(query)
print ""

print "check if file 0a4d55a8d778e5022fab701977c5d840bbc486d0 with unknown filesize found: should get True"
print db.FileAlreadyInDatabase("0a4d55a8d778e5022fab701977c5d840bbc486d0", -1)
print ""

print "check if file 0a4d55a8d778e5022fab701977c5d840bbc486d0 with unknown filesize found (method 2): should get True"
print db.FileAlreadyInDatabaseUnknownSize("0a4d55a8d778e5022fab701977c5d840bbc486d0")
print ""

print "check if file 00001111 with unknown size found: should get False"
print db.FileAlreadyInDatabaseUnknownSize("00001111")
print ""

print "insert new directories"
dirpath = "C:\\trythisone"
dirHash = SHA1HashUtilities.SHA1HashUtilities.HashString(dirpath)
db.addDirectory(dirHash,dirpath)
dirpath = "C:\\fakepath\\trythistwo"
dirHash = SHA1HashUtilities.SHA1HashUtilities.HashString(dirpath)
db.addDirectory(dirHash,dirpath)
query = "select * from originalDirectoriesV2;"
print dbInterface.ExecuteSqlQueryReturningMultipleRows(query)
print ""

print "check if directory hash already in database, should return true"
print db.DirectoryAlreadyInDatabase(dirHash)
print "check if directory hash 555555 already in database, should return false"
print db.DirectoryAlreadyInDatabase("555555")
print ""

print "insert file locations"
filepath = "C:\\trythisone\\filename1.jpg"
filehash = "111111"
db.AddFile("111111", 10400)
db.AddOriginalFileLocation(filehash, filepath)
filepath = "C:\\anotherfakepath\\trythisthree\\filename2.jpg"
filehash = "111111"
db.AddOriginalFileLocation(filehash, filepath)
query = "select * from OriginalDirectoriesForFileV5;"
print dbInterface.ExecuteSqlQueryReturningMultipleRows(query)
print ""

print "check if file location already in database, should return true"
parentDir, filename = os.path.split(filepath)
dirHash = SHA1HashUtilities.SHA1HashUtilities.HashString(parentDir)
print db.FileOriginalLocationAlreadyInDatabase(filehash, filename, dirHash)
print "check if incorrect file location in database, should return false"
print db.FileOriginalLocationAlreadyInDatabase(filehash, "differentName", dirHash)
print "check if incorrect file location in database, should return false"
print db.FileOriginalLocationAlreadyInDatabase(filehash, filename, "4444")
print "check if incorrect file location in database, should return false"
print db.FileOriginalLocationAlreadyInDatabase("454545", filename, dirHash)
print ""

print "testing link stuff"
print "adding files with hash 001 and 002 for test"
db.AddFile("001", 1000)
db.AddFile("002", -1)
print "replace 001 with link 003"
db.AddFile("003", 200)
db.SetLink("001", "003")
db.SetFileToReplacedByLink("001")
print "list contents of link table"
query = "select * from FileLink;"
print dbInterface.ExecuteSqlQueryReturningMultipleRows(query)
print ""
print "list values for 001"
query = "select * from FilesV2 where filehash = \"%s\";" % "001"
print dbInterface.ExecuteSqlQueryReturningMultipleRows(query)
print ""

print "testing RemoveFileCompletely"
print "removing 111111"
db.RemoveFileCompletely("111111")
print "OriginalDirectoriesForFileV5 table:"
query = "select * from OriginalDirectoriesForFileV5;"
print dbInterface.ExecuteSqlQueryReturningMultipleRows(query)
print ""
print "FilesV2 table:"
query = "select * from FilesV2;"
print dbInterface.ExecuteSqlQueryReturningMultipleRows(query)
print ""
print "removing 001"
db.RemoveFileCompletely("001")
print "OriginalDirectoriesForFileV5 table:"
query = "select * from OriginalDirectoriesForFileV5;"
print dbInterface.ExecuteSqlQueryReturningMultipleRows(query)
print ""
print "FilesV2 table:"
query = "select * from FilesV2;"
print dbInterface.ExecuteSqlQueryReturningMultipleRows(query)
print ""
print "FileLink table:"
query = "select * from FileLink;"
print dbInterface.ExecuteSqlQueryReturningMultipleRows(query)
print ""


print "testing AddDirSubdirMapping"
db.AddDirSubdirMapping("parentDirHash", "subdirHash")
db.AddDirSubdirMapping("parentDirHash2", "subdirHash2")
print "originalDirToSubdir table:"
query = "select * from originalDirToSubdir;"
print dbInterface.ExecuteSqlQueryReturningMultipleRows(query)
print ""

print "testing AddDirSubdirMapping"
print "check if subdirHash is subdir of parentDirHash - should return true"
print db.DirSubdirMappingExists("parentDirHash", "subdirHash")
print "check if subdirHash2 is subdir of parentDirHash - should return false"
print db.DirSubdirMappingExists("parentDirHash", "subdirHash2")
print ""


print "test GetListOfSubdirectoriesInOriginalDirectory"
dirpath = "C:\\anotherfakepath\\part2"
db.addDirectory("subdirHash",dirpath)
print db.GetListOfSubdirectoriesInOriginalDirectory("parentDirHash")
print ""



print "testing GetLargestFilesTodo"
print "current two largest files:"
print db.GetLargestFilesTodo(2)
print "adding new largest with hash 111"
db.AddFile("111", 1000000000000)
print "new two largest files"
print db.GetLargestFilesTodo(2)


print "testing setDirectoryStatus"
print "directory table before:"
query = "select * from originalDirectoriesV2;"
print dbInterface.ExecuteSqlQueryReturningMultipleRows(query)
print ""
print "setting status of %s to deleted" % dirHash
db.setDirectoryStatus(dirHash, "Deleted")
print "directory table after:"
query = "select * from originalDirectoriesV2;"
print dbInterface.ExecuteSqlQueryReturningMultipleRows(query)
print ""


print "testing setFileStatus"
print "FilesV2 table before:"
query = "select * from FilesV2;"
print dbInterface.ExecuteSqlQueryReturningMultipleRows(query)
print ""
print "setting status of %s to deleted" % "111"
db.setFileStatus("111", "Deleted")
print "FilesV2 table after:"
query = "select * from FilesV2;"
print dbInterface.ExecuteSqlQueryReturningMultipleRows(query)
print ""

# TODO: add negative implementation and test (what if hash not in db, we do not handle that at all)
print "testing getFileStatus"
print "getting status of 111, should be Deleted"
print db.getFileStatus("111")
print "getting status of 002, should be todo"
print db.getFileStatus("002")
print ""

print "buildCommandStringPart1"
print "calling with everything set to true"
print db.buildCommandStringPart1(2, True, True, True, True)
print "calling with includeTodo set to true"
print db.buildCommandStringPart1(2, True, False, False, False)
print "calling with includeTodoLater set to true"
print db.buildCommandStringPart1(2, False, True, False, False)
print "calling with includeToDelete set to true"
print db.buildCommandStringPart1(2, False, False, True, False)
print "calling with includeDeleted set to true"
print db.buildCommandStringPart1(2, False, False, False, True)
print "calling with includeTodo and includeToDelete set to true"
print db.buildCommandStringPart1(2, True, False, True, False)
print ""

print "testing GetLargestFiles"
print "putting 40 files in, hash values from 11 to 50, sizes from 1000011 to 1000050"
print "set 45 to todelete"
for i in range(11, 50):
	hash = "%d" % i
	size = 1000000 + i
	db.AddFile(hash, size)
	# add this for later tests
	filepath = "C:\\trythisone\\%s.mov" % hash
	db.AddOriginalFileLocation(hash, filepath)
db.setFileStatus("45", "todelete")

print "get largest, not including todelete:"
print db.Get30LargestFiles(10, True, True, False, True)
print ""

print "test JoinStringIfNeeded"
print "should return partial query string"
print db.JoinStringIfNeeded("a", None)
print "should return empty string"
print db.JoinStringIfNeeded(None, None)
print ""



print "test BuildExtensionSubQuery"
print "should be (1)"
print db.BuildExtensionSubQuery(None)
print "should be: extension COLLATE NOCASE IN (a, b, c)"
print db.BuildExtensionSubQuery("a, b, c")
print ""


print "test BuildSearchSubQuery"
print "should be (1)"
print db.BuildSearchSubQuery(None)
print "should be: filename like \'%.jpeg%\'"
print db.BuildSearchSubQuery(".jpeg")
print ""

print "test BuildStatusSubQuery"
print "should be (1)"
print db.BuildStatusSubQuery(None)
print "should be: status COLLATE NOCASE IN (deleted, todo)"
print db.BuildStatusSubQuery("deleted, todo")
print ""


print "test BuildFileLimitSubString"
print db.BuildFileLimitSubString(5)
print ""

#GetLargestFiles(self, numOfFiles, statusList, extensionList, searchTerm):

print "test GetLargestFiles"
print "using (4, None, None, None)"
print db.GetLargestFiles(4, None, None, None)
print ""
print "using (2, \"\"todo\"\", None, None)"
print db.GetLargestFiles(2, "\"todo\"", None, None)
print ""
print "using (5, None, \"\".mov\",\".jpg\"\", None)"
print db.GetLargestFiles(5, None, "\".mov\",\".jpg\"", None)
print ""
print "using (5, \"\"todo\"\", \"\".mov\",\".jpg\"\", \"\"5\"\")"
print db.GetLargestFiles(5, "\"todo\"", "\".mov\",\".jpg\"", "5")
print ""


print "test GetObjectStoreId"
print "negative test, should return none"
print db.GetObjectStoreId("\trythis\doesNotExst")
print "inserting c:\\test1\\test2"
newObjectStoreId = db.CheckObjectStoreExistsAndInsertIfNot("c:\\test1\\test2")
print "got newId %d, now get id and check is same" % newObjectStoreId	
print db.GetObjectStoreId("c:\\test1\\test2")
print ""


print "test GetUndeletedFilesWithOnlyOneLocation and doInsertForAddFreshLocation"
print "first give some of our test files (21 to 45) location of objectstore just created"
for i in range(21, 46):
	filehash = "%d" % i
	db.doInsertForAddFreshLocation(filehash, newObjectStoreId)
print "now look for 5 undeleted files with one location, should get 21 to 25 "
print db.GetUndeletedFilesWithOnlyOneLocation(5)
print ""

print "test GetListOfFilesToDelete"
print db.GetListOfFilesToDelete()
print ""


print "test GetListOfFilesToDeleteWithLocation"
print db.GetListOfFilesToDeleteWithLocation(1)
print ""


print "test GetListOfFilesToTryUnDelete"
db.setFileStatus("45", "tryToUndelete")
print db.GetListOfFilesToTryUnDelete()
print ""


print "test GetFilesFromObjectStore"
print db.GetFilesFromObjectStore(1)
print ""


print "test GetObjectStores"
db.CheckObjectStoreExistsAndInsertIfNot("c:\\test3\\test4")
print db.GetObjectStores()
print ""


print "test GetRootDirectories and AddOriginalRootDirectoryIfNotInDb"
print "no root dirs initially: should get empty list here"
print db.GetRootDirectories()
print "adding root dirs \"C:\\root1\" and \"C:\\root2\" the querying again"
db.AddOriginalRootDirectoryIfNotInDb("C:\\root1")
db.AddOriginalRootDirectoryIfNotInDb("C:\\root2")
db.AddOriginalRootDirectoryIfNotInDb("C:\\root1")
print db.GetRootDirectories()
print ""


print "test numOfFilesWithOnlyOneLocation"
print db.numOfFilesWithOnlyOneLocation(1, "objectStore2", "objectStore1", "objectStore3")
print db.numOfFilesWithOnlyOneLocation(1, "objectStore1", "objectStore2", "objectStore3")
print ""


print "test DeleteObjectStore"
print "try deleting from 1 - cannot as has files, will return false"
print db.DeleteObjectStore(1)
print "delete 2, will return true"
print db.DeleteObjectStore(2)
print "listing object stores"
command = "select * from  objectStores" 
print dbInterface.ExecuteSqlQueryReturningMultipleRows(command)
print ""



print "test UpdateObjectStore"
db.UpdateObjectStore(1, "C:\\newpath\\newsubdir")
command = "select * from  objectStores" 
print dbInterface.ExecuteSqlQueryReturningMultipleRows(command)
print ""


print "test getFirstFilenameForFile"
print db.getFirstFilenameForFile("12")
print ""


print "test GetOriginalDirectoriesForFile"
print db.GetOriginalDirectoriesForFile("12")
print ""


print "test GetOriginalDirectoryWithPath"
print db.GetOriginalDirectoryWithPath("C:\\trythisone")
print ""

dirHash = db.GetOriginalDirectoryWithPath("C:\\trythisone")[0][1]

print "test GetDirectoryPathForDirHash"
print db.GetDirectoryPathForDirHash(dirHash)
print ""


print "test UpdateStatusForDirectoryAndContents"
print "set to delete"
db.UpdateStatusForDirectoryAndContents(dirHash, "todelete")
print db.GetOriginalDirectoryWithPath("C:\\trythisone")
print ""



print "test GetListOfFilesInOriginalDirectory"
print db.GetListOfFilesInOriginalDirectory(dirHash)
print ""


print "test UpdateFileAndSubDirListForDir and GetFileListForDirectory"
db.UpdateFileAndSubDirListForDir(dirHash)
print db.GetFileListForDirectory(dirHash)
print ""



print "test GetStatusOfDirectory"
print db.GetStatusOfDirectory(dirHash)
print ""


print "test GetStatusOfFile"
print db.GetStatusOfFile("41")
print ""


print "test GetSubdirectories (use UpdateFileAndSubDirListForDir first)"
db.AddDirSubdirMapping(dirHash, "subdirHash")
db.AddDirSubdirMapping(dirHash, "subdirHash2")
db.UpdateFileAndSubDirListForDir(dirHash)
print db.GetSubdirectories(dirHash)
print ""


print "test doInsertForAddFreshLocation"
command = "select * from  fileLocations" 
print dbInterface.ExecuteSqlQueryReturningMultipleRows(command)
print ""
print db.doInsertForAddFreshLocation("555", 1)
print ""
print "listing fileLocations"
command = "select * from  fileLocations" 
print dbInterface.ExecuteSqlQueryReturningMultipleRows(command)
print ""




print "test InsertAdditionalFileLocation"
command = "select * from  fileLocations" 
print dbInterface.ExecuteSqlQueryReturningMultipleRows(command)
print ""
print db.InsertAdditionalFileLocation("555", 7)
print ""
print "listing fileLocations"
command = "select * from  fileLocations" 
print dbInterface.ExecuteSqlQueryReturningMultipleRows(command)
print ""


print "test ReplaceFileLocation"
print db.ReplaceFileLocation("555", 1, 8)
print ""
print "listing fileLocations"
command = "select * from  fileLocations" 
print dbInterface.ExecuteSqlQueryReturningMultipleRows(command)
print ""


 
print "test GetFileLocations"
print db.GetFileLocations("555")
print db.GetFileLocations("afdadfdfa")
print ""


print "test AddFileLocation"
print "first try to add existing location"
db.AddFileLocation("555", 7)
print db.GetFileLocations("555")
print ""
print "add new location"
db.AddFileLocation("555", 9)
print db.GetFileLocations("555")
print "try to add too many"
try:
	db.AddFileLocation("555", 15)
except:
	print "got an exception"
print "add location for file without location"
db.AddFileLocation("444", 2)
print db.GetFileLocations("444")
print ""


print "test addFileLocation2"
print "first try to add to existing location"
db.addFileLocation2("1111", "C:\\newpath\\newsubdir")
print db.GetFileLocations("1111")
print ""
print "add new location"
db.addFileLocation2("1111", "C:\\anotherpath\\newsubdir")
print db.GetFileLocations("1111")
print ""


print "test GetObjectStorePathsForFile"
print db.GetObjectStorePathsForFile("adfag")
print db.GetObjectStorePathsForFile("1111")
print ""


print "test SetToDelete"
print db.GetStatusOfFile("41")
db.SetToDelete("41")
print db.GetStatusOfFile("41")
print ""


print "test SetToRemoveCompletely"
print db.GetStatusOfFile("41")
db.SetToRemoveCompletely("41")
print db.GetStatusOfFile("41")
print ""

print "test SetToLater"
db.SetToLater("41")
print db.GetStatusOfFile("41")
print ""




print "test SetNewStatusIfNotDeleted"
print db.GetStatusOfFile("41")
db.SetNewStatusIfNotDeleted("41", "deleted")
print db.GetStatusOfFile("41")
db.SetNewStatusIfNotDeleted("41", "badStatus")
print db.GetStatusOfFile("41")
print ""

print "test GetListOfFilesWithExtensionInOneObjectStore"
print db.GetListOfFilesWithExtensionInOneObjectStore(".txt", "C:\\newpath\\newsubdir")
db.AddFileLocation("11", newObjectStoreId)
db.AddFileLocation("12", newObjectStoreId)
db.AddFileLocation("13", newObjectStoreId)
db.AddOriginalFileLocation("13", "C:\\tt.jpg")
print db.GetListOfFilesWithExtensionInOneObjectStore(".jpg", "C:\\newpath\\newsubdir")
print db.GetListOfFilesWithExtensionInOneObjectStore(".mov", "C:\\newpath\\newsubdir")
print ""



print "test MoveFileLocation"
command = "select * from  fileLocations where filehash = 11" 
print dbInterface.ExecuteSqlQueryReturningMultipleRows(command)
db.MoveFileLocation("11", "C:\\newpath\\newsubdir", "C:\\newObjectStore")
command = "select * from  fileLocations where filehash = 11" 
print dbInterface.ExecuteSqlQueryReturningMultipleRows(command)
print ""


print "test GetNumberOfFiles"
print db.GetNumberOfFiles()
print ""




print "test GetNumberOfOriginalVersionsOfFiles"
print db.GetNumberOfOriginalVersionsOfFiles()
print ""



print "test GetNumberOfDirectories"
print db.GetNumberOfDirectories()
print ""


print "test GetListOfExtensions"
print db.GetListOfExtensions(True)
print ""
print db.GetListOfExtensions(False)
print ""



print "test GetDirPathHashListForDirectoriesWithStatus"
print db.GetDirPathHashListForDirectoriesWithStatus("todelete")
print ""
print db.GetDirPathHashListForDirectoriesWithStatus("Deleted")
print ""


db.CloseConnection()

