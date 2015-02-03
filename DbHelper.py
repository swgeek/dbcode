# translation of legacy code from c# to python. Will rearchitect at some point

#using MpvUtilities; Have to implement this part at some point

# interface to database. Abstract out so can swap databases without changing code elsewhere

import os
import DbInterface
import SHA1HashUtilities

FilesTable = "FilesV2"
OriginalDirectoriesForFileTable = "OriginalDirectoriesForFileV5"
OldOriginalDirectoriesForFileTable = "OriginalDirectoriesForFileV2"
OriginalDirectoriesTable = "originalDirectoriesV2"
OriginalRootDirectoriesTable = "originalRootDirectories"
ObjectStoresTable = "objectStores"
FileLocationsTable = "fileLocations"
FileListingForDirTable = "FileListingForDir"
SubdirListingForDirTable = "SubdirListingForDir"
FileLinkTable = "FileLink"

class DbHelper:

    def __init__(self, databaseFilePathName):
        #TODO: have to define DB interface. Maybe do that first, before this...
        self.db = DbInterface.DbInterface(databaseFilePathName)

        # TODO: check which of these we are using
        self.NumOfNewFiles = 0
        self.NumOfNewDirectoryMappings = 0
        self.NumOfDuplicateFiles = 0
        self.NumOfDuplicateDirectoryMappings = 0

        self.NumOfNewDirs = 0
        self.NumOfNewDirSubDirMappings  = 0
        self.NumOfDuplicateDirs = 0
        self.NumOfDuplicateDirSubDirMappings = 0
        
        self.NumOfNewFileLocations = 0
        self.NumOfDuplicateFileLocations = 0

        self.NumOfFilesWithStatusChange = 0
        self.NumOfFilesAlreadyDeleted = 0


    def OpenConnection(self):
        self.db.OpenConnection()


    def CloseConnection(self):
        self.db.CloseConnection()


    @staticmethod
    def CreateDb(dbFilePath):
        if os.path.isfile(dbFilePath):
            raise Exception("file %s already exists" % dbFilePath)

        DbInterface.DbInterface.CreateEmptyDbFile(dbFilePath)
        newDb = DbInterface.DbInterface(dbFilePath)
        DbHelper.CreateAllTables(newDb)


    # is schema the right word?
    @staticmethod
    def CreateTable(tableName, tableSchema, dbInterface):
        createTableCommand = "create table %s (%s);" % (tableName, tableSchema)
        dbInterface.ExecuteNonQuerySql(createTableCommand)

    @staticmethod
    def CreateAllTables(newDb):
        # ExecuteMultipleSqlStattementsWithRollback but does not rollback for table creation. 
        # maybe look into this
        # so decided to just do table by table instead
        newDb.OpenConnection()

        DbHelper.CreateTable(FileLinkTable, "filehash char(40) PRIMARY KEY, linkFileHash char(40)", newDb)
        DbHelper.CreateTable(FileListingForDirTable, "dirPathHash char(40) PRIMARY KEY, files varchar(64000)", newDb)
        DbHelper.CreateTable(FilesTable, "filehash char(40) PRIMARY KEY, filesize int, status varchar(60)", newDb)
        DbHelper.CreateTable(OriginalDirectoriesForFileTable, "filehash char(40), filename varchar(300), "
            + "dirPathHash char(40), extension varchar(30), PRIMARY KEY (filehash, filename, dirPathHash)", newDb)
        DbHelper.CreateTable(SubdirListingForDirTable, "dirPathHash char(40) PRIMARY KEY, subdirs varchar(64000)", newDb)
        DbHelper.CreateTable(FileLocationsTable, "filehash char(40) PRIMARY KEY, objectStore1 int, objectStore2 int, "
            + "objectStore3 int, FOREIGN KEY (objectStore1) REFERENCES objectStores(id), FOREIGN KEY (objectStore2) REFERENCES objectStores(id), "
            + "FOREIGN KEY (objectStore3) REFERENCES objectStores(id)", newDb)
        DbHelper.CreateTable(ObjectStoresTable, "id INTEGER PRIMARY KEY AUTOINCREMENT,  dirPath varchar(500)", newDb)
        DbHelper.CreateTable("originalDirToSubdir", "dirPathHash char(40), subdirPathHash char(40), PRIMARY KEY (dirPathHash, subdirPathHash)", newDb)
        DbHelper.CreateTable("originalDirectoriesV2", "dirPathHash char(40) PRIMARY KEY, dirPath varchar(500), status varchar(60)", newDb)
        DbHelper.CreateTable(OriginalRootDirectoriesTable, "rootdir varchar(500) PRIMARY KEY", newDb)

        newDb.CloseConnection()


    #temporary code to create new version of a particular table. Leave code here for now in case need to do something similar in the
    #future. NOT TESTED IN PYTHON VERSION
    def CreateNewTable(self):
        self.db.OpenConnection()

        createTableCommand = "create table FilesV2 (filehash char(40) PRIMARY KEY, filesize int, status varchar(60));"
        self.db.ExecuteNonQuerySql(createTableCommand)

        transferDataCommand = "insert into FilesV2 (filehash, filesize, status) select hash, filesize, status from Files;"
        self.db.ExecuteNonQuerySql(transferDataCommand)

        self.db.CloseConnection()


    #NOT TESTED IN PYTHON VERSION   
    def CreateNewMappingTablesV2(self):

        self.db.OpenConnection()

        createTableCommand ="create table %s (dirPathHash char(40) PRIMARY KEY, subdirs varchar(64000));" % SubdirListingForDirTable
        self.db.ExecuteNonQuerySql(createTableCommand)

        #transfer data
        transferCommand = "insert into %s (dirPathHash, subdirs) select dirPathHash, group_concat(subdirPathHash, ';') " \
            + " from %s group by dirPathHash;" % (SubdirListingForDirTable, "originalDirToSubdir")
        self.db.ExecuteNonQuerySql(transferCommand);

        self.db.CloseConnection();



    def UpdateFileAndSubDirListForDir(self, dirPathHash):
        # this inserts only, need to check if exists and update...
        transferCommand = "insert or replace into %s (dirPathHash, files) select dirPathHash, group_concat(filehash, ';') " % FileListingForDirTable \
            + " from %s where dirPathHash = \"%s\" group by dirPathHash;" % (OriginalDirectoriesForFileTable, dirPathHash)

        self.db.ExecuteNonQuerySql(transferCommand)

        transferCommand = "insert or replace into %s (dirPathHash, subdirs) select dirPathHash, group_concat(subdirPathHash, ';') " % SubdirListingForDirTable \
            + " from %s where dirPathHash = \"%s\" group by dirPathHash;" % ("originalDirToSubdir", dirPathHash)
        self.db.ExecuteNonQuerySql(transferCommand)


    def AddFile(self, hashvalue, filesize):
        commandString = "insert into %s (filehash, filesize, status) values (\"%s\", %d, \"todo\")" % (FilesTable, hashvalue, filesize)
        self.db.ExecuteNonQuerySql(commandString)
        self.NumOfNewFiles += 1


    def FileAlreadyInDatabase(self, hashValue, filesize):
        commandString = "select filesize from %s where filehash = \"%s\"" % (FilesTable, hashValue)
        results = self.db.ExecuteSqlQueryReturningMultipleRows(commandString)

        if len(results) == 0:
            return False   # not in database

        self.NumOfDuplicateFiles += 1

        if filesize == -1:
            return True    # in database, no point in checking if filesize matches as we don't know filesize

        filesizeFromDb = results[0][0]

        if filesizeFromDb != filesize:
            if filesizeFromDb == -1:
                # filesize previously unknown, update db
                commandString = "update %s set filesize = %d where filehash = \"%s\"; " % (FilesTable, filesize, hashValue)
                self.db.ExecuteNonQuerySql(commandString)
            else:
                # oops, "known" filesizes do not match, we have a problem
                raise Exception("filesizes do not match for filehash %s" % hashValue)

        return True


    def FileAlreadyInDatabaseUnknownSize(self, hashValue):
        commandString = "select * from %s where filehash = \"%s\"" % (FilesTable, hashValue)
        results = self.db.ExecuteSqlQueryReturningMultipleRows(commandString)

        if len(results) == 0:
            return False   # not in database
        else:
            return True


    def addDirectory(self, dirPathHash, dirPath):
        commandString = "insert into %s (dirPathHash, dirPath) values (\"%s\", \"%s\")" \
            % (OriginalDirectoriesTable, dirPathHash, dirPath)
        self.db.ExecuteNonQuerySql(commandString);
        self.NumOfNewDirs += 1


    def DirectoryAlreadyInDatabase(self, dirPathHash):
        commandString = "select * from %s where dirPathHash = \"%s\"" % (OriginalDirectoriesTable, dirPathHash)
        # should be able to get single row, look into and change
        results = self.db.ExecuteSqlQueryReturningMultipleRows(commandString)

        if len(results) == 0:
            return False   # not in database
        else:
            self.NumOfDuplicateDirs += 1
            return True


    def FileOriginalLocationAlreadyInDatabase(self, hashValue, filename, dirHash):
        commandString = "select * from %s where filehash = \"%s\" and filename = \"%s\" and dirPathHash = \"%s\";" \
            % (OriginalDirectoriesForFileTable, hashValue, filename, dirHash)
        # should be able to get single row, look into and change
        results = self.db.ExecuteSqlQueryReturningMultipleRows(commandString)

        if len(results) == 0:
            return False   # not in database
        else:
            self.NumOfDuplicateFileLocations += 1
            return True


    def AddOriginalFileLocation(self, hashValue, filePath):
        directory, filename = os.path.split(filePath)
        partialName, extension = os.path.splitext(filename)
        dirHash = SHA1HashUtilities.SHA1HashUtilities.HashString(directory)

        if not self.DirectoryAlreadyInDatabase(dirHash):
            self.addDirectory(dirHash, directory)

        if not self.FileOriginalLocationAlreadyInDatabase(hashValue, filename, dirHash):
            command = "insert into %s (filehash, filename, dirPathHash, extension) values (\"%s\", \"%s\", \"%s\", \"%s\");" \
                % (OriginalDirectoriesForFileTable, hashValue, filename, dirHash, extension)
            self.db.ExecuteNonQuerySql(command)
            self.NumOfNewFileLocations += 1


    def ClearCounts(self):
        # TODO: check which of these we are using
        self.NumOfNewFiles = 0
        self.NumOfNewDirectoryMappings = 0
        self.NumOfDuplicateFiles = 0
        self.NumOfDuplicateDirectoryMappings = 0

        self.NumOfNewDirs = 0
        self.NumOfNewDirSubDirMappings  = 0
        self.NumOfDuplicateDirs = 0
        self.NumOfDuplicateDirSubDirMappings = 0
        
        self.NumOfNewFileLocations = 0
        self.NumOfDuplicateFileLocations = 0

        self.NumOfFilesWithStatusChange = 0
        self.NumOfFilesAlreadyDeleted = 0


    def SetLink(self, filehash, linkFileHash):
        if not self.FileAlreadyInDatabaseUnknownSize(filehash):
            raise Exception( "%s not in database, trying to set link to %s" % (filehash, linkFileHash) )

        #Temporary! Should not just ignore if different link file, maybe prompt user, but will do for now...
        # string sqlCommand = String.Format("insert into {0} (filehash, linkFileHash) values (\"{1}\", \"{2}\");",
        command = "insert or ignore into %s (filehash, linkFileHash) values (\"%s\", \"%s\");" % \
             (FileLinkTable, filehash, linkFileHash)
        self.db.ExecuteNonQuerySql(command)

        command = "update %s set status = \'replacedByLink\' where filehash = \'%s\';" % (FilesTable, filehash)
        self.db.ExecuteNonQuerySql(command)


    def SetFileToReplacedByLink(self, filehash):
        command = "select count(filehash) from %s where filehash = \"%s\";" % (FileLinkTable, filehash)
        count = self.db.ExecuteSqlQueryReturningSingleInt(command)
        if count == 0:
            raise Exception("%s does not exist in links table, cannot set status to replace by link" % filehash)

        command = "update %s set status = \"replacedByLink\" where filehash = \"%s\"; " % (FilesTable, filehash)
        self.db.ExecuteNonQuerySql(command)


    # removes fileinfo from db completely, used when added in error, e.g. xml or link files added early on
    def RemoveFileCompletely(self, filehash):

        # have to remove from these tables currently: update code if this has changed
        # FilesV2, FileListingForDir, FileLink, OriginalDirectoresForFileV5, fileLocations
        # in future set this as a transaction or something so can roll back if any one part is interuppted.

        # 1) remove from FilesV2
        command = "delete from %s where filehash in (select filehash from %s where filehash = \"%s\");" % \
            (FilesTable, FilesTable, filehash)
        self.db.ExecuteNonQuerySql(command)

        # skip this for now, will remove dir from dir tables directly as will remove all files from that dir for now...
        # remove from FileListingForDir
        # get parent dir
        # remove self from filelisting from parent dir

        # remove from FileLink
        command = "delete from %s where filehash in (select filehash from %s where filehash = \"%s\");" % \
            (FileLinkTable, FileLinkTable, filehash)
        self.db.ExecuteNonQuerySql(command)

        command = "delete from %s where linkFileHash in (select linkFileHash from %s where linkFileHash = \"%s\");" % \
            (FileLinkTable, FileLinkTable, filehash)
        self.db.ExecuteNonQuerySql(command)

        # remove from OriginalDirectoriesForFileV5
        command = "delete from %s where filehash in (select filehash from %s where filehash = \"%s\");" % \
            (OriginalDirectoriesForFileTable, OriginalDirectoriesForFileTable, filehash)
        self.db.ExecuteNonQuerySql(command)

        # remove from filelocations
        command = "delete from %s where filehash in (select filehash from %s where filehash = \"%s\");" % \
            (FileLocationsTable, FileLocationsTable, filehash)
        self.db.ExecuteNonQuerySql(command)

        # note: may not cover all cases. E.g. if linked file is removed does not go back and update original file
        # also, if original file is removed the linkfile ref is deleted but the link file still exists, which may be what we want
 


    # what do the lines below mean? TODO
    #string dirToSubdirSqlString = "create table originalDirToSubdir (dirPathHash char(40), subdirPathHash char(40), PRIMARY KEY (dirPathHash, subdirPathHash))";
    #THIS IS THE OLD WAY! Need to update SubdirListingForDir!!

    def AddDirSubdirMapping(self, dirPathHash, subdirPathHash):
        command = "insert into originalDirToSubdir (dirPathHash, subdirPathHash) values (\"%s\", \"%s\")" % \
            (dirPathHash, subdirPathHash)
        self.db.ExecuteNonQuerySql(command)
        self.NumOfNewDirSubDirMappings += 1


    def GetListOfSubdirectoriesInOriginalDirectory(self, dirPathHash):
        command = "select dirPath, OriginalDirectoriesV2.dirPathHash, status  from originalDirToSubdir, OriginalDirectoriesV2 " + \
            "where originalDirToSubdir.dirPathHash = \"%s\" and originalDirToSubdir.subdirPathHash = OriginalDirectoriesV2.dirPathHash " % dirPathHash;
        return self.db.GetDatasetForSqlQuery(command)


    def DirSubdirMappingExists(self, dirPathHash, subdirPathHash):
        command = "select * from originalDirToSubdir where dirPathHash = \"%s\" and subdirPathHash = \"%s\"" % \
            (dirPathHash, subdirPathHash)
        results = self.db.ExecuteSqlQueryReturningMultipleRows(command)

        if len(results) == 0:
            return False   # not in database
        else:
            self.NumOfDuplicateDirSubDirMappings += 1
            return True


    def GetLargestFilesTodo(self, numOfFiles):
        command = "select filehash from %s where status = \"todo\" order by filesize desc limit %s" % (FilesTable, numOfFiles)
        return self.db.ExecuteSqlQueryReturningMultipleRows(command)


    def setDirectoryStatus(self, dirPathHash, newStatus):
        command = "update %s set status = \"%s\" where dirPathHash = \"%s\";" % (OriginalDirectoriesTable, newStatus, dirPathHash)
        self.db.ExecuteNonQuerySql(command)


    def setFileStatus(self, fileHash, newStatus):
        command = "update %s set status = \"%s\" where filehash = \"%s\";" % (FilesTable, newStatus, fileHash)
        self.db.ExecuteNonQuerySql(command)


    def getFileStatus(self, fileHash):
        command = "select status from %s where filehash = \"%s\";" % (FilesTable, fileHash)
        return self.db.ExecuteSqlQueryForSingleString(command)


    #maybe change this to accept a list or string of status values to look for, this is bad form
    # NOTE: numOfFiles is ignored for now, update at some point
    def buildCommandStringPart1(self, numOfFiles, includeTodo, includeTodoLater, includeToDelete, includeDeleted):
        commandString = None
        if includeTodo and includeTodoLater and includeToDelete and includeDeleted:
            # everything chosen, simplify the query
            commandString = "select filehash, status from %s" % FilesTable

        else:
            # using the "filehash not null" just to simplify code to add to the query, can always use "or" after this 
            commandString = "select filehash, status from %s where filehash is null" % FilesTable

            if includeTodo:
                commandString += " or status = \"todo\""

            if includeTodoLater:
                commandString += " or status = \"todoLater\""

            if includeToDelete:
                commandString += " or status = \"todelete\""              

            if includeDeleted:
                commandString += " or status = \"deleted\""

        return commandString


     # NOTE: numOfFiles is ignored for now, update at some point
    def Get30LargestFiles(self, numOfFiles, includeTodo, includeTodoLater, includeToDelete, includeDeleted):
        command = self.buildCommandStringPart1(numOfFiles, includeTodo, includeTodoLater, includeToDelete, includeDeleted)
        command += " order by filesize desc limit 30; "
        return self.db.ExecuteSqlQueryReturningMultipleRows(command)


    def JoinStringIfNeeded(self, extensionList, searchTerm):
        if extensionList is None and searchTerm is None:
            return ""
        else:
            return " join %s using (filehash) " % OriginalDirectoriesForFileTable


    def BuildExtensionSubQuery(self, extensionList):
        if extensionList is None:
            return "(1)"
        else:
            return "( extension COLLATE NOCASE IN (%s) )" % extensionList
 

    def BuildSearchSubQuery(self, searchTerm):
        # get list of all files with given extensions 
        if searchTerm is None:
            return "(1)"
        else:
            return " filename like \'%%%s%%\' " % searchTerm
 

    def BuildStatusSubQuery(self, statusList):
        if statusList is None:
            return "(1)"
        else:
            return "( status COLLATE NOCASE IN (%s) )" % statusList


    def BuildFileLimitSubString(self, numOfFiles):
        return " order by filesize desc limit %d" % numOfFiles
 


    def GetLargestFiles(self, numOfFiles, statusList, extensionList, searchTerm):
        commandPart1 ="select filehash, status from %s " % FilesTable

        sqlCommand = commandPart1 + self.JoinStringIfNeeded(extensionList, searchTerm) + " where " \
            + self.BuildExtensionSubQuery(extensionList) + " and " + self.BuildSearchSubQuery(searchTerm) + " and " \
            + self.BuildStatusSubQuery(statusList)

        if numOfFiles > 0:
                sqlCommand = sqlCommand + self.BuildFileLimitSubString(numOfFiles)

        sqlCommand = sqlCommand + ";"

        return self.db.GetDatasetForSqlQuery(sqlCommand)


    def GetObjectStoreId(self, objectStorePath):
        depotID = None
        commandString = "select id from objectStores where dirPath = \"%s\"" % objectStorePath
        reader = self.db.GetDataReaderForSqlQuery(commandString)
        storeID = reader.next()
        if not storeID is None:
            return storeID[0]
        return storeID

    def CheckObjectStoreExistsAndInsertIfNot(self, objectStorePath):
        objectStoreId = self.GetObjectStoreId(objectStorePath)
        if not objectStoreId is None:
            return objectStoreId
        insertCommandString = "insert into objectStores (dirPath) values (\"%s\")" % objectStorePath
        self.db.ExecuteNonQuerySql(insertCommandString)
        return self.GetObjectStoreId(objectStorePath)



    def GetUndeletedFilesWithOnlyOneLocation(self, numOfFiles):
        locationsCommand = "select filehash from (select filehash from (select filehash, count(*) as K from " + \
            " (select filehash, objectStore1 as o from %s where o is not null " %  FileLocationsTable + \
            " union select filehash, objectStore2 as o from %s where o is not null " %  FileLocationsTable + \
            " union select filehash, objectStore3 as o from %s where o is not null " %  FileLocationsTable + \
            " ) group by filehash) where K = 1) join %s using (filehash) " %  FilesTable + \
            "where status <> \"deleted\" and status <> \"todelete\" and status <> \"replacedByLink\" " + \
            "limit %d;" % numOfFiles
        print locationsCommand
        return self.db.ExecuteSqlQueryForStrings(locationsCommand)


    def doInsertForAddFreshLocation(self, filehash, objectStoreID):
        insertCommandString = "insert into fileLocations (filehash, objectStore1) values (\"%s\", %d)" % (filehash, objectStoreID)
        self.db.ExecuteNonQuerySql(insertCommandString)


    #should find a way to roll these queries into one routine, but for now...
    def GetListOfFilesToDelete(self):
        commandString = "select filehash from %s where status = \'todelete\';" % FilesTable
        return self.db.ExecuteSqlQueryForStrings(commandString)


    def GetListOfFilesToDeleteWithLocation(self, objectStoreID):
        commandString = "select filehash from %s join %s using (filehash) where " % (FilesTable, FileLocationsTable) + \
            " (status = \'todelete\' or status = \'replacedByLink\') " + \
            " and (objectStore1 = %d or objectStore2 = %d or objectStore3 = %d);" % (objectStoreID, objectStoreID, objectStoreID)

        return self.db.ExecuteSqlQueryForStrings(commandString)


    def GetListOfFilesToTryUnDelete(self):
        commandString = "select filehash from %s where status = \'tryToUndelete\';" % FilesTable
        return self.db.ExecuteSqlQueryForStrings(commandString)


    def GetFilesFromObjectStore(self, objectStoreID):
        command = "select filehash from %s where objectStore1 = %d or objectStore2 = %d or objectStore3 = %d;" % \
            (FileLocationsTable, objectStoreID, objectStoreID, objectStoreID)
        return self.db.GetDatasetForSqlQuery(command)


    def GetObjectStores(self):
        command = "select id, dirPath from %s;" % ObjectStoresTable
        return self.db.GetDatasetForSqlQuery(command)


    def AddOriginalRootDirectoryIfNotInDb(self, dirPath):
        command = "select count(*) from originalRootDirectories where rootdir = \"%s\"" % dirPath
        count = self.db.ExecuteSqlQueryReturningSingleInt(command)
        if count == 0:
            command = "insert into originalRootDirectories (rootdir) values (\"%s\")" % dirPath
            self.db.ExecuteNonQuerySql(command)


    def GetRootDirectories(self):
        command = "select rootdir from %s;" % OriginalRootDirectoriesTable
        return self.db.ExecuteSqlQueryForStrings(command)

    # todo: combine this with method above
    def numOfFilesWithOnlyOneLocation(self, objectStoreID, mainField, secondfield, thirdField):
        command = "select count(*) from %s where %s = %s and %s is null and %s is null;" % \
            (FileLocationsTable, mainField, objectStoreID, secondfield, thirdField)
        return self.db.ExecuteSqlQueryReturningSingleInt(command)



    def DeleteObjectStore(self, objectStoreID):
        # inefficient to do it in multiple queries. May fix later, get it working first.
        # also, instead of passing in table names should get from schema. Find out how.

        # ugh, this is ugly. Cannot remember why need these specific combinations  and not others, what about 3, 2, 1 etc.

        count = self.numOfFilesWithOnlyOneLocation(objectStoreID, "objectStore1", "objectStore2", "objectStore3")
       # have files where the only location is the specified one, cannot delete that location or will lose track of file
        if count != 0:
            return False

        count = self.numOfFilesWithOnlyOneLocation(objectStoreID, "objectStore2", "objectStore1", "objectStore3")
        if count != 0:
            return False

        count = self.numOfFilesWithOnlyOneLocation(objectStoreID, "objectStore3", "objectStore1", "objectStore2")
        if count != 0:
            return False

        # can delete

        # delete from 1
        command = "update %s set objectStore1 = null where objectStore1 = %d;" % (FileLocationsTable, objectStoreID)
        self.db.ExecuteNonQuerySql(command)

        # delete from 2
        command = "update %s set objectStore2 = null where objectStore2 = %d;" % (FileLocationsTable, objectStoreID)
        self.db.ExecuteNonQuerySql(command)
        
        # delete from 3
        command = "update %s set objectStore3 = null where objectStore3 = %d;" % (FileLocationsTable, objectStoreID)
        self.db.ExecuteNonQuerySql(command)
        
        # delete the object store from the object store table
        command = "delete from %s where id = %s" % (ObjectStoresTable, objectStoreID)
        self.db.ExecuteNonQuerySql(command)

        return True


    def UpdateObjectStore(self,  objectStoreId, newPath):
        command = "update %s set dirPath = \"%s\" where id = %d" % (ObjectStoresTable, newPath, objectStoreId)
        self.db.ExecuteNonQuerySql(command)


    def getFirstFilenameForFile(self, filehash):
        command = "select filename from %s where filehash = \"%s\" limit 1;" % (OriginalDirectoriesForFileTable, filehash)
        return self.db.ExecuteSqlQueryForSingleString(command)

    def GetOriginalDirectoriesForFile(self, fileHash):
        command = "select dirPath, dirPathHash, filename from %s join %s using (dirPathHash) where filehash = \"%s\" limit 100;" % \
            (OriginalDirectoriesForFileTable, OriginalDirectoriesTable, fileHash)
        return self.db.GetDatasetForSqlQuery(command)



    def GetOriginalDirectoryWithPath(self, dirPath):
        # was get dirPath, dirPathHash, null: check if there was a reason
        command = "select dirPath, dirPathHash, status from %s where dirPath = \"%s\";" % (OriginalDirectoriesTable, dirPath)
        return self.db.GetDatasetForSqlQuery(command)


    def GetDirectoryPathForDirHash(self, dirHash):
        command = "select dirPath from %s where dirPathHash = \"%s\";" % (OriginalDirectoriesTable, dirHash)
        return self.db.ExecuteSqlQueryForSingleString(command)


    def UpdateStatusForDirectoryAndContents(self, dirHash, newStatus):
        # just mark directory for now, update the contents later...
        command =  "update %s set status = \"%s\" where dirPathHash = \"%s\" " % (OriginalDirectoriesTable, newStatus, dirHash)
        if newStatus != "tryToUndelete":
            command += " and ( status <> \"deleted\" or status is null);"
        print command
        self.db.ExecuteNonQuerySql(command)


    def GetListOfFilesInOriginalDirectory(self, dirPathHash):
        command = "select filename, filehash, status from %s join %s using (filehash) where dirPathHash = \"%s\" ;" % \
            (OriginalDirectoriesForFileTable, FilesTable, dirPathHash)
        return self.db.GetDatasetForSqlQuery(command)

 
    def GetFileListForDirectory(self, dirPathHash):
        command = "select files from %s where dirPathHash = \'%s\';" % (FileListingForDirTable, dirPathHash)
        filelistString = self.db.ExecuteSqlQueryForSingleString(command)
        if filelistString is None:
            return None
        return filelistString.split(';')


    def GetStatusOfDirectory(self, dirPathHash):
        command = "select status from %s where dirPathHash = \"%s\";" % (OriginalDirectoriesTable, dirPathHash)
        return self.db.ExecuteSqlQueryForSingleString(command)


    def GetStatusOfFile(self, filehash):
        command = "select status from %s where filehash = \"%s\";" % (FilesTable, filehash)
        return self.db.ExecuteSqlQueryForSingleString(command)


    def GetSubdirectories(self, dirPathHash):
        command = "select subdirs from %s where dirPathHash = \'%s\';" % (SubdirListingForDirTable, dirPathHash)
        result = self.db.ExecuteSqlQueryForSingleString(command)
        if result == None or result == "":
            return ""
        else:
            return result.split(';')


    def doInsertForAddFreshLocation(self, filehash, objectStoreID):
        command = "insert into fileLocations (filehash, objectStore1) values (\"%s\", %d)" % (filehash, objectStoreID)
        self.db.ExecuteNonQuerySql(command)


    def InsertAdditionalFileLocation(self, filehash, objectStoreID):
        insertSqlString = None
        command = "select objectStore1, objectStore2, objectStore3 from fileLocations where filehash = \"%s\"" % filehash
        result = self.db.GetDatasetForSqlQuery(command)[0]
        if result[0] is None:
            command = "update fileLocations set objectStore1 = %d where filehash = \"%s\";" % (objectStoreID, filehash)
        elif result[1] is None:
            command = "update fileLocations set objectStore2 = %d where filehash = \"%s\";" % (objectStoreID, filehash)
        elif result[2] is None:
            command = "update fileLocations set objectStore3 = %d where filehash = \"%s\";" % (objectStoreID, filehash)
        else:
            raise Exception("non of the entries were null")

        self.db.ExecuteNonQuerySql(command)



    def ReplaceFileLocation(self, filehash, oldObjectStoreID, newObjectStoreId):
        command = "select objectStore1, objectStore2, objectStore3 from fileLocations where filehash = \"%s\"" % filehash
        result = self.db.GetDatasetForSqlQuery(command)[0]
        if result[0] == oldObjectStoreID:
            command = "update fileLocations set objectStore1 = %d where filehash = \"%s\";" % (newObjectStoreId, filehash)
        elif result[1] == oldObjectStoreID:
            command = "update fileLocations set objectStore2 = %d where filehash = \"%s\";" % (newObjectStoreId, filehash)
        elif result[2] == oldObjectStoreID:
            command = "update fileLocations set objectStore3 = %d where filehash = \"%s\";" % (newObjectStoreId, filehash)
        else:
            raise Exception("non of the entries matched")
        self.db.ExecuteNonQuerySql(command)




    def GetFileLocations(self, filehash):
        command =  "select objectStore1, objectStore2, objectStore3 from fileLocations where filehash = \"%s\"" % filehash
        result = self.db.GetDatasetForSqlQuery(command)
        if len(result) == 0:
            return None
        return [value for value in result[0] if value is not None]


    def AddFileLocation(self, filehash, objectStoreID):
        existingLocations = self.GetFileLocations(filehash)

        if existingLocations is None:
            # does not exist in table, insert
            self.doInsertForAddFreshLocation(filehash, objectStoreID)
            return

        if objectStoreID in existingLocations:
            # location already in there
            self.NumOfDuplicateFileLocations += 1
            return

        # todo, handle this better or allow more than 3 locations
        if len(existingLocations) == 3:
            raise Exception("already reached max number of locations, cannot add another")

        self.InsertAdditionalFileLocation(filehash, objectStoreID)


    # was addFileLocation in c# version, but do not have overload in python
    # note, this one is lowercase a, but added the 2 to differentiate furter
    def addFileLocation2(self, filename, objectStoreRoot):
        depotId = self.CheckObjectStoreExistsAndInsertIfNot(objectStoreRoot)
        self.AddFileLocation(filename, depotId)


    def GetObjectStorePathsForFile(self, filehash):
        locationList = self.GetFileLocations(filehash)
        if locationList is None:
            return None

        locationPaths = []
        for location in locationList:
            command = "select dirPath from %s where id = %s" % (ObjectStoresTable, location)
            objectStoreRootPath = self.db.ExecuteSqlQueryForSingleString(command)
            locationPaths.append(objectStoreRootPath)

        return locationPaths


    def SetToDelete(self, fileHash):
        self.setFileStatus(fileHash, "todelete")
        self.NumOfFilesWithStatusChange += 1


    def SetToRemoveCompletely(self, fileHash):
        self.setFileStatus(fileHash, "toRemoveCompletely")
        self.NumOfFilesWithStatusChange += 1


    def SetToLater(self, fileHash):
        self.setFileStatus(fileHash, "todoLater")


    def SetNewStatusIfNotDeleted(self, fileHash, newStatus):
        status = self.getFileStatus(fileHash)
        if status in ["deleted", "replacedbyLink" ]:
            self.NumOfFilesAlreadyDeleted += 1
        else:
            self.setFileStatus(fileHash, newStatus)
            self.NumOfFilesWithStatusChange += 1




    def GetListOfFilesWithExtensionInOneObjectStore(self, extension, objectStorePath):
        # get object store id
        #command = "select id from objectStores where dirPath = \"%s\";" % objectStorePath
        #objectStoreId = self.db.ExecuteSqlQueryReturningSingleInt(command)
        objectStoreId = self.GetObjectStoreId(objectStorePath)
        if objectStoreId is None:
            return None

        command = "select distinct filehash from %s join %s " % (OriginalDirectoriesForFileTable, "fileLocations") + \
                    "using (filehash) where extension = \"%s\" COLLATE NOCASE and " % extension + \
                    "(objectStore1 = %d or objectStore2 = %d or objectStore3 = %d);" % (objectStoreId, objectStoreId, objectStoreId)

        results = self.db.GetDatasetForSqlQuery(command)
        return results



    def MoveFileLocation(self, filehash, oldObjectStore, newObjectStore):
        newObjectStoreID = self.CheckObjectStoreExistsAndInsertIfNot(newObjectStore)
        oldObjectStoreID = self.GetObjectStoreId(oldObjectStore)
        if not oldObjectStoreID is None:
            self.ReplaceFileLocation(filehash, oldObjectStoreID, newObjectStoreID)


    def GetNumberOfFiles(self):
        command = "select count(*) from %s" % FilesTable
        return self.db.ExecuteSqlQueryReturningSingleInt(command)



    def GetNumberOfOriginalVersionsOfFiles(self):
        command = "select count(*) from %s" % OriginalDirectoriesForFileTable
        return self.db.ExecuteSqlQueryReturningSingleInt(command)


    def GetNumberOfDirectories(self):
        command = "select count(*) from %s" % OriginalDirectoriesTable
        return self.db.ExecuteSqlQueryReturningSingleInt(command)

 
    def GetListOfExtensions(self, spaceTakenForEachType):
        if spaceTakenForEachType:
            command = "select extension, count(*) as fileCount, sum(filesize) as totalSize from " + \
                        " (select distinct filehash, extension, filesize from %s join %s using (filehash)) " % (OriginalDirectoriesForFileTable, FilesTable) + \
                        " group by extension order by totalSize desc"
        else:
            command = "select extension, count(*) as fileCount from (select distinct filehash, extension " + \
                        "from %s) group by extension order by fileCount desc" % OriginalDirectoriesForFileTable
                   
        return self.db.GetDatasetForSqlQuery(command)

 
 
    def GetDirPathHashListForDirectoriesWithStatus(self, status):
        command = "select dirPathHash from %s where status = \"%s\";" % (OriginalDirectoriesTable, status)
        return self.db.ExecuteSqlQueryForStrings(command)


