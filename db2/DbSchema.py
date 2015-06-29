
objectStoresTable = "objectStores"
objectStoresSchema = "depotId INTEGER PRIMARY KEY AUTOINCREMENT, description varchar(500), path varchar(500)"

OriginalDirectoryForFileTable = "originalDirectoryForFile"
OriginalDirectoryForFileSchema = "filehash char(40), filename varchar(500), dirPathHash char(40), PRIMARY KEY (filehash, filename, dirPathHash)"

OriginalDirectoriesTable = "originalDirectories"
OriginalDirectoriesSchema = "dirPathHash char(40) PRIMARY KEY, dirPath varchar(500)"


FilesTable = "files"
FilesSchema = "filehash char(40) PRIMARY KEY, filesize int, status varchar(60)"

FileListingTable = "fileListing"
FileListingSchema = "filehash char(40), depotId INTEGER, filesize int, FOREIGN KEY (depotId) REFERENCES objectStores(depotId), PRIMARY KEY (filehash, depotId)"

'''
OriginalDirectoriesTable = "originalDirectories"
OriginalDirectoriesSchema = "dirPathHash char(40) PRIMARY KEY, dirPath varchar(500)"

DirSubDirTable = "subDirsTable"
DirSubDirSchema = "dirPathHash char(40), subDirPathHash char(40), PRIMARY KEY (dirPathHash, subDirPathHash)"



# from old depot/database
OldStatusTable = "oldStatus"
OldStatusSchema = "filehash char(40) PRIMARY KEY, oldStatus varchar(60)"

OldFileLinkTable = "oldFileLink"
OldFileLinkSchema = "filehash char(40) PRIMARY KEY, linkFileHash char(40)"

OldOriginalRootDirectoryTable = "OldOriginalRootDirectory"
OldOriginalRootDirectorySchema = "rootdir varchar(500) PRIMARY KEY"


# derivedTables, for convenience, TODO: change names so obvious derived
LocationCountTable = "locationCount"
LocationCountSchema = "filehash char(40) PRIMARY KEY, locations INTEGER"

# temp working tables, e.g. for caching largest files so do not have to keep doing expensive queries
# for now, say 1000 entries

LargestFilesTable = "largestFiles"
LargestFilesSchema = "filehash char(40) PRIMARY KEY, filesize INTEGER, status varchar(60), filenames varchar(500), directories varchar(500), depotIds varchar(50) "

ToDeleteTable = "toDelete"
ToDeleteSchema = "filehash char(40) PRIMARY KEY, oldStatus varchar(60), filesize int, filenames varchar(500), directories varchar(500)"

ToKeepForNowTable = "toKeepForNow"
ToKeepForNowSchema = "filehash char(40) PRIMARY KEY"

# temporary table used to keep track of files backed up during current backup
CurrentBackupTable = "currentBackedUp"
CurrentBackupSchema = "filehash char(40) PRIMARY KEY"
'''
