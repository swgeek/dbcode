FilesTable = "files"
FilesSchema = "filehash char(40) PRIMARY KEY, filesize int, status varchar(60)"

objectStoresTable = "objectStores"
objectStoresSchema = "depotId INTEGER PRIMARY KEY AUTOINCREMENT, description varchar(500), path varchar(500)"

FileListingTable = "fileListing"
FileListingSchema = "filehash char(40), depotId INTEGER, filesize int, FOREIGN KEY (depotId) REFERENCES objectStores(depotId), PRIMARY KEY (filehash, depotId)"

FilenamesTable = "filenames"
FilenamesSchema = "filehash char(40), filename varchar(500), PRIMARY KEY (filehash, filename)"

OriginalDirectoriesTable = "originalDirectories"
OriginalDirectoriesSchema = "dirPathHash char(40) PRIMARY KEY, dirPath varchar(500)"

DirSubDirTable = "subDirsTable"
DirSubDirSchema = "dirPathHash char(40), subDirPathHash char(40), PRIMARY KEY (dirPathHash, subDirPathHash)"

# some redundancy between this and FileNamesTable. Probably best to make FilenamesTable a derived table?
# or could have a hash for either filename or fullpath (including name), but still overlap.
# or make part of this table a foreign key so the link is obvious?
# my preference is to make FileNamesTable a derived table
OriginalDirectoryForFileTable = "originalDirectoryForFile"
OriginalDirectoryForFileSchema = "filehash char(40), filename varchar(500), dirPathHash char(40), PRIMARY KEY (filehash, filename, dirPathHash)"

NotFoundFilesTable = "notFound"
NotFoundFilesSchema = "filehash char(40), oldStatus varchar(60), filesize int, filenames varchar(500), directories varchar(500)"

# from old depot/database
OldStatusTable = "oldStatus"
OldStatusSchema = "filehash char(40) PRIMARY KEY, oldStatus varchar(60)"

OldFileLinkTable = "oldFileLink"
OldFileLinkSchema = "filehash char(40) PRIMARY KEY, linkFileHash char(40)"


#putting this in old section as may prefer to use dirhash, not actual char path
OldOriginalRootDirectoryTable = "OldOriginalRootDirectory"
OldOriginalRootDirectorySchema = "rootdir varchar(500) PRIMARY KEY"


# derivedTables, for convenience, TODO: change names so obvious derived
LocationCountTable = "locationCount"
LocationCountSchema = "filehash char(40) PRIMARY KEY, locations INTEGER"

FilenameCountTable = "filenameCount"
FilenameCountSchema = "filehash char(40) PRIMARY KEY, names INTEGER"


# temp working tables, e.g. for caching largest files so do not have to keep doing expensive queries
# for now, say 1000 entries
LargestFilesTable = "largestFiles"
LargestFilesSchema = "filehash char(40) PRIMARY KEY, filesize INTEGER, status varchar(60), filenames varchar(500), directories varchar(500), depotIds varchar(50) "

