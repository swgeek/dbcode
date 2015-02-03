import FileList

class FileInfo(object):
	def __init__(self, fullName):
		self.FullName = fullName

class DirectoryInfo(object):
	def __init__(self, fullName):
		self.FullName = fullName

file1 = FileInfo("eg1")
file2 = FileInfo("eg2")
dir1 = FileInfo("eg3")
dir2 = FileInfo("eg4")

AA = FileList.FileList()
AA.AddFile(file1)
AA.AddFile(file2)
AA.AddDirectory(dir1)
AA.AddDirectory(dir2)

print AA.Count()

print AA.CurrentFile()

AA.RemoveCurrentFile()

print AA.CurrentFile()

print AA.Count()