# translation of legacy code from c# to python. Will rearchitect at some point

class FileList:

	def __init__(self):
		self.fileList = []

	def Count(self):
		return len(self.fileList)

	# expects type FileInfo
	def AddFile(self, newFile):
		self.fileList.append(newFile.FullName)

	# expects type DirectoryInfo
	def AddDirectory(self, dir):
		self.fileList.append(dir.FullName)

	def CurrentFile(self):
		if len(self.fileList) > 0:
			return self.fileList[0]
		else:
			return None

	def RemoveCurrentFile(self):
		self.fileList.pop(0)
