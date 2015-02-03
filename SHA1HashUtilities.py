import hashlib
import os
import sys
from functools import partial

class SHA1HashUtilities():

	@staticmethod
	def HashToString(hashValueByteArray):
		hexString = ''.join('%02x'%x for x in hashValueByteArray)
		return hexString

	@staticmethod
	def HashString(stringToHash):
		# will not work, need to convert to sequence of bytes
		hash = hashlib.sha1(stringToHash)
		hashDigest = hash.hexdigest()
		return hashDigest


	# note, partial returns an object that will behave like the function passed in
	# iter iterates until the sentinal is reached, in this case ''
	@staticmethod
	def HashFile(filename):
		if not os.path.isfile(filename):
			raise Exception("HashFile: file %s does not exist!" % filename)

		with open(filename, mode='rb') as f:
			shahash = hashlib.sha1()
			for buf in iter(partial(f.read, 2**10), b''):
				shahash.update(buf)

		return shahash.hexdigest()

	# there is a "CombineHashValues" in original code, but don't think I use that any more so not porting




