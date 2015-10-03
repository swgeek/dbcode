import hashlib
import os
import sys
from functools import partial


def HashToString(hashValueByteArray):
	hexString = ''.join('%02x'%x for x in hashValueByteArray)
	return hexString


# may have to do this to extract unicode values
#	asciiString = stringToHash.encode('ascii', 'ignore')
def HashString(stringToHash):
	asciiString = stringToHash
	# uncomment this if have problems with the hash because of unicode
	#asciiString = stringToHash.encode('utf-8')

	hash = hashlib.sha1(asciiString)
	hashDigest = hash.hexdigest()
	return hashDigest.upper()


# note, partial returns an object that will behave like the function passed in
# iter iterates until the sentinal is reached, in this case ''
def HashFile(filename):
	if not os.path.isfile(filename):
		raise Exception("HashFile: file %s does not exist!" % filename)

	with open(filename, mode='rb') as f:
		shahash = hashlib.sha1()
		for buf in iter(partial(f.read, 2**10), b''):
			shahash.update(buf)

	return shahash.hexdigest().upper()
