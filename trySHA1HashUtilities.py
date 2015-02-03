import SHA1HashUtilities
import base64

print "array of ints"
myByteArray = [10, 240, 20, 33]
print myByteArray
print SHA1HashUtilities.SH1HashUtilities.HashToString(myByteArray)
print ""

print "try bytearray"
myByteArray = bytearray([10, 240, 20, 33])
#print myByteArray
print SHA1HashUtilities.SH1HashUtilities.HashToString(myByteArray)
print ""

myString = "Hello World"
print "hashing 'Hello World': should come out to 0a4d55a8d778e5022fab701977c5d840bbc486d0"
hashDigest = SHA1HashUtilities.SH1HashUtilities.HashString(myString)
print hashDigest
print ""

f = open('testfile.txt', 'w')
f.write('Hello World')
f.close()
print "hashing file: should come out to 0a4d55a8d778e5022fab701977c5d840bbc486d0"
hashDigest = SHA1HashUtilities.SH1HashUtilities.HashFile('testfile.txt')
print hashDigest
print ""
