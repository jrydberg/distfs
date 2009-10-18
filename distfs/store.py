#

from twisted.internet.task import coiterate
from twisted.python.filepath import FilePath
from distfs.error import NoSuchChunkError
from distfs import idistfs
from zope.interface import implements

import hashlib

"""Storage for chunks of data.
"""

class PumpIterator(object):

    readSize = (128*1024 - 32)

    def __init__(self, toFile, fromFile):
        self.toFile = toFile
        self.fromFile = fromFile
        self.hash = hashlib.sha()

    def next(self):
        """
        Copy one chunk.
        """
        data = self.fromFile.read(self.readSize)
        if not data:
            self.toFile.close()
            raise StopIteration
        self.hash.update(data)
        self.toFile.write(data)

    def digest(self):
        """
        Return hash digest for the pumped file.

        @rtype: C{str}
        """
        return self.hash.hexdigest()


class StoreFile(object):

    def __init__(self, store, chunkName):
        self.pumpPath = store.dir.child('%s.pump' % chunkName)
        self.file = open(self.pumpPath.path, 'w')
        self.written = 0
        self.hash = hashlib.sha()
        self.chunkName = chunkName

    def write(self, bytes):
        self.written += len(bytes)
        self.file.write(bytes)
        self.hash.update(data)

    def close(self):
        """
        Close the chunk and make it available if there were any data
        written to the chunk.

        """
        if not self.written:
            self.pumpPath.remove()
            return

        destinationPath = self.store.dir.child('%s.data' % self.chunkName)
        self.pumpPath.moveTo(destinationPath)
        
        # Write hash digest to a separate file so that we may get hold
        # of it later.
        digestPath = self.store.dir.child('%s.hash' % self.chunkName)
        digestPath.setContent(self.hash.hexdigest())


class FileSystemStore(object):
    """
    Chunk store that stores chunks in a directory on the local file
    system.

    @ivar computes: a C{dict} that maps chunk ids to L{Deferreds} for
        hash computes that is currently taking place.

    """
    implements(idistfs.IStore)

    def __init__(self, dir):
        self.dir = FilePath(dir)
        self.computes = dict()

    def hasChunk(self, chunkName):
        """
        Check if the store has the specified chunk.

        @return: a C{bool} that is C{True} if the chunk is available
        locally.
        """
        dataPath = self.dir.child('%s.data' % chunkName)
        hashPath = self.dir.child('%s.hash' % chunkName)
        return dataPath.exists() and hashPath.exists()

    def __contains__(self, chunkName):
        return self.hasChunk(chunkName)

    def cbPump(self, iterator, chunkName, temporaryPath):
        """
        Called when all data has been pumped to a temporary file.

        Renames the chunk and writes a new hash file.
        """
        destinationPath = self.dir.child('%s.data' % chunkName)
        temporaryPath.moveTo(destinationPath)
        
        # Write hash digest to a separate file so that we may get hold
        # of it later.
        digestPath = self.dir.child('%s.hash' % chunkName)
        digestPath.setContent(iterator.digest())

    def pump(self, chunkName, fromFile):
        """
        Create a new file in the store by pumping data 
        from the given file.

        @return: a Deferred that will be called when the file has been
            transfered.
        """
        pumpPath = self.dir.child('%s.pump' % chunkName)

        try:
            iterator = PumpIterator(pumpPath.open('w'), fromFile)
        except OSError, e:
            return defer.fail(e)

        doneDeferred = coiterate(iterator).whenDone()
        
        # We use the same deferred as the cbPump callback is attached
        # to so that we get errors that it raises.
        return doneDeferred.addCallback(self.cbPump, chunkName, pumpPath)

    def store(self, chunkName):
        """
        Returns a file-like object that is used to write content to
        the chunk.

        The chunk is not available until the file-like object has
        been closed by invoking the C{close} method.
        """
        return StoreFile(self, chunkName)

    def query(self, chunkName):
        """
        Query store for information about the specified chunk.
        
        @return: a C{tuple} with an absolute path to the chunk, the
            chunk size and its hash digest.
        """
        dataPath = self.dir.child('%s.data' % chunkName)
        hashPath = self.dir.child('%s.hash' % chunkName)
        if dataPath.exists() and hashPath.exists():
            return (dataPath.path, dataPath.getsize(),
                    hashPath.open().read())

        raise NoSuchChunkError(chunkName)

