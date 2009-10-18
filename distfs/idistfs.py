#

from zope.interface import Interface, implements


class IStore(Interface):

    def hasChunk(chunkName):
        """
        Return C{True} if the specified chunk is available in the
        store.
        """
        
    def pump(chunkName, fromFile):
        """
        Create a new chunk in the store by pumping data from the given
        file.

        @return: a Deferred that will be called when the file has been
            transfered.
        """

    def query(self, chunkName):
        """
        Query store for information about the specified chunk.
        
        @return: a C{tuple} with an absolute path to the chunk, the
            chunk size and its hash digest.
        """

class IResolver(Interface):

    def resolve(chunk):
        """
        Resolve chunk into a list of locations where the chuck
        can be found.
        """

class IPublisher(Interface):

    def publish(chunk):
        """
        Publish to the network that the specified chunk can be found
        at this node.
        """


class IDirectoryService(Interface):
    """
    Providers of this interface enables users to resolve paths into
    chunk names.
    """

    def list(self, path):
        """
        """

    def mkdir(self, dir, filename):
        """
        """

    def unlink(self, dir, filename):
        """
        """
        
    def create(self, dir, filename, chunks):
        """
        """

    def lookup(self, dir, path):
        """
        """
