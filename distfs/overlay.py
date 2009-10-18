from zope.interface import implements
from distfs import idistfs
from twisted.internet import defer


class ResolverPublisher:
    """
    @ivar node: node connected to the DHT
    @type node: C{kademlia.node.Node}
    """
    implements(idistfs.IResolver, idistfs.IPublisher)

    def __init__(self, node):
        self.node = node

    def resolve(self, chunkName):
        """
        Resolve the specified chunk name into a list of contacts where
        the chunk can be found.

        @return: a L{Deferred} that will be called with a list of
            L{Contact} objects.
        """
        h = hashlib.sha1()
        h.update(chunkName)
        key = h.digest()

        def getTargetNodes(result):
            """
            Convert stored values into contacts.
            """
            if type(result) == dict:
                return defer.DeferredList(
                    [self.node.findContact(c) for c in result[key]])
            return []

        def filterResult(result):
            """
            Filter out not found results.
            """
            return (c for (s,c) in result if s)

        completeDeferred = self.node.iterativeFindValue(key)
        completeDeferred.addCallback(getTargetNodes)
        completeDeferred.addCallback(filterResult)
        return completeDeferred

    def publish(self, chunkName):
        """
        See IPublisher.publish.
        """
        key = util.shadigest(chunkName)

        def store(result, value):
            index = list()
            if type(result) == dict:
                index.extend(result[key])
            if not value in index:
                index.append(value)
            return self.iterativeStore(key, index)

        # FIXME: is there a public API for this?
        d = self.node._iterativeFind(key, rpc='findValue')
        return d.addCallback(store, value)
        
