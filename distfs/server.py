from twisted.web.resource import Resource
#from twisted.web.static import NoRangeStaticProducer
from twisted.web import http, server
from twisted.web.error import NoResource, ErrorPage


class ConflictResource(ErrorPage):
    """
    L{ConflictResource} is a specialization of L{ErrorPage} which returns the HTTP
    response code I{CONFLICT}.
    """
    def __init__(self, message="Sorry, that resource already exists."):
        ErrorPage.__init__(self, http.CONFLICT, "Conflict", message)



class ChunkResource(Resource):
    """
    Web resource for a specific chunk.

    @ivar store: The C{IStore} provider that stores the chunk.
    @type store: C{IStore}

    @ivar chunkName: The name of the chunk.
    @type chunkName: C{str}

    @cvar isLeaf: C{True} since a chunk can not have any children.
    """
    isLeaf = True

    def __init__(self, store, chunkName):
        """
        """
        self.store = store
        self.chunkName = chunkName

    def cbDone(self, deferResult, request):
        """
        """
        request.finish()

    def render_GET(self, request):
        """
        Render a GET request.
        """
        abspath, size, digest = self.store.query(self.chunkName)

        # update the request header with information needed for it to
        # render the response.
        try:
            contentFile = open(abspath, 'r')
        except (OSError, IOError), e:
            request.setResponseCode(500)
            return ''

        if request.setETag(digest) == http.CACHED:
            return ''

        request.setHeader('content-length', str(size))
        
        producer = NoRangeStaticProducer(request, contentFile)
        if request.method == 'HEAD':
            return ''

        producer.start()
        # and make sure the connection doesn't get closed
        return server.NOT_DONE_YET

    render_HEAD = render_GET

    def render_PUT(self, request):
        """
        Render a PUT request.
        """
        pumpDeferred = store.pump(name, request.content)
        request.setResponseCode(http.CREATED)
        pumpDeferred.addCallback(lambda x: request.finish())
        return server.NOT_YET_DONE


class StoreResource(Resource):
    """
    Resource that acts as an interface to the underlying L{IStore}
    that holds chunks of data.

    This resource is responsible for not creating any children that
    may not exist.  

    In other words; it may not create a L{ChunkResource} for a C{GET}
    request with an entity that does not exist in the store.  The same
    is true for C{PUT} requests; it may never create a child resource
    for a chunk that is availbale in the store.

    @ivar store: L{IStore} provider that acts as backing
    @type store: instance that provides L{IStore}
    """

    def __init__(self, store):
        self.store = store

    def getChild(self, chunkName, request):
        """
        Return resource for a names child.
        """
        if request.method == 'PUT':
            if self.store.hasChunk(chunkName):
                return ConflictResource()
            return ChunkResource(self.store, chunkName)
        elif request.method in ('GET', 'HEAD'):
            if not self.store.hasChunk(chunkName):
                return NoResource()
            return ChunkResource(self.store, chunkName)
        



