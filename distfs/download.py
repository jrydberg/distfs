
class QueueHandle(object):

    def __init__(self, downloader, chunkName, locations):
        self.downloader = downloader
        self.chunkName = chunkName
        self.deferred = defer.Deferred()
        
    def cancel(self):
        """
        Cancel the download of this chunk.
        """
        self.deferred = None
        self.downloader.cancel(self)

    def notify(self):
        """
        Notify listeners that this chunk has been downloaded.
        """
        d, self.deferred = self.deferred, None
        if d is not None:
            d.callback(None)

    def error(self):
        d, self.deferred = self.deferred, None
        if d is not None:
            d.errback(None)

    def whenDone():
        """
        Return a L{Deferred} that will be called when the chunk has
        been fetched.
        """
        doneDeferred = defer.Deferred()
        self.deferred.chainDeferred(doneDeferred)
        return doneDeferred

    def setCallback(self, callable):
        self.callback = callable

    def setErrback(self, callable):
        self.errback = callable



class Downloader(object):
    """
    Downloader.

    @ivar store: a L{I downloaded chunks will be stored
    @type store: L{IStore} 
    """

    def __init__(self, store):
        self.store = store
        self.handles = list()
        self.queue = ParallelQueue(self.startDownload)

    @defer.inlineCallbacks
    def startDownload(self, queueHandle):
        """
        Callback from the download queue that instructs us to start to
        download the chunk described by the given queue handle.
        """
        locations = list(queueHandler.locations)
        random.shuffle(locations)
        for location in locations:
            f = self.store.store(queueHandle.chunkName)
            try:
                value = yield downloadPage(location, f)
            except:
                continue
            defer.returnValue(None)

        raise Exception("could not download anything")

    def cancel(self, queueHandle):
        pass

    def add(self, chunkName, locations):
        """
        Tell the downloader to try to retrieve the specified chunk from
        one of the given locations.

        @return: a queue handle
        @rtype: L{QueueHandle}
        """
        queueHandler = QueueHandler(self, chunkName, locations)
        completedDeffered = self.queue.add(queueHandler)
        completedDeffered.addCallback(lambda result: queueHandler.notify())
        completedDeferred.addErrback(lambda result: queueHandler.error())
        return queueHandler


