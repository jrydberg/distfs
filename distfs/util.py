from twisted.internet import defer
import os


class ParallelQueue(object):
    """
    """
    maxProcessing = 8
    
    def __init__(self, start):
        self.start = start
        self.queue = list()
        self.processing = 0
        
    def done(self, result, item):
        self.processing -= 1
        self.schdule()
        return result

    def schdule(self):
        """
        Iterate through the queue and schedule as many tasks as
        possible.
        """
        while self.queue:
            if self.processing >= self.maxProcessing:
                # We have reached the maximum number of parallel
                # tasks.
                break

            item, completeDeferred = self.queue.pop(0)

            self.processing += 1            
            self.start(item).addBoth(self.done).chainDeferred(completeDeferred)


    def add(self, item):
        """
        Add an item to the queue.

        C{item} will be passed to the queue's C{start} function.
        """
        completeDeferred = defer.Deferred()
        self.queue.append((item, completeDeferred))


def shadigest(value):
    h = hashlib.sha1()
    h.update(value)
    return h.digest()


def daemonize():
    # See http://www.erlenstar.demon.co.uk/unix/faq_toc.html#TOC16
    if os.fork():   # launch child and...
        os._exit(0) # kill off parent
    os.setsid()
    if os.fork():   # launch child and...
        os._exit(0) # kill off parent again.
    null = os.open('/dev/null', os.O_RDWR)
    for i in range(3):
        try:
            os.dup2(null, i)
        except OSError, e:
            if e.errno != errno.EBADF:
                raise
    os.close(null)
