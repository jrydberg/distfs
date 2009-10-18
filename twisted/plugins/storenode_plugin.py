from twisted.application.service import IServiceMaker, MultiService
from twisted.application.internet import TCPServer
from twisted.plugin import IPlugin
from twisted.python import usage
from twisted.web.server import Site
from distfs.store import FileSystemStore
from distfs.server import StoreResource
from zope.interface import implements
import os


class Options(usage.Options):
    optParameters = (
        ('port', 'p', '8033', 'Listen port'),
        ('dir', 'd', '~/.distfs/store', 'Chunk storage directory'),
        ('introducer', 'i', None, 'Introducer address'),
        )


class ServiceMaker(object):
    implements(IPlugin, IServiceMaker)
    
    tapname = 'store-node'
    description = 'A distfs store node service'

    options = Options

    def makeService(self, config):
        """
        Build and return a service based on the given options.
        """
        store = FileSystemStore(os.path.expanduser(config['dir']))
        chunkFactory = Site(server.StoreResource(store))

        multiService = MultiService()
        multiService.addservice(TCPServer(int(config['port']),
                                          chunkFactory))
        # FIXME: create dht node here.
        return multiService

serviceMaker = ServiceMaker()
