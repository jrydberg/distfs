# DIST-FS

from twisted.python import usage, log
from twisted.python.filepath import FilePath
from twisted.python.util import getPassword
from twisted.internet.protocol import ClientCreator
from twisted.internet import reactor, defer, error, task
from entangled.kademlia.node import Node as KademliaNode
from entangled.kademlia.datastore import SQLiteDataStore
from distfs.central import connectDirectoryService
from distfs.store import FileSystemStore, publishChunks
from distfs.util import daemonize
from distfs.overlay import ResolverPublisher
from distfs import server, control
from twisted.web.server import Site

import sys
import os
import sys


class AgentCommand(usage.Options):

    def getCtrl(self, service, protocol=None):
        """
        Return an amp.AMP instance that 
        """
        if protocol is None:
            protocol = control.ControlClientProtocol

        basepath = FilePath(os.path.expanduser('~/.distfs'))
        cc = ClientCreator(reactor, protocol)
        return cc.connectUNIX(basepath.child('%s.ctrl' % service).path)

    def run(self):
        return

    def cbRun(self, result):
        pass

    def ebRun(self, reason):
        if not isinstance(reason.value, error.ConnectionDone):
            reason.printTraceback()
    
    def postOptions(self):
        """
        Execute command.
        """
        d = defer.maybeDeferred(self.run)
        d.addCallback(self.cbRun)
        d.addErrback(self.ebRun)
        d.addCallback(lambda x: reactor.stop())
        reactor.run()
    

class Disconnect(AgentCommand):
    synopsys = "SERVICE"

    def parseArgs(self, service):
        self.service = service

    def cbConnect(self, protocol):
        """
        Connected to control-port on service agent.

        Shut it down.
        """
        return protocol.callRemote(control.Shutdown)

    def ebConnect(self, reason):
        """
        Failure callback.
        """
        print "%s: %s: no such service" % (sys.argv[0], self.service)
    
    def run(self):
        """
        Execute command.
        """
        d = self.getCtrl(self.service)
        d.addCallbacks(self.cbConnect, self.ebConnect)
        return d
    

class Connect(usage.Options):

    optFlags = (
        ('no-daemon', 'n', 'Do not daemonize'),
        ('logging', 'l', 'Start logging'),
        )

    optParameters = (
        ('alias', 'a', None, 'Location alias'),
        ('username', 'u', None, 'Username'),
        ('port', 'p', None, 'Port that the server listens on'),
        ('service', 's', None, 'service name'),
        ('introducer', 'i', None, 'Overlay introducer address'),
        )

    def parseArgs(self, location):
        self.location = location

    def cbConnect(self, directoryService):
        """
        Callback from the directory service.

        From this point we're connected and authenticated.
        """
        basepath = FilePath(os.path.expanduser('~/.distfs'))
        if not basepath.exists():
            basepath.createDirectory()

        store = FileSystemStore(basepath.child('store').path)
        chunkFactory = Site(server.StoreResource(store))

        locname = self['alias'] or directoryService.service

        # Listen for remote connections.  This is for the other nodes
        # to access our store.
        port = self['port'] and int(self['port']) or 0
        listeningPort = reactor.listenTCP(port, chunkFactory)

        keyStore = SQLiteDataStore(basepath.child('%s.db' % locname).path)
        dhtNode = KademliaNode(listeningPort.getHost().port, keyStore,
                               reactor=reactor)
        
        # Listen locally so that applications can easily access the
        # store.
        reactor.listenUNIX(basepath.child('%s.http' % locname).path,
                           chunkFactory)

        resolverPublisher = ResolverPublisher(dhtNode)
        
        controlFactory = control.ControlFactory(store, directoryService,
                                                dhtNode, resolverPublisher)
        reactor.listenUNIX(basepath.child('%s.ctrl' % locname).path,
                           controlFactory)

        # Start a looping call that will publish chunks to the
        # overlay; do that every 6th hour.  Delay the procedure a bit
        # so that the node has a chance to join the network.
        looping = task.LoopingCall(publishChunks, store, resolverPublisher)
        reactor.callLater(10, looping.start, 6*60*60, True)

        # Try joining the network.
        introducers = list()
        if self['introducer']:
            try:
                address, port = self['introducer'].split(':')
            except ValueError:
                address, port = self['introducer'], 8033
            introducers.append((address, int(port)))
        dhtNode.joinNetwork(introducers)

        # At this point everything that can go (majorly) wrong has
        # been initialized and we can daemonize.
        if not self['no-daemon']:
            daemonize()

    def ebConnect(self, reason):
        reason.printTraceback()
        reactor.stop()

    def postOptions(self):
        if self['logging']:
            # start logging as soon as possible
            log.startLogging(sys.stdout)

        try:
            location, port = self.location.split(':')
        except ValueError:
            location, port = self.location, 8032

        # Create the directory service
        connectDeferred = connectDirectoryService(reactor, location, port,
                                                  self['service'],
                                                  username=self['username'],
                                                  getPassword=getPassword)
        # Start the reactor.
        connectDeferred.addCallback(self.cbConnect)
        connectDeferred.addErrback(self.ebConnect)
        
        # From this point on we're all async: yay!
        reactor.run()


class Options(usage.Options):

    subCommands = (
        ('connect', None, Connect, 'Connect to remote filesysem'),
        ('disconnect', None, Disconnect, 'Disconnect service'),
        )


def main():
    Options().parseOptions()

