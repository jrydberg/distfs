# DIST-FS

from twisted.python import usage, log
from twisted.python.filepath import FilePath
from twisted.python.util import getPassword
from twisted.internet.protocol import ClientCreator
from twisted.internet import reactor, defer, error
from distfs.central import connectDirectoryService
from distfs.store import FileSystemStore
from distfs.util import daemonize
from distfs import server, control
from twisted.web.server import Site

import sys
import os


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
        ('port', 'p', '8032', 'Port that the server listens on'),
        ('dht-port', 'P', 'XXXX', 'Port XXX'),
        ('service', 's', 'service name'),
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

        # Listen for remote connections.  This is for the other nodes
        # to access our store.
        listeningPort = reactor.listenTCP(0, chunkFactory)
        
        # Listen locally so that applications can easily access the
        # store.
        locname = self['alias'] or directoryService.service
        reactor.listenUNIX(basepath.child('%s.http' % locname).path,
                           chunkFactory)

        controlFactory = control.ControlFactory(store, directoryService,
                                                None)
        reactor.listenUNIX(basepath.child('%s.ctrl' % locname).path,
                           controlFactory)

        # At this point everything that can go (majorly) wrong has
        # been initialized and we can daemonize.
        if not self['no-daemon']:
            daemonize()

    def ebConnect(self, reason):
        reason.printTraceback()
        reactor.stop()

    def postOptions(self):
        if self['logging']:
            import sys
            log.startLogging(sys.stdout)

        # Create the directory service
        connectDeferred = connectDirectoryService(reactor, self.location, 
                                                  int(self['port']),
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

