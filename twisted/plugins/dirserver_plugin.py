from twisted.application.service import IServiceMaker
from twisted.application.internet import TCPServer
from twisted.plugin import IPlugin
from twisted.python import usage
from twisted.cred import strcred
from twisted.cred.portal import Portal, IRealm
from twisted.cred.checkers import AllowAnonymousAccess
#from twisted.conch.checkers import UNIXPasswordDatabase
from zope.interface import implements


from distfs.central import ServiceFactory, CentralServerFactory


class Realm:
    implements(IRealm)

    def requestAvatar(self, avatar, mind, *interfaces):
        return interfaces[0], ServiceFactory(), lambda x: None


class Options(usage.Options, strcred.AuthOptionMixin):
    optParameters = (
        ('port', 'p', '8032', 'Listen port'),
        )


class ServiceMaker(object):
    implements(IPlugin, IServiceMaker)
    
    tapname = 'dir-server'
    description = 'A distfs directory server service'

    options = Options

    def makeService(self, config):
        portal = Portal(Realm(), config["credCheckers"])
        factory = CentralServerFactory(portal)
        return TCPServer(int(config['port']), factory)

serviceMaker = ServiceMaker()
