from twisted.protocols import amp
from twisted.cred import credentials
from twisted.cred.error import UnauthorizedLogin
from twisted.internet.protocol import ClientFactory, Factory
from twisted.internet import defer
from zope.interface import implements, Interface
import os


VERSION = '0-experimental'


class NoSuchServiceError(Exception):
    pass


class Hello(amp.Command):
    """
    Must be sent as the first command by the client.

    This is used for version checking and simple authenitcation.
    """
    arguments = [('service', amp.String(optional=True)),
                 ('username', amp.String(optional=True)),
                 ('password', amp.String(optional=True))]
    response = [('version', amp.String()),
                ('service', amp.String())]
    errors = {UnauthorizedLogin: 'AUTH_ERROR',
              NoSuchServiceError: 'BAD_SERVICE'}


class EmptyService(object):
    name = 'empty'


class IServiceFactory(Interface):
    pass


class ServiceFactory:

    def buildService(self, service):
        """
        Return a C{ICentralService} based on the specified service
        name.
        """
        return EmptyService()


class CentralServerProtocol(amp.AMP):

    def __init__(self, portal):
        self.portal = portal
        self.service = None

    def cbLogin(self, (interface, serviceFactory, logout), serviceName):
        """
        Callback from portal.
        """
        self.service = serviceFactory.buildService(serviceName)
        if not self.service:
            raise NoSuchService(serviceName)
        return {'version':VERSION, 'service':self.service.name}

    def hello(self, service, username=None, password=None):
        """
        Client gretting.
        """
        creds = credentials.Anonymous()
        if username is not None:
            creds = credentials.UsernamePassword(username, password)
        authDeferred = self.portal.login(creds, None, IServiceFactory)
        authDeferred.addCallback(self.cbLogin, service)
        return authDeferred
    Hello.responder(hello)


class CentralServerFactory(Factory):

    def __init__(self, portal):
        self.portal = portal

    def buildProtocol(self, address):
        p = CentralServerProtocol(self.portal)
        p.factory = self
        return p
        

class CentralClientProtocol(amp.AMP):

    attempts = 0

    def cbHello(self, response):
        self.service = response['service']
        self.factory.callback(self)

    def ebHello(self, reason):
        reason.trap(UnauthorizedLogin)
        self.attempts += 1
        if self.attempts > 3 or self.factory.getPassword is None \
               or self.factory.username is None:
            self.factory.errback(failure.Failure())

        self.handshake(True)

    def handshake(self, prompt=False):
        if self.factory.username:
            password = ''
            if prompt:
                password = self.factory.getPassword()

            authDeferred = self.callRemote(Hello,
                                           username=self.factory.username,
                                           password=password,
                                           service=self.factory.service)
        else:
            authDeferred = self.callRemote(Hello, service=self.factory.service)
        authDeferred.addCallback(self.cbHello).addErrback(self.ebHello)

    def connectionMade(self):
        """
        Connection was made to the remote side.
        """
        self.handshake()



class _ClientFactory(ClientFactory):

    protocol = CentralClientProtocol

    def __init__(self, service, username, getPassword):
        self.connectDeferred = defer.Deferred()
        self.service = service
        self.username = username
        self.getPassword = getPassword

    def clientConnectionLost(self, connector, reason):
        """
        Connection was lost to the directory service.
        """
        connector.connect()

    def clientConnectionFailed(self, connector, reason):
        """
        Failed to connect to directory service.
        """
        self.errback(reason)
    
    def callback(self, result):
        d, self.connectDeferred = self.connectDeferred, None
        if d is not None:
            d.callback(result)

    def errback(self, result):
        d, self.connectDeferred = self.connectDeferred, None
        if d is not None:
            d.errback(result)


def connectDirectoryService(reactor, host, port, service, username=None,
                            getPassword=None):
    """
    Connect to a centralized directory service.

    @type reactor: C{IReactorTCP}
    @param host: hostname where the service is located
    @param port: on which port the service server answers
    @param service: the service to request
    @param username: an optional username used for authenitcation
    @param getPassword: an optional function to prompt the user for
        a password.
    """
    f = _ClientFactory(service, username, getPassword)
    reactor.connectTCP(host, port, f)
    return f.connectDeferred

