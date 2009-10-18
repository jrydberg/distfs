# -*- test-case-name: twisted.web.test.test_webclient -*-
# Copyright (c) 2001-2009 Twisted Matrix Laboratories.
# See LICENSE for details.

"""
HTTP client.
"""

import os, types
from urlparse import urlunparse

from twisted.python import log
from twisted.web import http
from twisted.internet import defer, protocol, reactor
from twisted.python import failure
from twisted.python.util import InsensitiveDict
from twisted.web import error
from twisted.web.http_headers import Headers
from twisted.python.compat import set


# The code which follows is based on the new HTTP client implementation.  It
# should be significantly better than anything above, though it is not yet
# feature equivalent.

from twisted.internet.protocol import ClientCreator
from twisted.web.error import SchemeNotSupported
from distfs.web._newclient import ResponseDone, Request, HTTP11ClientProtocol


class Agent(object):
    """
    L{Agent} is a very basic HTTP client.  It supports I{HTTP} scheme URIs.  It
    does not support persistent connections.

    @ivar _reactor: The L{IReactorTCP} implementation which will be used to set
        up connections over which to issue requests.

    @since: 9.0
    """
    _protocol = HTTP11ClientProtocol

    def __init__(self, reactor):
        self._reactor = reactor


    def request(self, method, uri, headers, bodyProducer):
        """
        Issue a new request.

        @param method: The request method to send.
        @type method: C{str}

        @param uri: The request URI send.
        @type uri: C{str}

        @param headers: The request headers to send.  If no I{Host} header is
            included, one will be added based on the request URI.
        @type headers: L{Headers}

        @param bodyProducer: An object which will produce the request body or,
            if the request body is to be empty, L{None}.
        @type bodyProducer: L{IEntityBodyProducer} provider

        @return: A L{Deferred} which fires with the result of the request (a
            L{Response} instance), or fails if there is a problem setting up a
            connection over which to issue the request.  It may also fail with
            L{SchemeNotSupported} if the scheme of the given URI is not
            supported.
        @rtype: L{Deferred}
        """
        scheme, host, port, path = _parse(uri)
        if scheme != 'http':
            return defer.fail(SchemeNotSupported(
                    "Unsupported scheme: %r" % (scheme,)))
        cc = ClientCreator(self._reactor, self._protocol)
        d = cc.connectTCP(host, port)
        if not headers.hasHeader('host'):
            # This is a lot of copying.  It might be nice if there were a bit
            # less.
            headers = Headers(dict(headers.getAllRawHeaders()))
            headers.addRawHeader(
                'host', self._computeHostValue(scheme, host, port))
        def cbConnected(proto):
            return proto.request(Request(method, path, headers, bodyProducer))
        d.addCallback(cbConnected)
        return d


    def _computeHostValue(self, scheme, host, port):
        """
        Compute the string to use for the value of the I{Host} header, based on
        the given scheme, host name, and port number.
        """
        if port == 80:
            return host
        return '%s:%d' % (host, port)



__all__ = [
    'PartialDownloadError',
    'HTTPPageGetter', 'HTTPPageDownloader', 'HTTPClientFactory', 'HTTPDownloader',
    'getPage', 'downloadPage',

    'ResponseDone', 'Agent']
