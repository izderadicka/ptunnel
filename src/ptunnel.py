#!/usr/bin/env python
'''
Created on Dec 25, 2010

@author: ivan
'''

import SocketServer
import socket
import optparse
import logging
logging.basicConfig()
log=logging.root
import sys
import threading
import os
import urlparse

REMOTE_TIMEOUT_CONNECT=10
REMOTE_TIMEOUT_RECEIVE=None

class ArgumentError(ValueError): pass

def _define_options(oparser):
    oparser.add_option('-v', '--verbose', action='store_true', help="print more information")
    oparser.add_option('-d', '--direct', action='store_true', )
    oparser.add_option('', '--debug', action='store_true')
    oparser.add_option('-p', '--proxy')

def _parse_args(args):
    parsed_args=[]
    if len(args)<1:
        raise ArgumentError("Tunnel definition arguments are mandatory")
    for item in args:
        tunnel_def=item.split(':')
        if len(tunnel_def)!= 3:
            raise ArgumentError("Invalid tunnel def - must have 3 parts")
        try:
            for idx in [0,2]:
                tunnel_def[idx]=int(tunnel_def[idx])
        except:
            raise ArgumentError("First and third value in tunnel definition must be int")
        parsed_args.append(tunnel_def)
    return parsed_args


def _system_proxy_url():
    proxy=os.environ.get('http_proxy')
    if proxy:
        return urlparse.urlparse(proxy).netloc
        
        
        
def _parse_options(oparser, args):
    opts, args=oparser.parse_args(args)
    
    proxy=opts.proxy or _system_proxy_url()
    if not proxy:
        raise ArgumentError("Proxy must be defined")
    proxy=proxy.split(':')
    if len(proxy)!=2:
        raise ArgumentError("Proxy definition must be in form host:port")
    try:
        proxy[1]=int(proxy[1])
    except:
        raise ArgumentError("Port in proxy definition must be numeric")
    opts.proxy=proxy
    args=_parse_args(args)
    
    return opts, args

servers=[]
class TunnelServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    def __init__(self, tunnel, proxy, direct):
        self.tunnel=tunnel
        self.proxy=proxy
        self.direct_as_fallback=direct
        SocketServer.TCPServer.__init__(self,('localhost', tunnel[0]),Tunnel)
        
class BackForwarder(threading.Thread): 
    def __init__(self, remote_socket, server_socket, closed_callback=None):
        super(BackForwarder, self).__init__()
        self.setDaemon(True)
        self.remote_socket=remote_socket
        self.server_socket=server_socket
        self.closed_callback=closed_callback
        self.start()
        
    def run(self):
        self.remote_socket.settimeout(REMOTE_TIMEOUT_RECEIVE)
        while 1:
            try:
                data = self.remote_socket.recv( 1024 )
                if not data: break
                self.server_socket.send( data )
            except Exception, e:
                log.debug("Remote Connection closed (%s, %s)"% (str(type(e)), str(e)))
                break
        log.info("connection to %s closed" % str(self.remote_socket))
        if self.closed_callback:
            self.closed_callback()
           
class Tunnel(SocketServer.BaseRequestHandler):
    def connect_remote(self):
        self.remote_socket=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.remote_socket.settimeout(REMOTE_TIMEOUT_CONNECT)
        self.remote_socket.connect(tuple(self.server.tunnel[1:]))
        self.remote_disconnected=False
        
    CONNECT = "CONNECT %s:%d HTTP/1.0\r\n\r\n"    
    def connect_remote_via_proxy(self):
        sock = socket.create_connection(self.server.proxy)
        sock.sendall(Tunnel.CONNECT % tuple(self.server.tunnel[1:]))
        s = ""
        while s[-4:] != "\r\n\r\n":
            s += sock.recv(1)
        print repr(s)
        self.remote_socket=sock
        self.remote_disconnected=False
        
    def notify_closed_remote(self):
        log.debug("Trying to end server connection")
        self.remote_disconnected=True
        
        
    def handle(self):
        log.info("Client %s connected for tunnel %s" % (str(self.client_address), str(self.server.tunnel)))
        
        try:
            try:
                self.connect_remote_via_proxy()
            except IOError, e:
                log.warning("Connection to proxy failed with %s" % str(e))
                if self.server.direct_as_fallback:
                    self.connect_remote()
                else:
                    raise
        except:
            log.exception("Connection to remote server %s failed" % str(self.server.tunnel[1:]))
            return
            
        fwd=BackForwarder(self.remote_socket, self.request, self.notify_closed_remote)
        self.request.settimeout(1)
        while not self.remote_disconnected:
            try:
                data = self.request.recv( 1024 )
                if not data: break
                self.remote_socket.send( data )
            except socket.timeout:
                pass
            except Exception, e:
                log.info("Local Connection closed by server (%s, %s)"% (str(type(e)), str(e)))
                break
        self.remote_socket.close()
        log.info("Client %s left" % str(self.client_address))

class ServerThread(threading.Thread):
    def __init__(self, tunnel, opts):
        self.server=TunnelServer(tunnel, opts.proxy, opts.direct)
        super(ServerThread, self).__init__()
        
    def run(self):
        self.server.serve_forever()
        
def wait_to_terminate():
    for s in servers:
        s.join()
        
def start_servers(args, opts):
    for tunnel in args:
        s=ServerThread(tunnel, opts)
        servers.append(s)
        s.start()

def main(args=sys.argv[1:]):
    oparser=optparse.OptionParser(usage= "%s [options] port:host:port [port:host:port ...]")
    _define_options(oparser)
    try:
        opts, args = _parse_options(oparser, args)
    except (optparse.OptionError, ArgumentError):
        log.exception("Invalid arguments")
        oparser.print_usage()
        sys.exit(1)
    if opts.verbose:
        log.setLevel(logging.INFO)
    if opts.debug:
        log.setLevel(logging.DEBUG)
    log.info('Tunelling %s' % ', '.join(map(lambda x: "%d->%s:%d" % tuple(x),args)))
    
    start_servers(args, opts)
    wait_to_terminate()
    
if __name__=='__main__':
    main()
    
    
