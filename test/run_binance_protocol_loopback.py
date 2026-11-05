#!/usr/bin/env python3
"""TLS/WebSocket/FIX integration tests, no exchange traffic or real credentials."""
import json
import os
from pathlib import Path
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
import time
from run_binance_sbe_loopback import exact, frame, receive
from generate_binance_protocol_fixtures import fix, ws, wrapped, base, NOW
import datetime
import base64
import hashlib


def read_fix(sock):
    header=exact(sock,6)
    length,encoding=struct.unpack('<IH',header)
    assert encoding==0xeb50 and 26<=length<=1048576
    data=header+exact(sock,length-6)
    block,tid,schema,version,seq=struct.unpack_from('<HHHHI',data,6)
    assert schema==1 and version==1
    return tid,seq,data,block


def test(executable):
    with tempfile.TemporaryDirectory() as directory:
        directory=Path(directory)
        key=directory/'key.pem'; cert=directory/'cert.pem'; tlskey=directory/'tls.pem'
        subprocess.run(['openssl','genpkey','-algorithm','ED25519','-out',str(key)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(tlskey),'-out',str(cert),'-days','1','-subj','/CN=127.0.0.1','-addext','subjectAltName=IP:127.0.0.1'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        public=directory/'public.pem'
        subprocess.run(['openssl','pkey','-in',str(key),'-pubout','-out',str(public)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(cert,tlskey)
        errors=[]; senders=[]; market_sessions=[]; finished=threading.Event()
        def serve_ws(raw):
            with context.wrap_socket(raw,server_side=True) as sock:
                sock.settimeout(10)
                head=b''
                while b'\r\n\r\n' not in head: head+=exact(sock,1)
                headers=head.decode();sbe='responseFormat=sbe' in headers
                if sbe: assert 'sbeSchemaId=3&sbeSchemaVersion=5' in headers
                key_value=next(line.split(':',1)[1].strip() for line in headers.split('\r\n') if line.lower().startswith('sec-websocket-key:'))
                accept=base64.b64encode(hashlib.sha1((key_value+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
                sock.sendall(('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n').encode())
                for operation in ['session.logon','order.place','order.cancel']:
                    opcode,data=receive(sock);assert opcode==1
                    request=json.loads(data);assert request['method']==operation
                    if operation=='session.logon':
                        assert request['params']['apiKey']=='test-key' and request['params']['signature']
                        payload=wrapped(51,dict(authorizedSince=NOW,connectedSince=NOW,serverTime=NOW,loggedOnApiKey='test-key'),request['id'])
                        result={}
                    elif operation=='order.place':
                        assert request['params']['newOrderRespType']=='ACK'
                        payload=wrapped(300,base,request['id']);result={'symbol':'BTCBRL','orderId':12345,'clientOrderId':'benchmark-1','transactTime':NOW//1000}
                    else:
                        payload=wrapped(305,dict(base,origClientOrderId='benchmark-1'),request['id']);result={'symbol':'BTCBRL','orderId':12345,'clientOrderId':'benchmark-1','status':'CANCELED'}
                    sock.sendall(frame(payload,2) if sbe else frame(json.dumps({'id':request['id'],'status':200,'result':result}).encode(),1))
                finished.wait(15)
        def serve_fix(raw):
            with context.wrap_socket(raw,server_side=True) as sock:
                sock.settimeout(10)
                tid,seq,data,block=read_fix(sock);assert tid==20008 and seq==1
                offset=26+block
                def variable(fmt):
                    nonlocal offset
                    width=struct.calcsize(fmt);length=struct.unpack_from('<'+fmt,data,offset)[0];offset+=width
                    value=data[offset:offset+length];offset+=length;return value
                sender=variable('B');target=variable('B');signature=variable('H');api_key=variable('H')
                senders.append(sender)
                assert 1 <= len(sender) <= 8
                assert target==b'SPOT' and api_key==b'test-key'
                sending_time=struct.unpack_from('<q',data,18)[0]
                stamp=datetime.datetime.fromtimestamp(sending_time/1000000,datetime.timezone.utc).strftime('%Y%m%d-%H:%M:%S.%f')
                payload=b'\x01'.join([b'A',sender,target,b'1',stamp.encode()])
                payload_path=directory/('payload-'+sender.decode());signature_path=directory/('signature-'+sender.decode())
                payload_path.write_bytes(payload);signature_path.write_bytes(base64.b64decode(signature))
                subprocess.run(['openssl','pkeyutl','-verify','-pubin','-inkey',str(public),'-rawin','-in',str(payload_path),'-sigfile',str(signature_path)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                logon=fix.message(20009,dict(HeartBtInt=30,UUID='loopback'),sequence=1)
                for chunk in [logon[:3],logon[3:17],logon[17:]]: sock.sendall(chunk);time.sleep(.003)
                tid,seq,data,block=read_fix(sock);assert seq==2
                if tid==202:
                    market_sessions.append(1)
                    reconnect=len(market_sessions)==1
                    for expected in [3,4]: assert read_fix(sock)[1]==expected
                    market=dict(TransactTime=NOW,PriceExponent=-2,QtyExponent=-8,Symbol='BTCBRL')
                    trade=fix.message(205,dict(market,MDEntries=[dict(TradeID=101,MDEntryPx=35000025,MDEntrySize=10000)]),sequence=2)
                    bbo=fix.message(206,dict(market,LastBookUpdateID=100,MDEntriesBids=[dict(MDEntryPx=35000025,MDEntrySize=10000)],MDEntriesAsks=[]),sequence=3)
                    depth=fix.message(207,dict(market,FirstBookUpdateID=101,LastBookUpdateID=101,MDEntriesBids=[],MDEntriesAsks=[]),sequence=4)
                    sock.sendall(trade+bbo+depth)
                    if reconnect: return
                else:
                    assert tid==99
                    expected=bytearray((Path(__file__).parent/'test_unit/src/binance_protocol_sbe/fixtures/fix_new_order.bin').read_bytes())
                    assert data[26:]==expected[26:]
                    sock.sendall(fix.message(198,dict(OrderID=12345,TransactTime=NOW,ExecType='0',OrdStatus='0',ClOrdID='benchmark-1',Symbol='BTCBRL'),sequence=2))
                    tid,seq,_,_=read_fix(sock);assert tid==101 and seq==3
                    sock.sendall(fix.message(198,dict(OrderID=12345,TransactTime=NOW,ExecType='4',OrdStatus='4',ClOrdID='cancel-1',Symbol='BTCBRL'),sequence=3))
                finished.wait(15)
        def guarded(handler,sock):
            try: handler(sock)
            except Exception as exc: errors.append(repr(exc))
        listeners=[];threads=[]
        for handler,connections in [(serve_fix,3),(serve_ws,2)]:
            listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen();listeners.append(listener)
            def accept_connections(listener=listener,handler=handler,connections=connections):
                for _ in range(connections):
                    raw,_=listener.accept();thread=threading.Thread(target=guarded,args=(handler,raw));thread.start();threads.append(thread)
            thread=threading.Thread(target=accept_connections,daemon=True);thread.start();threads.append(thread)
        env=dict(os.environ,CCAPI_PROTOCOL_FIX_PORT=str(listeners[0].getsockname()[1]),CCAPI_PROTOCOL_WS_PORT=str(listeners[1].getsockname()[1]),CCAPI_PROTOCOL_KEY_PATH=str(key),CCAPI_PROTOCOL_CA_PATH=str(cert))
        try: result=subprocess.run([executable,'--gtest_filter=BinanceProtocolLoopback.*'],env=env,timeout=25)
        finally:
            finished.set()
            for listener in listeners: listener.close()
            for thread in threads: thread.join(timeout=2)
        assert not errors,errors
        assert len(set(senders))==3,senders
        if result.returncode: raise SystemExit(result.returncode)
        print('TLS loopback passed: JSON/SBE WebSocket + concurrent FIX SBE sessions and reconnect')

if __name__=='__main__': test(sys.argv[1])
