#!/usr/bin/env python3
"""Independent struct-based wire fixtures from Binance's checked-in XML schemas."""
from pathlib import Path
import struct
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'test/test_unit/src/binance_protocol_sbe/fixtures'
FORMATS = {'char':'B','uint8':'B','int8':'b','uint16':'H','int16':'h','uint32':'I','int32':'i','uint64':'Q','int64':'q'}
NOW = 1720000000000123
NS = '{http://fixprotocol.io/2016/sbe}'

class Schema:
    def __init__(self, path):
        self.root = ET.parse(path).getroot()
        self.types = {e.get('name'): e for e in self.root.find('types')}
        self.messages = {int(e.get('id')): e for e in self.root.findall(NS+'message')}
    def primitive(self, kind):
        if kind in FORMATS: return kind
        node = self.types[kind]
        return node.get('primitiveType', node.get('encodingType'))
    def field(self, node, values):
        kind = node.get('type'); p = self.primitive(kind)
        optional = node.get('presence') == 'optional'
        t = self.types.get(kind)
        if node.get('name') in values: value = values[node.get('name')]
        elif optional:
            size = struct.calcsize(FORMATS[p])*8
            value = 0 if p=='char' else (-2**(size-1) if p.startswith('int') else 2**size-1)
        elif t is not None and t.tag=='enum': value = t[0].text
        else: value = 0
        if isinstance(value,str): value = ord(value) if p=='char' else int(value)
        return struct.pack('<'+FORMATS[p], value)
    def body(self, node, values, version):
        fields = [n for n in node if n.tag=='field' and int(n.get('sinceVersion','0'))<=version]
        fixed = b''.join(self.field(n,values) for n in fields)
        tail = b''
        for child in node:
            if child.tag=='group':
                entries = values.get(child.get('name'),[])
                group_fields = [n for n in child if n.tag=='field' and int(n.get('sinceVersion','0'))<=version]
                block = sum(struct.calcsize(FORMATS[self.primitive(n.get('type'))]) for n in group_fields)
                dim = self.types[child.get('dimensionType','groupSizeEncoding')]
                tail += struct.pack('<'+FORMATS[dim[0].get('primitiveType')]+FORMATS[dim[1].get('primitiveType')],block,len(entries))
                for entry in entries: tail += self.body(child,entry,version)[1]
            elif child.tag=='data':
                payload = values.get(child.get('name'),b'')
                if isinstance(payload,str): payload=payload.encode()
                primitive = self.types[child.get('type')][0].get('primitiveType')
                tail += struct.pack('<'+FORMATS[primitive],len(payload))+payload
        return len(fixed),fixed+tail
    def message(self,tid,values,sequence=2,version=None):
        if version is None: version=int(self.root.get('version'))
        block, body = self.body(self.messages[tid], values, version)
        head = struct.pack('<HHHH',block,tid,int(self.root.get('id')),version)
        if self.root.get('id')=='1':
            head+=struct.pack('<Iq',sequence,NOW)
            return struct.pack('<IH',6+len(head)+len(body),0xeb50)+head+body
        return head+body

fix=Schema(ROOT/'spec/spot-fixsbe-1_2.xml')
ws=Schema(ROOT/'spec/spot_3_4.xml')
def save(name,data): (OUT/(name+'.bin')).write_bytes(data)

def wrapped(tid,values,request_id='order_place7',status=200):
    return ws.message(50,dict(status=status,id=request_id,result=ws.message(tid,values)))
base=dict(priceExponent=-2,qtyExponent=-8,orderId=12345,transactTime=NOW,price=35000025,origQty=10000,orderType=1,side=0,symbol='BTCBRL',clientOrderId='benchmark-1')

def generate():
    OUT.mkdir(parents=True, exist_ok=True)
    save('fix_new_order',fix.message(99,dict(PriceExponent=-2,QtyExponent=-8,OrderQty=10000,OrdType='2',Side='1',Price=35000025,TimeInForce='1',ClOrdID='benchmark-1',Symbol='BTCBRL'),sequence=7))
    save('fix_cancel',fix.message(101,dict(OrderID=12345,ClOrdID='cancel-1',Symbol='BTCBRL'),sequence=8))
    save('fix_md_request',fix.message(202,dict(SubscriptionRequestType='1',MarketDepth=1,RelatedSym=[dict(Symbol='BTCBRL')],MDEntryTypes=[dict(MDEntryType='0'),dict(MDEntryType='1')],MDReqID='bbo')))
    save('fix_logon_ack',fix.message(20009,dict(HeartBtInt=30,UUID='test-session'),sequence=1))
    base=dict(TransactTime=NOW,PriceExponent=-2,QtyExponent=-8,Symbol='BTCBRL')
    save('fix_trade',fix.message(205,dict(base,MDEntries=[dict(TradeID=101,MDEntryPx=35000025,MDEntrySize=10000,AggressorSide='1'),dict(TradeID=102,MDEntryPx=35000026,MDEntrySize=10000,AggressorSide='2')])))
    for name,tid in [('fix_bbo',206),('fix_depth',207),('fix_snapshot',204)]:
        bid=dict(MDEntryPx=35000025)
        if tid!=207: bid['MDEntrySize']=10000
        save(name,fix.message(tid,dict(base,FirstBookUpdateID=100,LastBookUpdateID=101,MDEntriesBids=[bid],MDEntriesAsks=[dict(MDEntryPx=35000100,MDEntrySize=20000)])))
    execution=dict(base,OrderID=12345,OrderQty=10000,Price=35000025,OrdType='2',Side='1',ExecType='0',OrdStatus='0',ClOrdID='benchmark-1')
    save('fix_execution',fix.message(98,execution))
    save('fix_execution_v0',fix.message(98,execution,version=0))
    save('fix_ack',fix.message(198,dict(OrderID=12345,TransactTime=NOW,ExecType='0',OrdStatus='0',ClOrdID='benchmark-1',Symbol='BTCBRL')))
    save('fix_logon',fix.message(20008,dict(EncryptMethod=0,HeartBtInt=30,ResetSeqNumFlag=1,MessageHandling=1,SenderCompId='1',TargetCompId='SPOT',RawData='signature',Username='key'),sequence=1))

    base=dict(priceExponent=-2,qtyExponent=-8,orderId=12345,transactTime=NOW,price=35000025,origQty=10000,orderType=1,side=0,symbol='BTCBRL',clientOrderId='benchmark-1')
    save('ws_ack',wrapped(300,base))
    save('ws_result',wrapped(301,base))
    save('ws_full',wrapped(302,dict(base,fills=[dict(commissionExponent=-8,price=35000025,qty=10000,commission=1,commissionAsset='BTC',tradeId=101)])))
    save('ws_cancel',wrapped(305,dict(base,origClientOrderId='benchmark-1'),'order_cancel8'))
    save('ws_error',wrapped(100,dict(code=-2010,msg='Insufficient balance'),status=400))
    save('ws_logon',wrapped(51,dict(authorizedSince=NOW,connectedSince=NOW,serverTime=NOW,loggedOnApiKey='test-key'),'session_logon'))
    save('ws_execution',ws.message(603,dict(base,eventTime=NOW,transactTime=NOW,executionType=4,orderStatus=2,executedQty=10000,lastQty=10000,lastPrice=35000025,tradeId=101,commissionExponent=-8,commission=1,commissionAsset='BTC')))
    save('ws_balance',ws.message(607,dict(eventTime=NOW,updateTime=NOW,balances=[dict(exponent=-8,free=100000000,locked=0,asset='BTC')])) )

if __name__ == "__main__":
    generate()
