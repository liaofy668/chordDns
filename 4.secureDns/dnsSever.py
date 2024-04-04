import hashlib
import socket
import chord
import chord_pb2
import chord_pb2_grpc
from concurrent import futures
import grpc
import cache
import signal
import sys
import threading


class Server(chord.ChordNode):
    def __init__(self,id,port,existing_ip=None):
        super().__init__(id,port,existing_ip)
        self.cache =cache.LRUCache(10)  #可以存储十条数据的LRU内存
        self.lock = threading.Lock()
    

    def get_key(self,key,hashKey):
        node = self.find_successor(hashKey)
        if node == self.node :
            if key not in self.data:
                return chord_pb2.KeyValue(key='',value='None')
            else:value = self.data[key]
        else:
            stub = self.get_stub(node.ip)
            value = stub.direct_get_rpc(chord_pb2.KeyValue(key=key,value="")).value
        return chord_pb2.KeyValue(key='',value=value)
    def get_key_rpc(self,request,context):
        key  = request.key
        if key in self.cache:
            return chord_pb2.KeyValue(key='',value = self.cache.get(key))
        
        hashKey = self.hash_key(key)
        ret = self.get_key(key,hashKey)
        if (ret.value !='None'): return ret
        hashKey = self.hash_key(key+"2130")
        ret = self.get_key(key,hashKey)
        if (ret.value !='None'): return ret
        hashKey = self.hash_key(key+"7383")
        ret = self.get_key(key,hashKey)
        return ret
    def direct_get_rpc(self,request,context):
        if request.key in self.data:return chord_pb2.KeyValue(key = request.key,value=self.data[request.key])
        else :return chord_pb2.KeyValue(key = request.key,value='None')

    def del_key(self,key,hashKey):
        node = self.find_successor(hashKey)
        if node ==self.node:
            if key in self.data:
                with self.lock:
                    del self.data[key]
        else:
            stub = self.get_stub(node.ip)
            stub.direct_del_rpc(chord_pb2.KeyValue(key=key,value=''))
    def del_key_rpc(self,request,context):
        key = request.key
        self.cache.delete(key)  #如果内存有该key也删除
        hashKey = self.hash_key(key)
        self.del_key(key,hashKey)
        hashKey = self.hash_key(key+"2130")
        self.del_key(key,hashKey)
        hashKey = self.hash_key(key+"7383")
        self.del_key(key,hashKey)
        return chord_pb2.Empty()
    def direct_del_rpc(self,request,context):
        if request.key in self.data:del self.data[request.key]
        return chord_pb2.Empty()
    
    def add_key(self,key,value,hashKey):
        node = self.find_successor(hashKey)
        print(node.ip)
        if node ==self.node:
            with self.lock:
                self.data[key] = value
        else:
            stub = self.get_stub(node.ip)
            stub.direct_add_rpc(chord_pb2.KeyValue(key=key,value=value))
    def add_key_rpc(self,request,context):
        key = request.key;value = request.value
        if (value =='None'):return chord_pb2.Empty()
        self.cache.put(key,value)

        hashKey = self.hash_key(key)
        self.add_key(key,value,hashKey)
        hashKey = self.hash_key(key+"2130")
        self.add_key(key,value,hashKey)
        hashKey = self.hash_key(key+"7383")
        self.add_key(key,value,hashKey)
        return chord_pb2.Empty()

    def direct_add_rpc(self,request,context):
        self.data[request.key] =request.value
        return chord_pb2.Empty()


    def __del__(self):
        print('leave')
        self.leave()


def run(id,port,existing_ip=None):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 绑定处理器
    chord_pb2_grpc.add_chordRPCServicer_to_server(Server(id,port,existing_ip),server)
    server.add_insecure_port('[::]:'+str(port))
    server.start()
    print('gRPC 服务端已开启,端口为'+str(port)+'...')
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        del server
        sys.exit(0)
    # server.wait_for_termination()

def main ():
    run(1,50054)

if __name__ =='__main__':
    main()    
            




#  信道加密TLS
#  多用户with lock
#   冗余3hash
#  内存,file
#  性能测试
#  正确性测试
# python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. proto/chord.proto
        


    


    
        