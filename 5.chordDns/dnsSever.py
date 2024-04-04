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
import os
# import file


class Server(chord.ChordNode):
    def __init__(self,id,port,existing_ip=None,filename=None):
        super().__init__(id,port,existing_ip,filename)
        self.cache =cache.LRUCache(10)  #可以存储十条数据的LRU内存
        self.lock = threading.Lock()    #全局互斥锁


    def fileDel(self,key):
        # 读取文件内容
        with open(self.filePath, 'r') as file:
            lines = file.readlines()
        
        # 查找并删除包含指定key的行
        new_lines = [line for line in lines if line.split(',')[0] != key]
        
        # 将新内容写入文件
        with open(self.filePath, 'w') as file:
            file.writelines(new_lines)
        
    def get_key(self,key,hashKey):
        node = self.find_successor(hashKey)
        value = 'None'
        if node == self.node :
            with open(self.filePath, 'r') as file:
                for line in file:
                    current_key, current_value = line.strip().split(',')
                    if current_key == key:
                        value = current_value
        else:
            stub = self.get_stub(node.ip)
            value = stub.direct_get_rpc(chord_pb2.KeyValue(key=key,value="")).value
        return chord_pb2.KeyValue(key='',value=value)
    def get_key_rpc(self,request,context):
        key  = request.key
        if key in self.cache.cache:
            val = self.cache.get(key)
            # print(key,val)
            return chord_pb2.KeyValue(key='',value = val)
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
        value = 'None';key = request.key
        with open(self.filePath, 'r') as file:
                for line in file:
                    current_key, current_value = line.strip().split(',')
                    if current_key == key:
                        value = current_value
                        break
        return chord_pb2.KeyValue(key = request.key,value=value)
        # if request.key in self.data:return chord_pb2.KeyValue(key=request.key,value=self.data[request.key])
        # else :return chord_pb2.KeyValue(key = request.key,value='None')
    

    def del_key(self,key,hashKey):
        node = self.find_successor(hashKey)
        if node ==self.node:
                with self.lock:
                    self.fileDel(key)
        else :
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
        key = request.key
        self.cache.delete(key)
        with self.lock:
            self.fileDel(key)
        return chord_pb2.Empty()
    

    def add_key(self,key,value,hashKey):
        node = self.find_successor(hashKey)
        if node ==self.node:
            # self.data[key] = value
            d = {key:value}
            with self.lock:
                self.dictAddFile(d,self.filePath)
        else:
            stub = self.get_stub(node.ip)
            stub.direct_add_rpc(chord_pb2.KeyValue(key=key,value=value))
    def add_key_rpc(self,request,context):
        key = request.key;value = request.value
        self.cache.put(key,value)
        # print(self.cache.cache)
        hashKey = self.hash_key(key)
        self.add_key(key,value,hashKey)
        hashKey = self.hash_key(key+"2130")
        self.add_key(key,value,hashKey)
        hashKey = self.hash_key(key+"7383")
        self.add_key(key,value,hashKey)
        return chord_pb2.Empty()
    def direct_add_rpc(self,request,context):
        key = request.key;value =request.value
        self.cache.put(key,value)
        d = {key:value}
        with self.lock:
            self.dictAddFile(d,self.filePath)
        return chord_pb2.Empty()

    def __del__(self):
        print('leave')
        self.leave()

def run(id,port,filename='server.txt',existing_ip=None):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 绑定处理器
    chord_pb2_grpc.add_chordRPCServicer_to_server(Server(id,port,filename,existing_ip),server)
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
        


    


    
        