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

class Server(chord_pb2_grpc.chordRPCServicer):
    def __init__(self,file_path):
        self.file_path = file_path
        with open(self.file_path, 'w') as file:
            pass

    def get_key_rpc(self,request,context):
        key = request.key
        value = 'None'
        with open(self.file_path, 'r') as file:
            for line in file:
                current_key, current_value = line.strip().split(',')
                if current_key == key:
                    value = current_value
        return chord_pb2.KeyValue(key='',value=value)
    
    def add_key_rpc(self,request,context):
        key = request.key;value = request.value
        with open(self.file_path, 'a') as file:
            file.write(f"{key},{value}\n")
        return chord_pb2.Empty()


def run(id,port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 绑定处理器
    chord_pb2_grpc.add_chordRPCServicer_to_server(Server('oneSever.txt'),server)
    server.add_insecure_port('[::]:'+str(port))
    server.start()
    print('gRPC 服务端已开启,端口为'+str(port)+'...')
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        del server
        sys.exit(0)


def main ():
    run(1,50054)

if __name__ =='__main__':
    main()    
            