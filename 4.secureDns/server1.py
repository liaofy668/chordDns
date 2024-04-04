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
from dnsSever import Server

# server端
def run(id,port,existing_ip=None):
    server_credentials = grpc.ssl_server_credentials(
        [(open('server.key', 'rb').read(), open('server.crt', 'rb').read())]
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 绑定处理器
    chord_pb2_grpc.add_chordRPCServicer_to_server(Server(id,port,existing_ip),server)
    # server.add_insecure_port('[::]:'+str(port))
    server.add_secure_port('[::]:'+str(port),server_credentials)
    # 使用安全端口，参数是服务的生成证书
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
            




#  信道加密TLS
#  多用户with lock
#   冗余3hash
#  内存,file
#  性能测试
#  正确性测试
# python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. proto/chord.proto
        


    


    
        