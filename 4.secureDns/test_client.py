import dnsSever
import grpc
import chord_pb2,chord_pb2_grpc
import hashlib

m = 3

class Client():
    def __init__(self,server_ip) -> None:
        self.stub = self.get_stub(server_ip)

    def hash_key(self,x):
        return int((hashlib.sha1(x.encode()).hexdigest()),16)%(2**m)

    def get_stub(self,server_ip):
        channel_credentials = grpc.ssl_channel_credentials(
            open('server.crt', 'rb').read()
        )
        channel = grpc.secure_channel(server_ip, channel_credentials)
        # channel = grpc.insecure_channel(server_ip)
        
        # 客户端实例
        stub = chord_pb2_grpc.chordRPCStub(channel)
        return stub
    
    def get(self,key):
        value = self.stub.get_key_rpc(chord_pb2.KeyValue(key=key,value="")).value
        return value
    
    def get_data(self):
        data = self.stub.get_data(chord_pb2.Empty()).data
        ret = {}
        for key,value in data.items():
            ret[key] = value
            # print(key,value)
        return ret
    
    def add(self,key,value):
        # print(self.hash_key(key))
        self.stub.add_key_rpc(chord_pb2.KeyValue(key=key,value=value))

    def update(self,key,value):
        # print(self.hash_key(key))
        self.stub.add_key_rpc(chord_pb2.KeyValue(key=key,value=value))

    def delete(self,key):
        self.stub.del_key_rpc(chord_pb2.KeyValue(key=key,value=""))

    def leave(self):
        self.stub.leave_rpc(chord_pb2.Empty())


def run():
    client1 = Client("127.0.0.1:50054")
    # client2 = Client("127.0.0.1:50053")
    # client3 = Client("127.0.0.1:50052")
    # client1.add("aaa","aaa")
    # print(client1.hash_key('aaa'),client1.hash_key('aaa2130'),client1.hash_key('aaa7383'))
    # client1.add("bbb",'bbb')
    # print(client1.hash_key('bbb'),client1.hash_key('bbb2130'),client1.hash_key('bbb7383'))
    # print(client1.get_data())
    # print(client2.get_data())
    # print(client3.get_data())
    # client.leave()
    # client1.add("aaa","aaa")
    print(client1.get_data())
    return 
    

if __name__ == '__main__':
    run()
