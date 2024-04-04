import hashlib
import socket
import chord_pb2
import chord_pb2_grpc
from concurrent import futures
from chord_pb2 import Empty
import grpc

m = 3   # chord 网络有2**m个节点

class Node():
    def __init__(self,id,ip) -> None:
        self.id = id
        self.ip = ip

class ChordNode(chord_pb2_grpc.chordRPCServicer):
    def __init__(self,id,port=80054,existing_ip=None,filePath=None) -> None:
        super().__init__()
        self.node = Node(id,socket.gethostbyname(socket.gethostname())+":"+str(port))
        if filePath ==None: self.filePath = str(self.node.id)+'.txt'
        else : self.filePath = filePath
        self.finger = {}    #finger 里面存储node
        self.data ={}
        self.successor = None
        self.predecessor = None
        self.create_file()
        self.join(existing_ip)
    #--------------------工具函数--------------------
    def create_file(self):
        with open(self.filePath, 'w') as file:
            pass
    def get_stub(self,ip):
        channel = grpc.insecure_channel(ip)
        stub = chord_pb2_grpc.chordRPCStub(channel)
        return stub
    def hash_key(self,x):
        return int((hashlib.sha1(x.encode()).hexdigest()),16)%(2**m)
    def node2proto(self,node):
        return chord_pb2.Node(id=node.id,ip=node.ip)
    def proto2node(self,proto):
        return Node(id=proto.id,ip=proto.ip)
    def dict2proto(self,data:dict):
        ret = chord_pb2.Data()
        for key,value in data.items():
            ret.data[key] = value
        return ret
    def proto2dict(self,proto):
        ret  = {}
        for key,value in proto.items():
            ret[key] = value
        return ret
    def file2dict(self,path):
        result_dict = {}
        with open(path, 'r') as file:
            for line in file:
                key, value = line.strip().split(',')
                result_dict[key] = value
        return result_dict
    def dict2file(self,d):
        with open(self.filePath, 'w') as file:
            for key, value in d.items():
                file.write(f"{key},{value}\n")
    def dictAddFile(self,d,p):
        with open(p, 'a') as file:
            for key, value in d.items():
                file.write(f"{key},{value}\n")
        
    #--------------------接口函数--------------------
    def get_predecessor(self, request, context):
        return self.node2proto(self.predecessor)
    def get_successor(self, request, context):
        return self.node2proto(self.successor)
    def change_predecessor(self, request, context):
        self.predecessor = self.proto2node(request)
        return Empty()
    def change_successor(self, request, context):
        self.successor = self.proto2node(request)
        return Empty()
    def get_data(self, request, context):
        if request.ip =='None':return self.dict2proto(self.file2dict(self.filePath))
        else: 
            # 如果新hashkey值在[newnode.id,selfnode.id)中，就转移
            mydict = self.file2dict(self.filePath)
            data = {key: value for key, value in mydict.items() 
                    if  (self.hash_key(key)-request.id)%2**m < (self.node.id-request.id)%2**m}
            return self.dict2proto(data)
    def set_data(self, request, context):
        addDict = self.proto2dict(request.data)
        self.dictAddFile(addDict,self.filePath)
        return Empty()
    def add_key_rpc(self, request, context):
        self.data[request.key] = request.value
        return Empty()
    def get_key_rpc(self, request, context):
        return chord_pb2.KeyValue(value = self.data[request.key],key ='')
    def del_key_rpc(self, request, context):
        if request.key in self.data:
            del self.data[request.key]
        return Empty()
    def find_predecessor_rpc(self, request, context):
        ret = self.find_predecessor(request.hashKey)
        return self.node2proto(ret)
    def find_successor_rpc(self,request,context):
        # print('find_success')
        ret = self.find_successor(request.hashKey)
        return self.node2proto(ret)
    def change_finger_create_rpc(self,request,context):
        self.change_finger_create(request)
        return Empty()
    def change_finger_delete_rpc(self,request,context):
        self.change_finger_delete(request)
        return Empty()
    def leave_rpc(self,request,context):
        self.leave()
        return Empty()


    #--------------------功能函数--------------------
    def find_predecessor(self,hashKey):
        successNode = self.find_successor(hashKey)
        successStub = self.get_stub(successNode.ip)
        ret = successStub.get_predecessor(self.node2proto(successNode))
        return self.proto2node(ret)
    def find_successor(self,hashKey):   #返回node
        # 只有一个节点
        # print(self.predecessor.id,self.successor.id)
        if self.successor == self.node:
            return self.node
        elif hashKey==self.predecessor.id:
            return self.predecessor
        # hashkey in (self.predecessor.id,self.id],返回self
        elif (self.node.id-self.predecessor.id) % 2**m >= (hashKey - self.predecessor.id) % 2**m  :
            return self.node
        # hashkey in (self.node.id,self.successor.id],返回successor
        elif (self.successor.id-self.node.id) % 2**m >= (hashKey-self.node.id) % 2**m :
            return self.successor
        else:
            node = self.find_closest_node(hashKey)
            stub = self.get_stub(node.ip)
            ret = stub.find_successor_rpc(chord_pb2.HashKey(hashKey=hashKey))
            return self.proto2node(ret)
    def find_closest_node(self,hashKey):    #返回node
        # 在node n的finger table中寻找identifier hashKey的最近的successor
        # 如果finger[i]的id在(self.node.id,hashKey]之间，就返回
        for i in range(m,0,-1):
            if (self.finger[i].id-self.node.id) % 2**m <= (hashKey-self.node.id) % 2**m :
                return self.finger[i]    
    
    def join(self,existing_ip):
        if existing_ip is None:
            self.predecessor = self.node
            self.successor =  self.node
            for i in range(1,m+1):self.finger[i] = self.node
        else:
            existStub = self.get_stub(existing_ip)
            self.successor = self.proto2node(existStub.find_successor_rpc(chord_pb2.HashKey(hashKey = self.node.id)))  
            successStub = self.get_stub(self.successor.ip) 
            self.predecessor = self.proto2node(successStub.get_predecessor(Empty()))  
            predeceStub = self.get_stub(self.predecessor.ip)
            self.init_finger()
            self.notify_other_create()
            successStub.change_predecessor(self.node2proto(self.node))
            predeceStub.change_successor(self.node2proto(self.node))
            if successStub.get_successor(Empty()).ip == self.successor.ip:
                successStub.change_successor(self.node2proto(self.node)) 
            self.init_data()
    def init_finger(self):
        # existStub =self.get_stub(existing_ip)
        self.finger[1] = self.successor
        successStub = self.get_stub(self.successor.ip)
        for i in range(1,m):
            # 优化，如果第i+1项的start比上一项i找到的id还要大,证明在第i项和第i+1项中间没有节点，所以直接等于上一项的节点
            if self.finger[i].id<self.node.id+2**(i): # 取出finger_i_id 用于比较
                finger_i_id = self.finger[i].id
            else :
                finger_i_id = self.finger[i].id + 2**(m)
            # 这个优化能将复杂度降到logn*logn
            if self.node.id+2**(i) < finger_i_id:
                self.finger[i+1] = self.finger[i]
            # 进一步优化，不使用existing_node，而是使用self.successor,更容易找到节点，时间复杂度为logn
            self.finger[i+1] = self.proto2node(successStub.find_successor_rpc(chord_pb2.HashKey(hashKey = self.node.id+2**i)))
            # self.finger[i+1] = self.successor.find_successor(self.node.id+2**(i))    # need RPC
    def notify_other_create(self):
        successorStub = self.get_stub(self.successor.ip)
        for i in range(1,m+1):
            p_id = (self.node.id - (2 ** (i - 1))+1) % (2 ** m)
            p_ip = successorStub.find_predecessor_rpc(chord_pb2.HashKey(hashKey = p_id)).ip
            pStub = self.get_stub(p_ip)
            pStub.change_finger_create_rpc(chord_pb2.NodeI(id=self.node.id,ip = self.node.ip,i=i))
    def change_finger_create(self,request):
        newNodeId = request.id;newNodeIP = request.ip;i = request.i
        if (newNodeId-(self.node.id+2**(i-1))) % 2**m < (self.finger[i].id-(self.node.id+2**(i-1))) % 2**m : 
            self.finger[i] = Node (id =newNodeId,ip =newNodeIP)
            pStub = self.get_stub(self.predecessor.ip)
            pStub.change_finger_create_rpc(chord_pb2.NodeI(id=self.node.id,ip = self.node.ip,i=i))
    def init_data(self):
        successStub = self.get_stub(self.successor.ip)
        data = successStub.get_data(self.node2proto(self.node)).data
        # self.data = self.proto2dict(data)
        self.dictAddFile(self.proto2dict(data),self.filePath)

    def leave(self):
        print('leave')
        if (self.successor is None or self.successor.ip==self.node.ip):return
        self.notify_other_delete()
        self.rm_data()
        successStub = self.get_stub(self.successor.ip)
        successStub.change_predecessor(self.node2proto(self.predecessor))
        # print(self.successor.ip)
        predeceStub = self.get_stub(self.predecessor.ip)
        predeceStub.change_successor(self.node2proto(self.successor))
    def notify_other_delete(self):
        # successorStub = self.get_stub(self.successor)
        for i in range(1,m+1):
            p_id = (self.node.id - (2 ** (i - 1))+1) % (2 ** m)
            p_ip = self.find_predecessor(p_id).ip
            pStub = self.get_stub(p_ip)
            pStub.change_finger_delete_rpc(chord_pb2.NodeII(rid=self.node.id,id= self.successor.id,ip = self.successor.ip,i=i))
    def change_finger_delete(self,request):
        deleteId = request.rid;i=request.i;ip = request.ip;id = request.id
        if self.finger[i].id == deleteId:
            self.finger[i] = Node(id,ip)
            pStub =self.get_stub(self.predecessor.ip)
            pStub.change_finger_delete_rpc(chord_pb2.NodeII(rid=deleteId,id=id,ip=ip,i=i))
    def rm_data(self):
        successStub = self.get_stub(self.successor.ip)
        successStub.set_data(self.dict2proto(self.file2dict(self.filePath)))
        # self.data ={}


def run():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 绑定处理器
    chord_pb2_grpc.add_chordRPCServicer_to_server(ChordNode(1,50054),server)
    server.add_insecure_port('[::]:50054')
    server.start()
    print('gRPC 服务端已开启,端口为50054...')
    server.wait_for_termination()

def main ():
    run()

if __name__ =='__main__':
    main()


# python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. proto/chord.proto