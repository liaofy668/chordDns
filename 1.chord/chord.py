import hashlib


m = 3   # chord 网络有2**m个节点

class ChordNode():
    
    def __init__(self,node_id) -> None:
        self.id = node_id
        self.data = {}
        self.finger = {}
        self.successor = None
        self.predecessor = None

    def hash_key(self,x):
        ret = int((hashlib.sha1(x.encode()).hexdigest()),16)%(2**m)
        # print(ret)
        return ret

    def change_id(self,id):
        self.id = id

    def change_successor(self,successor):   #RPC no return
        self.successor = successor

    def change_predecessor(self,predecessor):   #RPC no return
        self.predecessor = predecessor

    def get_data(self): # RPC return dict
        return self.data

    # 加入网络
    def join(self,existing_node):
        # 如果是该网络的第一个节点
        if existing_node is None:
            self.predecessor = self
            self.successor = self
            # 初始化finger表
            # finger table中的第i项存储了在 id+2^i-1 mod2^m 之后环上的第一个node
            # 存储了从该节点开始加1、2、4……半圈后的第一个node 
            for i in range(1,m+1):
                self.finger[i] = self
        else:
            # self.successor = existing_node.find_successor(self.id)
            # if  self.successor.id == self.id:
            #     # 管理节点为当前节点，证明你的chord网里面已经有你id的节点了，此时要返回错误或者id
            #     self.change_id()
            # else :
            self.successor = existing_node.find_successor(self.id)
            self.predecessor = self.successor.predecessor
            self.init_finger_table(existing_node)   #   初始化自己节点的finger表
            self.update_other_node_create(existing_node)                #   通知其他节点更新finger表和
            self.successor.predecessor = self       # need RPC
            self.predecessor.successor = self       # need RPC
            if (self.successor.successor==self.successor): self.successor.successor = self  #need RPC
            self.update_data_create()                      #   做数据迁徙相关工作
                
    def init_finger_table(self,existing_node):
        self.finger[1] = existing_node.find_successor(self.id+1)    # 得到当前节点的successor   need RPC
        for i in range(1,m):
            # 取出finger_i_id 用于比较
            if self.finger[i].id<self.id+2**(i):
                finger_i_id = self.finger[i].id
            else :
                finger_i_id = self.finger[i].id + 2**(m)
            # 优化，如果第i+1项的start比上一项i找到的id还要大,证明在第i项和第i+1项中间没有节点，所以直接等于上一项的节点
            # 省略一次RPC,这个优化能将复杂度降到logn*logn
            if self.id+2**(i)< finger_i_id:
                self.finger[i+1] = self.finger[i]
            # 利用已知节点查找chord网络上的已知的节点，用来初始化当前的finger表,无优化时间复杂度为 mlogn
            # self.finger[i+1] = existing_node.find_successor(self.id+2**(i))
            # 进一步优化，不使用existing_node，而是使用self.successor,更容易找到节点，时间复杂度为logn
            self.finger[i+1] = self.successor.find_successor(self.id+2**(i))    # need RPC

    def update_other_node_create(self,existing_node):
        # 遍历i，对i，查找其他节点中finger[i]要变成当前节点的进行操作
        # 对于一个已有node p来说，新的node n会成为其finger table中的第i项，当且仅当：
        #       node p在node n之前并相隔至少 2^(i-1),finger表的定义
        #       node p当前的finger table中的第i项在node n之后
        for i in range(1,m+1):
            p_id = (self.id - (2 ** (i - 1))+1) % (2 ** m)
            # print(p_id)
            p    = existing_node.find_predecessor(p_id)     # need RPC
            # print(p_id,p.id)
            p.update_finger_table_create(self,i)       # need RPC
        pass

    def update_finger_table_create(self,newNode,i):     #need RPC
        # 对当前节点,要判断第i项是否改为newNode
        # 如果newNode的id在第一行开始的地方，且比当前的finger[i]小就更新
        # if newNode.id >= self.id+2**(i-1) and newNode.id < self.finger[i].id
        if (newNode.id-(self.id+2**(i-1))) % 2**m < (self.finger[i].id-(self.id+2**(i-1))) % 2**m : 
            self.finger[i] = newNode
            p = self.predecessor
            p.update_finger_table_create(newNode,i)     # need RPC

    def update_data_create(self):
        # 将successor的一部分数据转移到自己的数据中
        for key,value in self.successor.data.items():   # need RPC
            if (self.id-self.hash_key(key))%2**m <= (self.successor.id-self.hash_key(key))%2**m:
                self.data[key] = value
                del self.successor.data[key]
    
    def leave(self):
        if (self.successor == self):return
        self.update_other_node_delete()
        self.update_data_delete()
        self.successor.predecessor = self.predecessor   # need RPC
        self.predecessor.successor = self.successor     # need RPC

    def update_other_node_delete(self): 
        # 遍历i，对i，查找其他节点中finger[i]是当前节点的进行操作
        # 对于一个已有node p来说，新的node n会成为其finger table中的第i项，当且仅当：
        #       node p在node n之前并相隔至少 2^(i-1),finger表的定义
        #       node p当前的finger table中的第i项在node n之后
        for i in range(1,m+1):
            p_id = (self.id - (2 ** (i - 1))+1) % (2 ** m)
            p    = self.find_predecessor(p_id)
            p.update_finger_table_delete(self,i)    # need RPC
        
    def update_finger_table_delete(self,newNode,i):     #RPC
        if self.finger[i] == newNode :
            self.finger[i] = newNode.successor
            p  = self.predecessor
            p.update_finger_table_delete(newNode,i) # need RPC
        
    def update_data_delete(self):
        # 将当前节点的数据转移到自己的successor中
        for key,value in self.data.items():
            self.successor.data[key] = value    # need RPC
        self.data = {}

    # 找到管理key的节点,时间复杂度为logn
    def find_successor(self,hashKey):   #RPC
        # 是自己节点存储,predecessor为none表明还没初始化完成
        if self.successor == self:
            return self
        elif self.predecessor != None and (self.id-self.predecessor.id) % 2**m >= (hashKey - self.predecessor.id) % 2**m  :
            return self 
        # hashkey in (self.id,self.successor.id],返回successor
        elif (self.successor.id-self.id) % 2**m >= (hashKey-self.id) % 2**m :
            return self.successor
        else:
            return self.find_cloest_key_successor(hashKey).find_successor(hashKey)  # need RPC

    def find_predecessor(self,hashKey):
        # print(hashKey)
        node = self.find_successor(hashKey).predecessor
        # if node == self.predecessor : return self
        return node

    # 通过finger表找到最接近hashkey的节点
    def find_cloest_key_successor(self,hashKey):
        # 在node n的finger table中寻找identifier hashKey的最近的successor
        # 如果finger[i]的id在(self.id,hashKey]之间，就返回21
        for i in range(m,0,-1):
            if (self.finger[i].id-self.id) % 2**m <= (hashKey-self.id) % 2**m :
                return self.finger[i]
        #如果都不是，则它的自己是管理人
        # return self

    # 增
    def store(self,key,value):
        hashKey = self.hash_key(key)
        # 找到管理key的节点，将数据存储到该节点
        successor = self.find_successor(hashKey)
        successor.data[key] = value

    # 删
    def delete(self,key):
        hashKey = self.hash_key(key)
        # 找到管理key的节点，将key的键值删除
        successor = self.find_successor(hashKey)
        del successor.data[key]

    # 查
    def retrieve(self,key):
        hashKey = self.hash_key(key)
        # 找到管理key的节点，取出节点的数据
        successor = self.find_successor(hashKey)
        return successor.data[key]
    
    # 改
    def update(self,key,value):
        hashKey = self.hash_key(key)
        # 找到管理key的节点，将key值更新
        successor = self.find_successor(hashKey)
        successor.data[key] = value
    

    
    def __del__(self):
        self.leave()



def main ():
    global m
    m =3
    # 创建Chord节点
    node1 = ChordNode(0)
    node2 = ChordNode(3)
    node3 = ChordNode(5)

    # 加入Chord环
    node1.join(None)
    node2.join(node1)
    node3.join(node1)


    # print()
    # print(node1.predecessor.id,node1.successor.id)
    # print(node2.predecessor.id,node2.successor.id)
    # print(node3.predecessor.id,node3.successor.id)
    # print(node4.predecessor.id,node4.successor.id)

    # for p in node1.finger.values(): print(p.id,end=" ")
    # print()
    # for p in node2.finger.values(): print(p.id,end=" ")
    # print()
    # for p in node3.finger.values(): print(p.id,end=" ")
    # print()
    # for p in node4.finger.values(): print(p.id,end=" ")

    # 存储和检索数据
    node1.store("key1", "value1")
    print(node1.hash_key('key1'))
    node2.store("key2", "value2")
    print(node1.hash_key('key2'))
    node3.store("key3", "value3")
    print(node1.hash_key('key3'))
    node3.store("key4",' aa')
    print(node1.hash_key('key4'))
    print(node1.data)
    print(node2.data)
    print(node3.data)
    node3.update('key4','value4')
    node3.leave()
    print(node1.retrieve("key1"))  
    print(node1.retrieve("key2"))  
    print(node1.retrieve("key4"))  
    node1.delete("key1")
    print(node1.data)
    print(node2.data)


if __name__ =='__main__':
    main()


