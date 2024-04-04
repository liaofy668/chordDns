from chord import ChordNode

def main ():
    # 创建Chord节点
    node1 = ChordNode(0)
    node2 = ChordNode(3)
    node3 = ChordNode(5)
    node4 = ChordNode(7)

    # 加入Chord环
    node1.join(None)
    node2.join(node1)
    node3.join(node1)
    node4.join(node1)

    node4.leave()

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
    node2.store("key2", "value2")
    node3.store("key3", "value3")
    node3.store("key4",'aa')

    node3.leave()
    print(node1.retrieve("key1"))  # 输出: value1
    print(node1.retrieve("key2"))  # 输出: value2
    print(node1.retrieve("key3"))  # 输出: value3

    print(node1.data)
    print(node2.data)
    print(node3.data)


if __name__ =='__main__':
    main()
