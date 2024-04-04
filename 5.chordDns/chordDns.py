import sys
import os
from optparse import OptionParser
from client import Client
from grpc import RpcError

default_port = 50054

class App(object):
    def __init__(self):
        self.parser = OptionParser(version="%prog 0.1")
        # OptionParser 是一个命令行选项解析器的类，它用于解析命令行参数并生成帮助信息
        self.change_sever('12.0.0.1',50054)

    def print_help(self):
        commands = {
            "help":     "print this help",
            "connect":  "connect to another server ip:port,default is 127.0.0.1:50054",
            "add":      "add a {domain,ip} to dns serve",
            "get":      "get a ip by a domain",
            'update':   "update the domain's ip to new ip",
            'delete':   "delete the {domain,ip}",
            "quit":     "exit this process"
            }
        self.parser.print_help()
        print("\nCLI commands:")
        for (command, explanaiton) in sorted( commands.items() ):
            print( "%-10s %s" % (command, explanaiton) )

    def stop(self):
        sys.exit()

    def change_sever(self,ip,port):
        self.client = Client(str(ip)+":"+str(port))
        try:
            self.client.get('')
            print('success to connect server')
        except RpcError :
            print('failed to connect server,please start the server or choose other server')
        

    def run(self):
        print( """
        _                      _  ____              
   ___ | |__    ___   _ __  __| ||  _ \  _ __   ___ 
  / __|| '_ \  / _ \ | '__|/ _` || | | || '_ \ / __|
 | (__ | | | || (_) || |  | (_| || |_| || | | |\__ \\
  \___||_| |_| \___/ |_|   \__,_||____/ |_| |_||___/
              
     Welcome to this chord-dns client.
To find out what other commands exist, type 'help'""" )
        while True:
            try:
                io = input("~> ")
                if io.strip() == "help":
                    self.print_help()
                elif io.strip() == "connect":
                    server = input("server:")
                    port = input("port[%s]:" % default_port)
                    try:
                        port = int(port)
                    except:
                        port = default_port
                    
                    self.change_sever(server, port)
                elif io.strip() == "add":
                    domain = input("domain:").strip()
                    ip = input("IP:").strip()
                    self.client.add(domain,ip)
                    print('add new record successfully \^o^/')
                elif io.strip() == "get":
                    domain = input("domain:").strip()
                    ip = self.client.get(domain).strip()
                    print("the domain "+str(domain)+" is: "+str(ip))
                elif io.strip() == "update":
                    domain = input("domain:").strip()
                    ip = input("new IP:").strip()
                    self.client.add(domain,ip)
                    print('successfully update the record O(∩_∩)O')
                elif io.strip() == "delete":
                    domain = input("domain:").strip()
                    self.client.delete(domain)
                    print("the record "+domain+","+ip+" be deleted successfully (●'◡'●)")
                elif io.strip() == "quit":
                    print("see you again (*^_^*)")
                    self.stop()
                else:
                    print("Didn't recognize command. "
                            "Please retry or type 'help'")
            except EOFError:
                self.stop()


if __name__ == "__main__":
    app = App()
    app.run()






