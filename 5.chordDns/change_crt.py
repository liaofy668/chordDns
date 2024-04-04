import subprocess
import os

# 获取当前工作目录
current_directory = os.getcwd()
current_file = os.path.abspath(__file__)
# 上一级目录
directory = os.path.dirname(current_file)
relative_path = os.path.relpath(directory, current_directory)

# 运行文件
# subprocess.call("cd "+relative_path,shell=True)
os.chdir(directory)
subprocess.call("python change_cnf.py", shell=True)

# 运行其他命令行语句
# subprocess.call("cd ssl",shell=True)
os.chdir(directory+"/ssl")
# subprocess.call("openssl req -new -key server.key -out server.csr -config server.cnf", shell=True)
process = subprocess.Popen("openssl req -new -key server.key -out server.csr -config server.cnf", shell=True, stdin=subprocess.PIPE)
process.communicate(input=b'\n\n\n\n\n')  # 模拟敲击5次回车键
subprocess.call("openssl x509 -req -days 365 -CA ca.crt -CAkey ca.key -CAcreateserial -in server.csr -out server.crt -extensions req_ext -extfile server.cnf", shell=True)