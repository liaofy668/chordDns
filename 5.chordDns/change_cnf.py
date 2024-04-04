import socket

with open("./ssl/server.cnf", "r") as file:
    lines = file.readlines()

with open("./ssl/server.cnf", "w") as file:
    for line in lines:
        if "IP.1" in line:
            line = "IP.1    = "+str(socket.gethostbyname(socket.gethostname()))+"\n"
        file.write(line)

# openssl req -new -key server.key -out server.csr -config server.cnf
# openssl x509 -req -days 365 -CA ca.crt -CAkey ca.key -CAcreateserial -in server.csr -out server.crt -extensions req_ext -extfile server.cnf
