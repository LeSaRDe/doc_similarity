import socket
import sys

def main():
    w1 = sys.argv[1]
    w2 = sys.argv[2]
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    serv_addr = ("localhost", 8306)
    sock.sendto(w1+"#"+w2, serv_addr)
    msg, addr = sock.recvfrom(4096)
    print "sim = " + str(msg)

main()
