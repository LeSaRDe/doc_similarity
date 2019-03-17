import socket
import sys

def nasari_test(w1, w2):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    serv_addr = ("localhost", 8306)
    sock.sendto(w1+"#"+w2, serv_addr)
    msg, addr = sock.recvfrom(4096)
    print "sim = " + str(msg)

#e.g. like:v love:v
def adw_test(w1, w2):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    serv_addr = ("localhost", 8607)
    sock.sendto("tt#"+w1+"#"+w2, serv_addr)
    msg, addr = sock.recvfrom(4096)
    print "sim = " + str(msg)

def word_vect(w):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    serv_addr = ("localhost", 8306)
    sock.sendto(w, serv_addr)
    msg, addr = sock.recvfrom(4096)
    print "vect_str = " + str(msg)

def main():
    mode = sys.argv[1]
    if mode == 'nasari':
        w1 = sys.argv[2]
        w2 = sys.argv[3]
        nasari_test(w1, w2)
    elif mode == 'adw':
        w1 = sys.argv[2]
        w2 = sys.argv[3]
        adw_test(w1, w2)
    elif mode == 'vect':
        w1 = sys.argv[2]
        word_vect(w1)

main()
