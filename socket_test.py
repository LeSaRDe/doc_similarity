import socket

SEND_ADDR_NASARI = 'hswlogin1'
SEND_PORT_NASARI = 8306
RECV_PORT = 2001

def send_wordsim_request(mode, input_1, input_2, recv_port):
    global SEND_PORT_NASARI
    global SEND_ADDR_NASARI
    attemp = 0
    ret = float(0)
    if mode == 'oo':
        synset_1_str = '+'.join(input_1)
        synset_2_str = '+'.join(input_2)
        send_str = mode + '#' + synset_1_str + '#' + synset_2_str
        send_port = SEND_PORT_ADW
        send_addr = SEND_ADDR_ADW
    elif mode == 'ww':
        #input_1 and input_2 are the two words here
        send_str = input_1 + '#' + input_2 
        send_port = SEND_PORT_NASARI
        send_addr = SEND_ADDR_NASARI

    c_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    c_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #c_sock.bind((socket.gethostname(), recv_port))
    #while attemp < 10:
    #    try:
    #        c_sock.bind((socket.gethostname(), recv_port))
    #    except socket.error, msg:
    #        print "[ERR]: bind error. " + "port:" + str(recv_port) + " is in use."
    #        print msg
    #        recv_port += 33
    #        recv_port = recv_port % 50000
    #        if recv_port < 2001:
    #            recv_port += 2001
    #        attemp += 1
    print "sendto..."
    c_sock.sendto(send_str, (send_addr, send_port))
    attemp = 0
    while attemp < 10:
        try:
            ret_str, serv_addr = c_sock.recvfrom(4096)
            ret = float(ret_str)
            #print "[DBG]: send_word_sim_request:" + str(ret)
            c_sock.close()
            return ret
        except socket.error, msg:
            print "[ERR]: Cannot get word similarity!"
            print msg
            sleep(random.randint(1, 6))
            attemp += 1
    c_sock.close()
    return ret

def main():
    for i in range(1000):
        ret = send_wordsim_request('ww', 'woman', 'man', 4000)
        print ret

main()

