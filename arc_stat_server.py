import socket
import gensim
import os
from multiprocessing.dummy import Pool, Array
import multiprocessing
import ctypes
from ctypes import *

SERV_PORT = 9103

def compute_ws(param):
    global g_wv_model
    global g_serv_sock
    msg = param[0]
    a1 = int(msg.split('#')[0].strip())
    a2 = int(msg.split('#')[1].strip())
    arc_lens_arr = param[1]
    idx1 = param[2]
    idx2 = param[2]+1
    arc_lens_arr[idx1] = a1
    arc_lens_arr[idx2] = a2
    print "[DBG]: add : %d" % arc_lens_arr[idx1] 
    print "[DBG]: add : %d" % arc_lens_arr[idx2] 
    


def main():
    global g_serv_sock
    arc_lens = multiprocessing.Array(ctypes.c_int, 122500)
    g_serv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    g_serv_sock.bind(("", SERV_PORT))
    t_pool = Pool(500)
    arc_lens_idx = 0
    while True:
       msg, addr = g_serv_sock.recvfrom(4096)
       if msg == 'done':
           break
       param = (msg, arc_lens, arc_lens_idx)
       l_param = list()
       l_param.append(param)
       print l_param
       arc_lens_idx += 2
       t_pool.map(compute_ws, l_param)
    with open('/home/fcmeng/workspace/data/arc_len_stat.txt', 'w+') as arc_out:
        for i in range(len(arc_lens)):
            arc_out.write(str(arc_lens[i]) + '\n')
    arc_out.close()


main()
