import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import norm, entropy
from sklearn.preprocessing import StandardScaler

run_name = 'nasari_40_rmswexpws_w3-2'

def entropy_multi(pk, qk):
    sum = 0
    for p, q in zip(pk, qk):
        if q != 0:
            sum += p * np.log(p / q)
        return sum 

def entropy_single(p):
    return np.sum(p * np.log(p), axis=0)

def kl_div(pk, qk):
    pk = np.asarray(pk)
    #pk = 1.0 * pk / np.sum(pk, axis=0)

    if qk is None:
        return np.sum(entropy_single(pk), axis=0)
    else:
        qk = np.asarray(qk)
        if len(qk) != len(pk):
            print("[ERR]: qk, pk don't match in size!")
            return
        #qk = 1.0 * qk / np.sum(qk, axis=0)
        return entropy(pk, qk)

def smooth_data(pk, qk):
    p_ret = []
    q_ret = []
    for p, q in zip(pk, qk):
        if q != 0.0 and p != 0.0:
            q_ret.append(q)
            p_ret.append(p)
    return p_ret, q_ret

def prob_data(pk):
    pk_ret = []
    sum_pk = sum(pk)
    for p in pk:
        p_prob = 1.0*p/sum_pk
        pk_ret.append(p_prob)
    return pk_ret

def main():
    x = np.linspace(-10.0, 10.0, 1000)
    plt.figure(figsize=(12,8))
    try:
        data_file = open('/home/fcmeng/workspace/data/{0}_compare.txt'.format(run_name), 'r')
        data_file.seek(0, 0)

        pk = data_file.readline().split(',')
        pk = map(lambda s : s.strip(), pk)
        pk = map(lambda s : float(s), pk)
        
        qk = data_file.readline().split(',')
        qk = map(lambda s : s.strip(), qk)
        qk = map(lambda s : float(s), qk) 

        pk_s, qk_s = smooth_data(pk, qk)
        pk_p = prob_data(pk_s)
        qk_p = prob_data(qk_s)
        
        kl_pq = kl_div(pk_p, qk_p)
        print("kl_pq = " + str(kl_pq))
        kl_qp = kl_div(qk_p, pk_p)
        print("kl_qp = " + str(kl_qp))
        js = (kl_pq + kl_qp) / 2
        print("[INF]: JS Div = " + str(js))
        data_file.close()
    except Exception as e:
        print(e)

main()
