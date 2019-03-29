from scipy import stats
import math

#from dit.divergences import jensen_shannon_divergence
#import dit

pk = [0.2, 0.4, 0.4]
qk = [0.1, 0.3, 0.6]

#def dit_js_div():
#    schema = [0, 1, 2]
#    x = dit.ScalarDistribution(schema, pk)
#    y = dit.ScalarDistribution(schema, qk)
#    print jensen_shannon_divergence([x, y])

def js_div(pk, qk):
    a = []
    for i in range(len(pk)):
        a.append(0.5*(pk[i]+qk[i]))
    js_div = 0.5*stats.entropy(pk, a, base=2) + 0.5*stats.entropy(qk, a, base=2)
    return js_div

def js_div_rbf(pk, qk):
    div = js_div(pk, qk)
    gamma = -1.0
    sim = math.exp(gamma * math.pow(div, 2))
    return sim

def main():
    print js_div(pk, qk)
#    dit_js_div()

main()