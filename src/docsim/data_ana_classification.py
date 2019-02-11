import numpy as np
import scipy as sp
import scipy.stats
import sqlite3
# from plot_roc import plot_roc
import json


def map_result(ff, ta):
    sub_ta = []
    for kk in ff:
        jj = kk.strip()
        sub_ta.append(ta[jj])
    return sub_ta


# calculate mean, mean confidence interval for 0.95
def mean_confidence_interval(data, confidence=0.99):
    a = 1.0*np.array(data)
    n = len(a)
    # ------->Method1<-------:
    # calculate mean and standard error
    m, se = np.mean(a), scipy.stats.sem(a)
    # ppf(q, df, loc=0, scale=1)	Percent point function (inverse of cdf - percentiles)
    h = se * sp.stats.t.ppf((1+confidence)/2., n-1)
    # ------->Method2<-------:
    # print "\n++++++\n", sp.stats.t.interval(0.95, len(a)-1, loc=np.mean(a), scale=scipy.stats.sem(a)),"\n++++++\n"
    return m, m-h, m+h


# Note on Standard deviation:
    # https://docs.scipy.org/doc/numpy/reference/generated/numpy.std.html
    # Means Delta Degrees of Freedom.
    # The divisor used in calculations is N - ddof, where N represents the number of elements.
    # By default ddof is zero.
def prt_result(data, mean_val, mean_conf_l, mean_conf_h, val):
    print "\n----------------------------\nError%s\nMean:%s\nMean Confidence Interval:[%s, %s]" \
          "\nStandard Deviation:%s\n----------------------------\n" % \
          (val, mean_val, mean_conf_l, mean_conf_h, np.std(data, ddof=1))
    res_output.write("\n----------------------------\nError%s\nMean:%s\nMean Confidence Interval:[%s, %s]"
                     "\nStandard Deviation:%s\n----------------------------\n" %
                     (val, mean_val, mean_conf_l, mean_conf_h, np.std(data, ddof=1)))


def cal_precision_recall_f1_kappa(res_list):
    vars_ = dict()
    vars_['precision'] = float(res_list['true_pos'])/(res_list['true_pos']+res_list['false_pos'])
    vars_['recall'] = float(res_list['true_pos'])/(res_list['true_pos']+res_list['false_neg'])
    try:
        vars_['f1_score'] = 2.0/(1.0/vars_['precision']+1.0/vars_['recall'])
    except ZeroDivisionError as e:
        print e.args, e.message
    # kappa
    total = res_list['true_pos']+res_list['true_neg']+res_list['false_pos']+res_list['false_neg']
    p0 = float((res_list['true_pos']+res_list['false_pos']))/total
    pe = float(((res_list['true_pos']+res_list['false_pos'])*(res_list['true_pos']+res_list['false_neg'])
          +(res_list['true_neg']+res_list['false_pos'])*res_list['true_neg']+res_list['false_neg']))/(total^2)
    vars_['kappa'] = (p0-pe)/(1-pe)
    res_output.write(json.dumps(vars_))
    print vars_


def main(project_name):
    dataset_name, col_name = project_name[:project_name.find('_')], project_name[project_name.find('_') + 1:]
    global conn, cur
    conn = sqlite3.connect("/home/fcmeng/workspace/data/%s.db" % dataset_name)
    cur = conn.cursor()

    # target_path = 'C:/Users/Yan Zheng/Documents/fanchao/textprocess/python/our_model/'
    # sim_file = "MBC_Babel_ADW_ALLPOS_Weight1_RMSW_NoLemma_25.txt"
    # target_path = '/home/fcmeng/workspace/data/'
    # sim_file = "sim_adw_40_w3_tag.txt"

    # file_name = re.split(r'[_.\s]\s*', sim_file)
    # thrds = []
    # for ele in file_name:
    #     if ele[0].isdigit():
    #         thrds.append("T" + ele)
    # thrds = ["T100", "T95", "T90", "T85", "T80", "T75", "T70", "T65", "T60", "T55", "T50",
    #          "T45", "T40", "T35", "T30", "T25", "T20", "T15", "T10", "T5", "T0"]
    global res_output
    output_file_path = '/home/fcmeng/workspace/data/analysis/'
    res_output = open(output_file_path + project_name+ "_res_99.txt", 'a')
    TOTAL_hs25 = 1095
    TOTAL_hs35 = 46

    human_rec_files = '/home/fcmeng/workspace/doc_similarity/res/'

    # with open(human_rec_files+"%s_all_keys.txt" % dataset_name, "r") as f:
    #     content = f.readlines()
    # all_key_pairs = [x.strip() for x in content]
    with open(human_rec_files+"hsless25.txt", "r") as f:
        content = f.readlines()
    hs25_key_pairs = [x.replace('(','').replace(')','').replace(',','#').strip() for x in content]
    with open(human_rec_files+"hsgreat35.txt", "r") as f:
        content = f.readlines()
    hs35_key_pairs = [x.replace('(','').replace(')','').replace(',','#').strip() for x in content]

    # Step1: read in similarity scores
    # to_ana_all = dict.fromkeys(thrds, dict())

    # for th in thrds:
    #     to_ana_all[th] = dict.fromkeys(all_key_pairs, dict())
    # tmp = open(target_path + sim_file, "r")
    # for line in tmp:
    #     ll = line.replace(':', '').strip().split()
    #     for c, th in enumerate(thrds):
    #         to_ana_all[th][ll[0]] = float(ll[c + 1])
    to_ana = dict(cur.execute('SELECT doc_id_pair, "%s" from docs_sim order by doc_id_pair' % col_name).fetchall())
    # TPR = 0.0
    # FPR = 0.0

    # for i, each_thrd in enumerate(thrds):
    # to_ana = to_ana_all[each_thrd]
    res = {}
    res_reverse = {}
    err25 = []
    err35 = []

    print "\n\n=========\n%s\n=========\n\n" % project_name
    res_output.write("\n\n=========\n%s\n=========\n\n" % project_name)

    # read in data
    to_ann_25 = map_result(hs25_key_pairs, to_ana)
    to_ann_35 = map_result(hs35_key_pairs, to_ana)

    # calculate mean confidence intervals
    mean25, mean25_conf_l, mean25_conf_h = mean_confidence_interval(to_ann_25)
    mean35, mean35_conf_l, mean35_conf_h = mean_confidence_interval(to_ann_35)

    # print mean confidence intervals
    prt_result(to_ann_25, mean25, mean25_conf_l, mean25_conf_h, '25')
    prt_result(to_ann_35, mean35, mean35_conf_l, mean35_conf_h, '35')

    true_pos = 0
    false_pos = 0  # count_error_25
    false_neg = 0  # count_error_35
    true_neg = 0

    # count errors
    # count_error_25 = 0
    for kkk in hs25_key_pairs:
        if to_ana[kkk] >= mean35_conf_l:
            # if to_ana[kkk] <= mean35_conf_h:  #euc
            # count_error_25 += 1
            err25.append(kkk)
            false_pos += 1
        elif to_ana[kkk] <= mean25_conf_h:
            true_pos += 1

    # count_error_35 = 0
    for kkk in hs35_key_pairs:
        if to_ana[kkk] <= mean25_conf_h:
            # if to_ana[kkk] >= mean25_conf_l:  #euc
            # count_error_35 += 1
            err35.append(kkk)
            false_neg += 1
        elif to_ana[kkk] >= mean35_conf_l:
            true_neg += 1

    print "Error 25: ", false_pos, float(false_pos) / TOTAL_hs25
    print err25
    print "Error 35: ", false_neg, float(false_neg) / TOTAL_hs35
    print err35

    res_output.write("\nError 25: %s, %s" % (false_pos, float(false_pos) / TOTAL_hs25))
    res_output.write("\nError 35: %s, %s\n\n" % (false_neg, float(false_neg) / TOTAL_hs35))
    res_output.write("\n%s\n\n%s\n\n" % (json.dumps(err25), json.dumps(err35)))

    res['true_pos'] = true_pos
    res['false_pos'] = false_pos
    res['false_neg'] = false_neg
    res['true_neg'] = true_neg

    res_reverse['true_pos'] = true_neg
    res_reverse['false_pos'] = false_neg
    res_reverse['false_neg'] = false_pos
    res_reverse['true_neg'] = true_pos

    tpr = float(true_pos) / (true_pos + false_neg)
    fpr = float(false_pos) / (false_pos + true_neg)
    # TPR.append(tpr)
    # FPR.append(fpr)
    print res, '\nTPR:', tpr, '\nFPR:', fpr

    res_output.write(str(res))
    res_output.write("\n\nTPR:%s\nFPR:%s" % (tpr, fpr))

    res_output.write("\nRegular:")
    cal_precision_recall_f1_kappa(res)
    res_output.write("\nReversed:")
    cal_precision_recall_f1_kappa(res_reverse)

    # res_output.write("\n\nTPR:%s\nFPR:%s" % (json.dumps(TPR), json.dumps(FPR)))
    # plot_roc(FPR, TPR, target_path + sim_file.replace(".txt", "") + "_roc")


main('lee_nasari_35_rmsw_w3-3')
