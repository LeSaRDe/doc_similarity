import itertools

USER_LIST_FILE = '/home/fcmeng/user_similarity_project/top25avg'
USER_SIM_CONFIG = '/home/fcmeng/user_similarity_project/user_sim_config_top25avg'
MONTH_LIST_FILE = '/home/fcmeng/user_similarity_project/month_list_top100avg'
#USER_LIST_FILE = '/home/fcmeng/doc_sim/userlist'
#USER_SIM_CONFIG = '/home/fcmeng/doc_sim/docsim_config'
#MONTH_LIST_FILE = '/home/fcmeng/doc_sim/month_list'

def main():
    l_user= []
    with open(USER_LIST_FILE, 'r') as ulf:
        l_user = ulf.readlines()
        l_user = [x.strip() for x in l_user]
    #print l_user

    # (user_1, user_2)
    user_comb_gen = itertools.combinations(l_user, 2)
    l_user_comb = list(user_comb_gen)
    print len(l_user_comb)
     
    # (mon_1, mon_2)
    l_mon = []
    with open(MONTH_LIST_FILE, 'r') as mlf:
        l_mon = mlf.readlines()
        l_mon = [x.strip() for x in l_mon]
    l_mon_comb = zip(*(l_mon[i:] for i in xrange(2)))
    print len(l_mon_comb)

    l_config = []
    for uu in l_user_comb:
        for mm_1 in l_mon_comb:
            for mm_2 in l_mon_comb:
                l_config.append((uu[0], uu[1], mm_1[0], mm_1[1], mm_2[0], mm_2[1]))
    print len(l_config)


    with open(USER_SIM_CONFIG, 'w') as usc:
        for one_config in l_config:
            config_str = '|'.join(one_config)
            config_str += '\n'
            usc.write(config_str)

main()
