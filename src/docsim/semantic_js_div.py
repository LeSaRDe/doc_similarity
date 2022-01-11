from scipy.spatial.distance import jensenshannon
import os


g_lexvec_model_path = '%s/workspace/lib/lexvec/' % os.environ['HOME'] + 'lexvec.commoncrawl.300d.W+C.pos.vectors'
g_sim_threshold = 0.5


def semantic_js_div(l_text_1, l_text_2, lexival_sim_func):
