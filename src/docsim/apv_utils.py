import pandas as pd
import scipy.spatial.distance as scipyd


def load_apv_from_csv(file_path):
    apv_df = pd.read_csv(file_path, index_col=0)
    return apv_df


def compare_two_apvs(col1_vect, col2_vect, dis_mode):
    if dis_mode == 'cosine':
        return scipyd.cosine(col1_vect, col2_vect)

# input: list of list (each element list is an APV)
# output: list of list (same number of APVs yet with lower dimensions)
# TODO
def dim_reduct(apv_mat):
    return apv_mat


# Construct affinity matrix
# Require a mapping between document ids and matrix ids


# Construct APV matrix (list of list, and each element list is an APV.)