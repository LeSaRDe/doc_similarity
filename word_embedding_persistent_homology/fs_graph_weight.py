import networkx as nx
import json

g_l_weeks = ['20180415_20180422', '20180429_20180506', '20180513_20180520',
            '20180527_20180603', '20180610_20180617', '20180624_20180701',
            '20180708_20180715', '20180722_20180729', '20180805_20180812',
            '20180819_20180826']
g_fs_graph_path_format = '/home/mf3jh/workspace/data/white_helmet/White_Helmet/Twitter/sampled_data/updated_data/{0}/tw_wh_fsgraph_{1}_10_sample.json'
g_fs_graph_path_out_format = '/home/mf3jh/workspace/data/white_helmet/White_Helmet/Twitter/sampled_data/updated_data/{0}/tw_wh_fsgraph_{1}_10_sample_weight.json'


def main():
    fs_undi_graph = nx.Graph()
    for week in g_l_weeks:
        with open(g_fs_graph_path_format.format(week,week), 'r') as in_fd:
            fs_graph = nx.adjacency_graph(json.load(in_fd))
            l_edge_sum = []
            # for edge in fs_graph.edges.data():
            #     d_edge_data = edge[2]
            #     edge_w_sum = d_edge_data['r'] + d_edge_data['q'] + d_edge_data['t']
            #     l_edge_sum.append(edge_w_sum)
            # max_edge_sum = max(l_edge_sum)
            for edge in fs_graph.edges.data():
                d_edge_data = edge[2]
                edge_w_sum = d_edge_data['r'] + d_edge_data['q'] + d_edge_data['t']
                # fs_graph.edges[edge[0], edge[1]]['weight'] = float(edge_w_sum) / max_edge_sum
                if not fs_undi_graph.has_edge(edge[0], edge[1]):
                    fs_undi_graph.add_edge(edge[0], edge[1], weight=edge_w_sum)
                else:
                    fs_undi_graph.edges[edge[0], edge[1]]['weight'] += edge_w_sum
            for edge in fs_undi_graph.edges.data('weight'):
                weight = edge[2]
                l_edge_sum.append(weight)
            max_weight = max(l_edge_sum)
            for edge in fs_undi_graph.edges.data('weight'):
                weight = float(edge[2]) / max_weight
                fs_undi_graph.edges[edge[0], edge[1]]['weight'] = weight
        in_fd.close()
        with open(g_fs_graph_path_out_format.format(week, week), 'w+') as out_fd:
            fs_undi_graph_data = nx.adjacency_data(fs_undi_graph)
            json.dump(fs_undi_graph_data, out_fd, indent=4)
        out_fd.close()


if __name__ == '__main__':
    main()