import uuid
from itertools import product

import numpy as np
import pandas as pd

##################################################################
# data_fake 复赛
##################################################################
def data_generate():
    OUTPUT_DIR = r'/home/ubuntu/penggan/PycharmProjects/huawei/data_test'
    site_num = 130
    client_num = 35
    stream_num = 90
    T = 8000

    qos_constraint = 405
    V = 8000
    qos_mean, qos_var = 465, 8000
    site_bw_upper_mean, site_bw_upper_var = 100000, 4000000000
    demand_mean, demand_var = 230, 185000

    with open(f"{OUTPUT_DIR}/config.ini", 'wt') as _w:
        _w.write(f"[config]\r\nqos_constraint={qos_constraint}\r\nbase_cost={V}\r\n")


    site_names = [f'Si{i}' for i in range(site_num)]
    client_names = [f'Cl{i}' for i in range(client_num)]
    stream_names = [f'St{i}' for i in range(stream_num)]

    qos_df = pd.DataFrame(np.random.gamma((qos_mean ** 2) / qos_var, qos_var / qos_mean, size=(site_num, client_num)).astype(np.int32),
                          columns=client_names)
    qos_df['site_name'] = site_names
    qos_df = qos_df[['site_name'] + client_names]
    qos_df.to_csv(f"{OUTPUT_DIR}/qos.csv", index=False)

    site_bandwidth_df = pd.DataFrame({'site_name': site_names,
                                      'bandwidth': np.random.gamma((site_bw_upper_mean ** 2) / site_bw_upper_var,
                                                                   site_bw_upper_var / site_bw_upper_mean,
                                                                   size=site_num).astype(np.int32)})
    site_bandwidth_df.loc[0, 'bandwidth'] = int(site_bw_upper_mean / 3)
    site_bandwidth_df.loc[site_num - 1, 'bandwidth'] = int(site_bw_upper_mean / 3)
    site_bandwidth_df.to_csv(f"{OUTPUT_DIR}/site_bandwidth.csv", index=False)

    p_small = 0.99
    mean_small = 230
    var_small = 18000

    p_big = 1 - p_small
    mean_big = 10000
    var_big = 23000000

    _mean_var_table = np.array([[mean_small, var_small], [mean_big, var_big]])
    mean_var_matrix = _mean_var_table[np.random.choice(2, size=T * stream_num, p=[p_small, p_big])]
    demand_matrix = np.array([np.random.gamma((m ** 2) / v, v / m, size=client_num).astype(np.int32) for m, v in mean_var_matrix])
    demand_matrix = np.clip(demand_matrix, a_min=None, a_max=site_bw_upper_mean)

    demand_df = pd.DataFrame(demand_matrix, columns=client_names)
    mtime_list = [uuid.uuid1().hex for _ in range(T)]
    demand_time_stream_df = pd.DataFrame(product(mtime_list, stream_names), columns=['mtime', 'stream_id'])

    demand_df = pd.concat((demand_time_stream_df, demand_df), axis=1)
    demand_df.to_csv(f"{OUTPUT_DIR}/demand.csv", index=False,)

if __name__ == '__main__':
    data_generate()