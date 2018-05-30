import numpy as np
import pandas as pd

from .aggregation import expectation_maximization


class EstimationTaskParams:

    def __init__(self, db, job_id, out_threshold):
        # here 'criteria' == 'filter'
        self.db = db
        self.job_id = job_id
        self.project_id = self.db.get_project_id(self.job_id)
        self.out_threshold = out_threshold

    def get_thuthfinder_input(self, filter_id):
        sql_data = '''
        select item_id, worker_id, (data->'criteria')::json#>>'{{0,workerAnswer}}' as vote
        from task
        where job_id = {job_id}
            and data->'criteria' @> concat('[{{"id": "',{criteria_id},'"}}]')::jsonb;
        '''.format(job_id=self.job_id, criteria_id=filter_id)
        data_df = pd.read_sql(sql_data, self.db.con)

        # item_map {index_in_list: item_id in DB}
        item_map = {}
        for item_index, item_id in enumerate(sorted(data_df['item_id'].unique())):
            item_map[item_index] = item_id
        # worker_map {index_in_list: worker_id in DB}
        worker_map = {}
        # worker_map_inverted {worker_id in DB: index_in_list} for usability
        worker_map_inverted = {}
        for worker_index, worker_id in enumerate(sorted(data_df['worker_id'].unique())):
            worker_map[worker_index] = worker_id
            worker_map_inverted[worker_id] = worker_index

        # create TruthFinder input format
        data_formated = []
        for item_index in sorted(item_map.keys()):
            data_formated.append([])
            item_id = item_map[item_index]
            item_data = data_df.loc[data_df['item_id'] == item_id]
            for _, row in item_data.iterrows():
                worker_index = worker_map_inverted[row['worker_id']]
                worker_vote = 0 if row['vote'] == 'no' else 1
                data_formated[item_index].append((worker_index, worker_vote))

        return data_formated, worker_map, item_map

    def aggregate_data(self, workers_num, items_num, data):
        acc, p_distribution = expectation_maximization(workers_num, items_num, data)
        p_out = [i[0] for i in p_distribution]

        return acc, p_out

    def estimate_filter_params(self, acc, p_out):
        filter_acc = np.mean(acc)
        filter_select = np.mean(p_out)

        return filter_acc, filter_select
