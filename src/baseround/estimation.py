import numpy as np


class EstimationTaskParams:

    def __init__(self, db, job_id, out_threshold):
        # here 'criteria' == 'filter'
        self.db = db
        self.job_id = job_id
        self.project_id = self.db.get_project_id(self.job_id)
        self.out_threshold = out_threshold

    def get_thuthfinder_input(self, filter_id):
        data = None
        # workers_map {index_in_list: worker_id in DB}
        workers_map = {0: 123, 1: 234}
        # item_map {index_in_list: item_id in DB}
        item_map = {0: 20}

        return data, workers_map, item_map

    def aggregate_data(self, data):
        acc = [1, 0.8]
        p_out = [0.3, 0.8]

        return acc, p_out

    def estimate_filter_params(self, acc, p_out):
        filter_acc = np.mean(acc)
        filter_select = np.mean(p_out)

        return filter_acc, filter_select

    def classify(self, item_filter_pout):
        insert_items_filters = True
        if insert_items_filters:
            return 'classified'
        else:
            return 'error'
