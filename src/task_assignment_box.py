import numpy as np
import pandas as pd
import operator
from scipy.special import binom


class TaskAssignmentBaseline:

    def __init__(self, db, job_id, worker_id, max_items):
        self.db = db
        self.job_id = job_id
        self.worker_id = worker_id
        self.max_items = max_items

    def get_tasks(self):
        filter_list = self.db.get_filters(self.job_id)
        for filter_id in filter_list:
            items_tolabel = self.db.get_items_tolabel(filter_id, self.worker_id, self.job_id)
            items_tolabel_num = len(items_tolabel)

            if items_tolabel_num == 0:
                continue
            if items_tolabel_num >= self.max_items:
                return items_tolabel[:self.max_items], [filter_id]
            else:
                return items_tolabel, [filter_id]
                
        return None, None


class FilterAssignment:

    def __init__(self, db, job_id, stop_score, out_threshold, filters_data):
        # here 'criteria' == 'filter'
        self.db = db
        self.job_id = job_id
        self.stop_score = stop_score
        self.out_threshold = out_threshold
        self.filters_params_dict = filters_data
        self.filter_list = self.db.get_filters(self.job_id)
        self.filters_num = len(self.filter_list)

    def assign_filters(self):
        items_votes_data = self.db.get_items_tolabel_msr(self.job_id)
        items_tolabel_ids = items_votes_data['id'].unique()

        filters_assigned = []
        items_new = []
        for item_id in items_tolabel_ids:
            classify_score = {}
            n_min = {}

            # initialize joint probability of getting N min OUT votes
            joint_prob_votes_neg = {}
            for filter_id in self.filter_list:
                joint_prob_votes_neg[filter_id] = 1.
                filter_acc = self.filters_params_dict[str(filter_id)]['accuracy']
                filter_select = self.filters_params_dict[str(filter_id)]['selectivity']
                prob_item_neg = filter_select

                pos_c, neg_c = items_votes_data.loc[(items_votes_data['id'] == item_id) &
                                                    (items_votes_data['criteria_id'] == filter_id)][
                                                    ['in_votes', 'out_votes']].values[0]

                # estimate N min votes needed to exclude the item by a filter filter_id
                for n in range(1, 11):
                    # new value is negative
                    prob_vote_neg = filter_acc * prob_item_neg + (1 - filter_acc) * (1 - prob_item_neg)
                    joint_prob_votes_neg[filter_id] *= prob_vote_neg
                    term_neg = binom(pos_c + neg_c + n, neg_c + n) * filter_acc ** (neg_c + n) \
                               * (1 - filter_acc) ** pos_c * filter_select
                    term_pos = binom(pos_c + neg_c + n, pos_c) * filter_acc ** pos_c \
                               * (1 - filter_acc) ** (neg_c + n) * (1 - filter_select)
                    prob_item_pos = term_pos * prob_vote_neg / (term_neg + term_pos)
                    prob_item_neg = 1 - prob_item_pos
                    if prob_item_neg >= self.out_threshold:
                        classify_score[filter_id] = joint_prob_votes_neg[filter_id] / n
                        n_min[filter_id] = n
                        break
                    elif n == 10:
                        classify_score[filter_id] = joint_prob_votes_neg[filter_id] / n
                        n_min[filter_id] = n

            # find most promising filter to exclude the item
            filter_ = max(classify_score.items(), key=operator.itemgetter(1))[0]
            n_min_val = n_min[filter_]
            joint_prob = joint_prob_votes_neg[filter_]

            # check if it is needed to do stop collect votes on the item (to mark it as IN-item)
            if n_min_val / joint_prob < self.stop_score:
                filters_assigned.append(filter_)
                items_new.append(item_id)

        if self._insert_items_filters(filters_assigned, items_new):
            return "filters_assigned"
        return 'Error'

    def _insert_items_filters(self, filters, items):
        sql_step_old = "select max(step) from backlog where job_id = {job_id};".format(job_id=self.job_id)
        step_old = pd.read_sql(sql_step_old, self.db.con)['max'].values[0]
        if step_old == None:
            step = 0
        else:
            step = step_old + 1

        # create a list of tuples for inserting to the DB
        # [(job_id, item_id, criterion_id, step),..]
        data_to_insert = zip([self.job_id]*len(items), items, filters, [step]*len(items))
        trans = self.db.con.begin()
        try:
            for data_row in data_to_insert:
                sql_insert_data_row = '''
                insert into backlog (job_id, item_id, criterion_id, step)
                values {}
                '''.format(data_row)
                self.db.con.execute(sql_insert_data_row)
        except:
            trans.rollback()
            return False
        return True
