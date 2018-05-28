import pandas as pd
import numpy as np
import operator
from scipy.special import binom


class TaskAssignmentMSR:

    def __init__(self, db, job_id, worker_id, max_items):
        self.db = db
        self.job_id = job_id
        self.worker_id = worker_id
        self.max_items = max_items

    def get_tasks(self):
        sql_filter_list = '''
            select distinct(b.criterion_id) 
            from backlog b 
            where b.job_id = {job_id}
            and b.step = (
                select max(step) from backlog where job_id = {job_id}
            );'''.format(job_id=self.job_id)
        filter_list = pd.read_sql(sql_filter_list, self.db.con)['criterion_id'].values

        # randomize the order of filters available
        np.random.shuffle(filter_list)
        filter_list = [int(i) for i in filter_list]
        for filter_id in filter_list:
            sql_items_tolabel = '''
                select b.item_id 
                from backlog b 
                where b.job_id = {job_id}
                and b.criterion_id = {filter_id}
                and b.step = (
                    select max(step) from backlog where job_id = {job_id}
                )
                and compute_item_entries(b.job_id, b.item_id, b.criterion_id)
                 < (select max(step)+1 from backlog where job_id = {job_id})
            '''.format(job_id=self.job_id, filter_id=filter_id)

            items_tolabel = pd.read_sql(sql_items_tolabel, self.db.con)['item_id'].values
            items_tolabel = [int(i) for i in items_tolabel]
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

    def assign_filters(self):
        items_votes_data = self.db.get_items_tolabel_msr(self.job_id)
        items_tolabel_ids = items_votes_data['id'].unique()

        filters_assigned = []
        items_new = []
        items_stopped = {}
        for item_id in items_tolabel_ids:
            item_data = {
                'criteria': []
                }
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

                # add payload to item_data dict
                item_data['criteria'].append(self.__compute_item_filter_data(pos_c,
                                             neg_c, filter_acc, filter_select, filter_id))

                # estimate N min votes needed to exclude the item by a filter filter_id
                for n in range(1, 11):
                    # new value is negative
                    prob_vote_neg = filter_acc * prob_item_neg + (1 - filter_acc) * (1 - prob_item_neg)
                    joint_prob_votes_neg[filter_id] *= prob_vote_neg
                    term_neg = binom(pos_c + neg_c + n, neg_c + n) * filter_acc ** (neg_c + n) \
                               * (1 - filter_acc) ** pos_c * filter_select
                    term_pos = binom(pos_c + neg_c + n, pos_c) * filter_acc ** pos_c \
                               * (1 - filter_acc) ** (neg_c + n) * (1 - filter_select)
                    prob_item_neg = term_neg / (term_neg + term_pos)

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
            else:
                # mark the item as classified
                item_data['outcome'] = 'STOPPED'
                items_stopped[item_id] = item_data

        if self.__insert_items_filters(filters_assigned, items_new) and \
                self.__insert_items_filters_stopped(items_stopped):
            return "filters_assigned"
        return 'Error'

    def __compute_item_filter_data(self, pos_c, neg_c, filter_acc, filter_select, filter_id):
        term_neg = binom(pos_c + neg_c, neg_c) * filter_acc ** (neg_c) \
                   * (1 - filter_acc) ** pos_c * filter_select
        term_pos = binom(pos_c + neg_c, pos_c) * filter_acc ** pos_c \
                   * (1 - filter_acc) ** (neg_c) * (1 - filter_select)
        prob_item_filter_neg = term_pos / (term_neg + term_pos)

        item_filter_data = {
                    'id': filter_id,
                    'pout': prob_item_filter_neg,
                    'in': pos_c,
                    'out': neg_c
                }

        return item_filter_data

    def __insert_items_filters(self, filters, items):
        sql_step_old = "select max(step) from backlog where job_id = {job_id};".format(job_id=self.job_id)
        step_old = pd.read_sql(sql_step_old, self.db.con)['max'].values[0]
        if step_old == None:
            step = 0
        else:
            step = step_old + 1

        # create a list of tuples for inserting to the DB
        # [(job_id, item_id, criterion_id, step),..]
        data_to_insert = zip([self.job_id]*len(items), items, filters, [step]*len(items))
        connection = self.db.con.connect()
        trans = connection.begin()
        try:
            for data_row in data_to_insert:
                sql_insert_data_row = '''
                insert into backlog (job_id, item_id, criterion_id, step)
                values {}
                '''.format(data_row)
                connection.execute(sql_insert_data_row)
            trans.commit()
        except:
            trans.rollback()
            return False
        return True

    def __insert_items_filters_stopped(self, items_stopped):
        connection = self.db.con.connect()
        trans = connection.begin()
        try:
            for item_id in items_stopped.keys():
                data_json = pd.Series(items_stopped[item_id]).to_json()
                sql_insert_data_row = '''
                insert into result (job_id, item_id, created_at, data)
                values({job_id}, {item_id}, now(), '{data_json}')
                '''.format(job_id=self.job_id, item_id=item_id, data_json=data_json)
                connection.execute(sql_insert_data_row)
            trans.commit()
        except:
            trans.rollback()
            return False
        return True


class FilterParameters:

    def __init__(self, db, job_id, filters_data):
        # here 'criteria' == 'filter'
        self.db = db
        self.job_id = job_id
        self.filters_params_dict = filters_data
        self.filter_list = self.db.get_filters(self.job_id)

    def update_filter_select(self):
        apply_filters_prob = {}
        for filter_id in self.filter_list:
            apply_filters_prob[filter_id] = []

        # select all item-filter with at least one vote
        item_filter_data = [(54, 41), (54, 42), (54, 43)]  # TO DO!!
        for item_id, filter_id in item_filter_data:
            filter_acc = self.filters_params_dict[str(filter_id)]['accuracy']
            filter_select = self.filters_params_dict[str(filter_id)]['selectivity']

            # compute prob of applying the filter filter_id on item item_id
            pos_c, neg_c = 0, 0  # TO DO!!
            term_neg = binom(pos_c + neg_c, neg_c) * filter_acc ** (neg_c) \
                       * (1 - filter_acc) ** pos_c * filter_select
            term_pos = binom(pos_c + neg_c, pos_c) * filter_acc ** pos_c \
                       * (1 - filter_acc) ** (neg_c) * (1 - filter_select)
            prob_item_neg = term_neg / (term_neg + term_pos)
            # add prob_item_neg to the list of probs related to filter_id
            apply_filters_prob[filter_id].append(prob_item_neg)

        # update selectivity of filters
        filter_select_new = {}
        for filter_id in self.filter_list:
            filter_select_new[filter_id] = np.mean(apply_filters_prob[filter_id])

        return filter_select_new
