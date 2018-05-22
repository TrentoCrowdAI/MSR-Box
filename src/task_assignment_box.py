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
