import os
import pandas as pd
from flask import Flask
from flask import request
from flask import jsonify
from flask import abort

from src.msr_box import TaskAssignmentMSR
from src.msr_box import ClassificationMSR
from src.msr_box import FilterAssignment
from src.msr_box import FilterParameters
from src.msr_box import Baseround
from src.db import Database
from src.baseround.estimation import EstimationTaskParams

# DB constants
USER = os.getenv('PGUSER') or 'postgres'
PASSWORD = os.getenv('PGPASSWORD') or 'postgres'
DB = os.getenv('PGDATABASE') or 'crowdrev'
HOST = os.getenv('PGHOST') or 'localhost'
PORT = os.getenv('PGPORT') or 5432

# connect to database
db = Database(USER, PASSWORD, DB, HOST, PORT)

app = Flask(__name__)


@app.route('/msr/generate-tasks', methods=['POST'])
def generate_tasks():
    # TO DO: check if json is valid
    # if not request.is_json:
    #     abort(400)
    content = request.get_json()
    job_id = int(content['jobId'])
    stop_score = content['stopScore']
    out_threshold = content['outThreshold']
    filters_data = content['criteria']
    fib = FilterAssignment(db, job_id, stop_score, out_threshold, filters_data)
    if fib.assign_filters() == "filters_assigned":
        response = {"message": "filters_assigned"}
        return jsonify(response)
    else:
        abort(500, {"message": "error"})


@app.route('/msr/next-task', methods=['GET'])
def tab_msr():
    job_id = int(request.args.get('jobId'))
    worker_id = int(request.args.get('workerId'))
    max_items = int(request.args.get('maxItems'))

    # task assignment baseline
    tab_msr = TaskAssignmentMSR(db, job_id, worker_id, max_items)
    items, filters = tab_msr.get_tasks()

    # check if job is finished
    # items == None -> job finished
    # items == [] -> no items to a given worker
    if items != None:
        response = {
            'items': items,
            'criteria': filters
        }
    else:
        response = {
            'done': True
        }

    return jsonify(response)


@app.route('/msr/update-filter-params/<int:job_id>', methods=['PUT'])
def update_filter_params(job_id):
    content = request.get_json()
    filters_data = content['criteria']

    fp = FilterParameters(db, job_id, filters_data)
    filter_select_new = fp.update_filter_params()

    return jsonify(filter_select_new)


@app.route('/msr/classify', methods=['POST'])
def classify():
    content = request.get_json()
    job_id = int(content['jobId'])
    filters_data = content['criteria']
    out_threshold = content['outThreshold']
    in_threshold = content['inThreshold']

    cl_msr = ClassificationMSR(db, job_id, filters_data, out_threshold, in_threshold)
    if cl_msr.classify() == "classified":
        response = {"message": "classified"}
        return jsonify(response)
    else:
        abort(500, {"message": "error"})


@app.route('/msr/estimate-task-parameters', methods=['POST'])
def estimate_task_parameters():
    content = request.get_json()
    job_id = int(content['jobId'])
    out_threshold = content['outThreshold']

    etp = EstimationTaskParams(db, job_id, out_threshold)
    p_out_statistics_raw = []  #[[p_outs for filter1], [p_outs for filter2], ..]
    response_payload = {'criteria': {}}
    workers_accuracy = {}
    filter_list = db.get_filters(job_id)
    for filter_id in filter_list:
        # get data for filter_id
        data, workers_map, item_map = etp.get_thuthfinder_input(filter_id)
        workers_num = len(workers_map)
        items_num = len(item_map)
        acc, p_out = etp.aggregate_data(workers_num, items_num, data)
        p_out_statistics_raw.append(p_out)
        filter_acc, filter_select = etp.estimate_filter_params(acc, p_out)

        # construct response payload
        # estimated accuracy of workers
        worker_acc_pair_list = [(workers_map[key], acc[key]) for key in workers_map.keys()]
        workers_accuracy[filter_id] = []
        for worker_acc in worker_acc_pair_list:
            workers_accuracy[filter_id].append(dict([worker_acc]))

        # estimated filter's accuracy and selectivity
        response_payload['criteria'][filter_id] = {
            'accuracy': filter_acc,
            'selectivity': filter_select
        }
    response_payload['workersAccuracy'] = workers_accuracy

    item_filter_pout = {}
    for item_index, item_id in item_map.items():
        item_filter_pout[item_id] = [filter_pouts[item_index]
                                     for filter_pouts in p_out_statistics_raw]

    return pd.Series(response_payload).to_json()


@app.route('/msr/generate-baseround', methods=['POST'])
def generate_baseround():
    content = request.get_json()
    job_id = int(content['jobId'])
    size = content['size']
    base = Baseround(db, job_id, size)

    if base.generate_baseround() == 'generated':
        return jsonify({"message": "generated"})
    else:
        abort(500, {"message": "error"})
