import os
from flask import Flask
from flask import request
from flask import jsonify
from flask import abort

from src.msr_box import TaskAssignmentMSR
from src.msr_box import FilterAssignment
from src.msr_box import FilterParameters
from src.db import Database

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
    job_id = int(request.args.get('jobId'))
    # TO DO: check if json is valid
    # if not request.is_json:
    #     abort(400)
    content = request.get_json()
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


@app.route('/msr/update-filter-params', methods=['GET'])
def update_filter_params():
    job_id = int(request.args.get('jobId'))
    fp = FilterParameters(db, job_id)
    filter_select_new = fp.update_filter_params()

    response = {"selectivity": filter_select_new}
    return jsonify(response)
