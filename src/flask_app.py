import os
from flask import Flask
from flask import request
from flask import jsonify

from src.task_assignment_box import TaskAssignmentBaseline
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


@app.route('/next-task', methods=['GET'])
def tab_baseline():
    job_id = int(request.args.get('jobId'))
    worker_id = int(request.args.get('workerId'))
    max_items = int(request.args.get('maxItems'))

    # task assignment baseline
    tab = TaskAssignmentBaseline(db, job_id, worker_id, max_items)
    items, criteria = tab.get_tasks()

    # check if job is finished
    # items == None -> job finished
    # items == [] -> no items to a given worker
    if items != None:
        response = {
            'items': items,
            'criteria': criteria
        }
    else:
        response = {
            'done': True
        }

    return jsonify(response)
