from celery import shared_task
from django.http import HttpResponse, HttpResponseBadRequest
from redis import BlockingConnectionPool
from redis.client import Redis
import json

client = Redis(connection_pool=BlockingConnectionPool(max_connections=5))


def handler(request, task_id=None):
    if request.method == 'GET':
        return get_table(task_id)
    elif request.method == 'POST':
        return post_table(request)


def get_table(task_id):
    if id is None:
        return HttpResponseBadRequest("Missing task id as path param, ie. /table/410bf3ba-d77b-4840-8001-18d1888de11f")

    raw_bytes = client.get("celery-task-meta-" + task_id)
    json_representation = raw_bytes.decode('utf-8')
    return HttpResponse(json_representation)


def post_table(request):
    header = request.GET.get('headerRow', 'False')
    raw_data = request.body
    response = {}

    if raw_data is None:
        response = {"status": "error"}
        return HttpResponseBadRequest(response)

    data_map = {'data': raw_data.decode("utf-8")}
    json_data = json.dumps(data_map)
    result = csv_processing.delay(header, json_data)
    response['id'] = result.id
    response['status'] = "processing"
    return HttpResponse(json.dumps(response))


@shared_task
def csv_processing(header, data):
    json_data = {}
    data_struct = json.loads(data)
    data_lines = data_struct['data'].splitlines()
    data_start_index = 0

    if header == 'True':
        columns = split_by_comma(data_lines[0])
        json_data['header'] = columns
        data_start_index = 1

    formatted_rows = []
    for i in range(data_start_index, len(data_lines)):
        row = data_lines[i]
        formatted_row = split_by_comma(row)
        formatted_rows.append(formatted_row)

    json_data['rows'] = formatted_rows
    return json.dumps(json_data)


def split_by_comma(string):
    items = string.split(',')
    return [x.strip() for x in items]
