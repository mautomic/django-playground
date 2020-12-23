from celery import shared_task
from django.http import HttpResponse, HttpResponseBadRequest
from redis import BlockingConnectionPool
from redis.client import Redis
import json
import requests
import urllib.parse

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
    if raw_bytes is None:
        return HttpResponse(json.dumps({"status": "not found"}))
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
    json_data = json.dumps(data_map, ensure_ascii=False)
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
    date_index = 0

    if header == 'True':
        columns = split_by_comma(data_lines[0])
        json_data['header'] = columns
        for i in range(0, len(columns)):
            if columns[i].lower() == "date":
                date_index = i
                break
        data_start_index = 1

    formatted_rows = []
    temporals = []
    for i in range(data_start_index, len(data_lines)):
        row = data_lines[i]
        formatted_row = split_by_comma(row)

        query_param = urllib.parse.quote(formatted_row[date_index])
        temporal_response = requests.get("https://stanford-public.alkymi.cloud/getTemporals",
                                         {"text": query_param})

        if temporal_response.text != "[]":
            temporals.append(json.loads(temporal_response.text)[0])

        # TODO: if we don't know which column is the date, to retrieve temporal details, we need to scan the first row

        formatted_rows.append(formatted_row)

    json_data['rows'] = formatted_rows
    json_data['temporals'] = temporals
    return json_data


def split_by_comma(string):
    items = string.split(',')
    return [x.strip() for x in items]
