from django.http import HttpResponse, HttpResponseBadRequest
import json


def handler(request, id=None):
    if request.method == 'GET':
        return get_table(id)
    elif request.method == 'POST':
        return post_table(request)


def get_table(id):
    if id is not None:
        return HttpResponse("Will query for json with table id: " + id)
    return HttpResponseBadRequest("Please send a request with a table id, ie. /table/{xyz}")


def post_table(request):

    header = request.GET.get('headerRow', 'False')
    raw_data = request.body

    if raw_data is None:
        return HttpResponseBadRequest("Please send a post request with proper csv data")

    json_data = {}

    data = raw_data.decode("utf-8")
    data_lines = data.splitlines()
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
    return HttpResponse(json.dumps(json_data))


def split_by_comma(string):
    items = string.split(',')
    return [x.strip() for x in items]
