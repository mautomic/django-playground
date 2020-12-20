from django.http import HttpResponse, HttpResponseBadRequest


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
    raw_data = request.body
    if raw_data is not None:
        data = raw_data.decode("utf-8")
        return HttpResponse("Submitting csv to task queue with data: " + data)
    return HttpResponseBadRequest("Please send a post request with proper csv data")
