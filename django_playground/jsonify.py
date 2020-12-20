from django.http import HttpResponse


def response(request):
    message = "Im going to jsonify whatever csv comes here"
    name = request.GET.get('name', '')
    if name is not '':
        message += " for " + name
    return HttpResponse(message)
