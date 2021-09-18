import requests

r = requests.post('http://localhost:8000', data={'key': 'value'})
print(r.status_code)
