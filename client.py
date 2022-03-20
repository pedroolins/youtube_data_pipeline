from urllib import response
import requests

json = {
    'msg': 'pantanal'
}

res = requests.post('http://localhost:5000/teste', json=json)
print(res.status_code)