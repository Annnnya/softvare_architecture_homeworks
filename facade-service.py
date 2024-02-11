from flask import Flask, request
import requests
import uuid

app = Flask(__name__)

logging_service_url = "http://localhost:5001"
messages_service_url = "http://localhost:5002"

@app.route('/facade-service', methods=['POST', 'GET'])
def handle_requests():
    if request.method == 'POST':
        msg = request.json['msg']
        msg_id = str(uuid.uuid4())
        data = {'uuid': msg_id, 'msg': msg}
        requests.post(logging_service_url+'/log', json=data)
        return msg_id
    elif request.method == 'GET':
        logging_response = requests.get(logging_service_url+'/log').text
        messages_response = requests.get(messages_service_url+'/messages').text
        return logging_response + " " + messages_response

if __name__ == '__main__':
    app.run(port=5000)
