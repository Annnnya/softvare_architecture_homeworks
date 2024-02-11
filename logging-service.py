from flask import Flask, request
app = Flask(__name__)

messages = {}

@app.route('/log', methods=['POST', 'GET'])
def handle_requests():
    if request.method == 'POST':
        data = request.json
        uuid = data['uuid']
        msg = data['msg']
        messages[uuid] = msg
        print(f"Received message: {msg}")
        return "Message logged successfully"
    elif request.method == 'GET':
        return "\n".join(messages.values())

if __name__ == '__main__':
    app.run(port=5001)
