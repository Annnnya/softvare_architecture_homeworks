from flask import Flask
app = Flask(__name__)

@app.route('/messages')
def return_static_message():
    return 'not implemented yet'

if __name__ == '__main__':
    app.run(port=5002)
