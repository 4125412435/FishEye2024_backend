from flask import Flask
import service

app = Flask(__name__)
service.initialize('./data/MC2/mc2.json')


@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'


if __name__ == '__main__':
    app.run()
