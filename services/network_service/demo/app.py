import random
import string
from flask import Flask, make_response, jsonify

app = Flask(__name__)


@app.route('/v1/detection/jsonrpc', methods=['GET', 'POST'])
def hello_world():
    response = make_response(
        jsonify(
            {"message": ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))}
        ),
        200,
    )
    response.headers["Content-Type"] = "application/json"
    return response


if __name__ == "__main__":
    app.run(debug=True)
