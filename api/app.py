import logging
from flask import Flask
from flask import abort
from flask import request
from flask import jsonify
from flask_cors import CORS

from models.saturn_table import SaturnTable
from models.de_identification_table import DeIdentificationTable
from models.swing_table import SwingTable
from models.swing_pk_table import SwingPKTable

from initialize import init_db

init_db()

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False
CORS(app)

if __name__ != "__main__":
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)


@app.route("/", methods=["GET"])
def default_route():
    print("Default Access")
    return "", 204


@app.route("/v1/saturn_tables", methods=["POST"])
def post_saturn_table():
    if not request.json:
        abort(400)
    table = SaturnTable(**request.json)
    try:
        SaturnTable.add(table)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return "", 201


@app.route("/v1/saturn_tables", methods=["GET"])
def get_saturn_tables():
    tables = SaturnTable.get_all()
    return jsonify(tables), 200


@app.route("/v1/saturn_tables/<string:table_id>", methods=["DELETE"])
def delete_saturn_table(table_id):
    SaturnTable.delete(table_id)
    return "", 202


@app.route("/v1/saturn_tables/<string:table_id>", methods=["PATCH"])
def update_saturn_table(table_id):
    r = SaturnTable.update(table_id, request.json)
    return jsonify(r), 204


# de-identification
@app.route("/v1/de_identifications", methods=["POST"])
def post_de_identification():
    if not request.json:
        abort(400)
    table = DeIdentificationTable(**request.json)
    try:
        DeIdentificationTable.add(table)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return "", 201


@app.route("/v1/de_identifications", methods=["GET"])
def get_de_identification_tables():
    tables = DeIdentificationTable.get_all()
    return jsonify(tables), 200


@app.route("/v1/de_identifications/<string:table_id>", methods=["GET"])
def get_de_identification_table(table_id):
    tables = DeIdentificationTable.get_table(table_id)
    return jsonify(tables), 200


@app.route("/v1/de_identifications/<string:table_id>", methods=["DELETE"])
def delete_de_identification_table(table_id):
    DeIdentificationTable.delete(table_id)
    return "", 202


@app.route("/v1/de_identifications/<string:table_id>", methods=["PATCH"])
def update_de_identification_table(table_id):
    r = DeIdentificationTable.update(table_id, request.json)
    return jsonify(r), 204


# swing mapping table
@app.route("/v1/swing_table", methods=["POST"])
def post_swing_table():
    if not request.json:
        abort(400)
    table = SwingTable(**request.json)
    try:
        SwingTable.add(table)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return "", 201


@app.route("/v1/swing_table", methods=["GET"])
def get_swing_tables():
    tables = SwingTable.get_all()
    return jsonify(tables), 200


@app.route("/v1/swing_table/<string:table_id>", methods=["GET"])
def get_swing_table(table_id):
    tables = SwingTable.get_table(table_id)
    return jsonify(tables), 200


@app.route("/v1/swing_table/<string:table_id>", methods=["DELETE"])
def delete_swing_table(table_id):
    SwingTable.delete(table_id)
    return "", 202


@app.route("/v1/swing_table/<string:table_id>", methods=["PATCH"])
def update_swing_table(table_id):
    r = SwingTable.update(table_id, request.json)
    return jsonify(r), 204


@app.route("/v1/swing_rowkey/<string:table_id>", methods=["GET"])
def get_swing_rowkey(table_id):
    r = SwingTable.get_rowkey(table_id=table_id)
    return jsonify(r), 200


# swing pk table
@app.route("/v1/swing_pk", methods=["POST"])
def post_swing_pk_table():
    if not request.json:
        abort(400)
    table = SwingPKTable(**request.json)
    try:
        SwingPKTable.add(table)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return "", 201


@app.route("/v1/swing_pk", methods=["GET"])
def get_swing_pk_tables():
    tables = SwingPKTable.get_all()
    return jsonify(tables), 200


@app.route("/v1/swing_pk/<string:table_id>", methods=["GET"])
def get_swing_pk_table(table_id):
    tables = SwingPKTable.get_pk_table(table_id)
    return jsonify(tables), 200


@app.route("/v1/swing_pk/<string:table_id>", methods=["DELETE"])
def delete_swing_pk_table(table_id):
    SwingPKTable.delete(table_id)
    return "", 202


@app.route("/v1/swing_pk/<string:table_id>", methods=["PATCH"])
def update_swing_pk_table(table_id):
    r = SwingPKTable.update(table_id, request.json)
    return jsonify(r), 204
