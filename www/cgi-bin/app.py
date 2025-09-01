from flask import Flask, request, jsonify # type: ignore
from flask_cors import CORS# type: ignore
import json

from listDatalakes import listDatalakes
from getInfoDatalake import get_info
from getMane import get_mane
from getExomeBed import get_exome_bed
from parquet2json import parquet2json
from parquet2json_more import parquet2json_more

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)
@app.route('/listDatalakes', methods=['GET'])
def route_listDatalakes():
    try:
        data = listDatalakes()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
 
@app.route('/getInfoDatalake', methods=['GET'])
def route_getInfoDatalake():
    datalake = request.args.get('datalake')
    try:
        data = get_info(datalake)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/getMane', methods=['GET'])
def route_getMane():
    try:
        data = get_mane()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/getExomeBed', methods=['GET'])
def route_getExomeBed():
    try:
        data = get_exome_bed()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/parquet2json', methods=['GET'])
def route_parquet2json():
    datalake = request.args.get('datalake')
    chrom = request.args.get('chrom')
    start = request.args.get('start')
    end = request.args.get('end')
    try:
        data = parquet2json(datalake, chrom, start, end)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/parquet2json_more', methods=['POST'])
def route_parquet2json_more():
    try:
        data = request.get_json(force=True)
        print("Received JSON:", data)  # debug : vérifier ce qui arrive
        datalake = data.get('datalake')
        chrom = data.get('chrom')
        start = data.get('start')
        end = data.get('end')

        result = parquet2json_more(
            datalake,
            ",".join(chrom),
            ",".join(map(str, start)),
            ",".join(map(str, end))
        )
        return jsonify(result)
    except Exception as e:
        print("Error in /parquet2json_more:", e)
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
