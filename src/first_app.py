from flask import Flask, render_template, request, jsonify, flash
import logging
import pandas as pd
from datetime import datetime
import duckdb
import uuid
import utils
import os

app = Flask(__name__)
app.secret_key='1234'
DB_PATH = utils.DB_PATH
PATH_BUCKET = utils.PATH_BUCKET

# Rota principal para renderizar a página
@app.route('/')
def index():
    return render_template("index.html")

# Rota para receber e processar o upload do arquivo
@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files.get('file')
    if not file:
        logging.error("Nenhum arquivo foi enviado.")
        return jsonify({"error": "Nenhum arquivo enviado"}), 400
    flash('---------------Arquivo encontrado')
    # Gera um código único de importação
    #import_code = str(uuid.uuid4().hex[:8])
    import_code = int(datetime.now().timestamp())

    # Carrega o arquivo em um DataFrame do Pandas
    try:
        if file.filename.endswith('.csv'): 
                     
            name_file = file.filename       
           
            data = pd.read_csv(file, sep=';', encoding='ISO-8859-1')
            data['name_file_controller'] = name_file

            path_name_parquet = f'{PATH_BUCKET}/bronze/{import_code}.parquet'
            data.to_parquet(path_name_parquet, compression='snappy')

        elif file.filename.endswith(('.xls', '.xlsx')):
            data = pd.read_excel(file)
            data['uuid'] = import_code
        else:
            logging.error("Formato de arquivo inválido: %s", file.filename)
            return jsonify({"error": "Formato de arquivo inválido"}), 400

        # Verifica se as colunas esperadas estão presentes
        #expected_columns = {"id", "cpf", "nome", "canal"}
        #if not expected_columns.issubset(data.columns):
        #    logging.error("Colunas faltando no arquivo: %s", data.columns)
        #    return jsonify({"error": "Colunas esperadas não encontradas no arquivo"}), 400

        # Salva os dados no banco DuckDB
        with duckdb.connect(DB_PATH) as conn:

            flash('---------------aqui-----------------')
            conn.execute('create schema if not exists s1')
            conn.execute('drop table if exists s1.tbl_teste')
            conn.execute(f"create table s1.tbl_teste as SELECT * FROM '{path_name_parquet}'")
           
            #conn.execute("CREATE TABLE IF NOT EXISTS dados (id INTEGER, cpf TEXT, nome TEXT, canal TEXT, import_code TEXT)")
            #conn.execute("INSERT INTO dados SELECT *, ? FROM data", [import_code], data=data)
            logging.info("Dados inseridos com sucesso com o código de importação %s", import_code)

    except Exception as e:
        logging.error("Erro ao processar o arquivo: %s", str(e))
        return jsonify({"error": str(e)}), 500

    return jsonify({"import_code": import_code})

if __name__ == "__main__":
    app.run(debug=True)
