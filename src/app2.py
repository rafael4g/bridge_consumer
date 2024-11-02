from flask import Flask, jsonify, render_template, request, flash
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
import logging
import pandas as pd
import duckdb
import utils
import time
from datetime import datetime, timedelta

app = Flask(__name__)
app.secret_key='1234'
# Configuração JWT
app.config["JWT_SECRET_KEY"] = "sua_chave_secreta"  # Altere para uma chave secreta segura
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(minutes=30)  # Sessão expira após 30 minutos
jwt = JWTManager(app)

DB_PATH = utils.DB_PATH
PATH_BUCKET = utils.PATH_BUCKET

# Usuários de exemplo (somente para teste)
users = {"usuario": "asd"}

@app.route("/login", methods=["POST"])
def login():
    username = request.json.get("username", None)
    password = request.json.get("password", None)
    if username not in users or users[username] != password:
        return jsonify({"error": "Usuário ou senha incorretos"}), 401
    
    # Criação do token JWT
    access_token = create_access_token(identity=username)
    return jsonify(access_token=access_token)

@app.route('/')
def index():
    return render_template("index.html")

@app.route('/upload', methods=['POST'])
@jwt_required()  # Requer autenticação JWT
def upload_file():
    file = request.files.get('file')
    if not file:
        logging.error("Nenhum arquivo foi enviado.")
        return jsonify({"error": "Nenhum arquivo enviado"}), 400
    
    logging.error("---------------Arquivo encontrado")
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
