from flask import Flask, jsonify, render_template, request, flash, make_response
from werkzeug.security import generate_password_hash, check_password_hash
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
import logging
import uuid # for public id
import pandas as pd
import duckdb
import utils
# imports for PyJWT authentication
import jwt
import time
from datetime import datetime, timedelta
import os
from functools import wraps


DB_PATH = utils.DB_PATH
PATH_BUCKET = utils.PATH_BUCKET

app = Flask(__name__) # to make the app run without any
# Configuração JWT
app.config["JWT_SECRET_KEY"] = utils.SECRET_KEY  # Altere para uma chave secreta segura
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(minutes=30)  # Sessão expira após 30 minutos
# Configuração SQLite
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///Database.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
#jwt = JWTManager(app)

# creates SQLALCHEMY object
db = SQLAlchemy(app)
#db.init_app(app)
CORS(app)

# Database ORMs
class User(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    public_id = db.Column(db.String(100))
    name = db.Column(db.String(100))
    email = db.Column(db.String(70), unique = True)
    password = db.Column(db.String(80))
    

# decorator for verifying the JWT
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        # jwt is passed in the request header
        if 'x-access-token' in request.headers:
            token = request.headers['x-access-token']
        # return 401 if token is not passed
        if not token:
            return jsonify({'message' : 'Token is missing !!'}), 401
  
        try:
            # decoding the payload to fetch the stored details
            data = jwt.decode(token, app.config['SECRET_KEY'])
            current_user = User.query\
                .filter_by(public_id = data['public_id'])\
                .first()
        except:
            return jsonify({
                'message' : 'Token is invalid !!'
            }), 401
        # returns the current logged in users context to the routes
        return  f(current_user, *args, **kwargs)
  
    return decorated
  
# User Database Route
# this route sends back list of users
@app.route('/user', methods =['GET'])
@token_required
def get_all_users(current_user):
    # querying the database
    # for all the entries in it
    users = User.query.all()
    # converting the query objects
    # to list of jsons
    output = []
    for user in users:
        # appending the user data json 
        # to the response list
        output.append({
            'public_id': user.public_id,
            'name' : user.name,
            'email' : user.email
        })
  
    return jsonify({'users': output})

# Usuários de exemplo (somente para teste)
#users = {"usuario": "asd"}

@app.route("/auth", methods=["POST"])
def auth():
    username = request.json.get("firstname", None)
    password = request.json.get("password", None)
    if username not in users or users[username] != password:
        return jsonify({"error": "Usuário ou senha incorretos"}), 401
    
    # Criação do token JWT
    access_token = create_access_token(identity=username)
    return jsonify(access_token=access_token)
    
@app.route('/')
def index():
    return render_template("home.html")

@app.route('/about')
@jwt_required()
def about():
    return render_template('about.html')

@app.route('/auth/signup', methods=['GET'])
def signup_auth():
    return render_template("signup.html")

@app.route('/auth/login', methods=['GET'])
def login_auth():
    return render_template("login.html")

# route for logging user in
@app.route('/login', methods =['POST'])
def login():
    # creates dictionary of form data
    data = request.form
  
    if not data or not data.get('email') or not data.get('password'):
        # returns 401 if any email or / and password is missing
        return make_response(
            'Could not verify',
            401,
            {'WWW-Authenticate' : 'Basic realm ="Login required !!"'}
        )
  
    user = User.query\
        .filter_by(email = data.get('email'))\
        .first()
  
    if not user:
        # returns 401 if user does not exist
        return make_response(
            'Could not verify',
            401,
            {'WWW-Authenticate' : 'Basic realm ="User does not exist !!"'}
        )
  
    if check_password_hash(user.password, data.get('password')):
        # generates the JWT Token
        token = jwt.encode({
            'public_id': user.public_id,
            'exp' : datetime.now() + timedelta(minutes = 30)
        }, app.config['SECRET_KEY'])
  
        return make_response(jsonify({'token' : token.decode('UTF-8')}), 201)
    # returns 403 if password is wrong
    return make_response(
        'Could not verify',
        403,
        {'WWW-Authenticate' : 'Basic realm ="Wrong Password !!"'}
    )
  
# signup route
@app.route('/signup', methods =['POST'])
def signup():
    # creates a dictionary of the form data
    data = request.form
  
    # gets name, email and password
    name, email = data.get('firstname'), data.get('email')
    password = data.get('password')
  
    # checking for existing user
    user = User.query\
        .filter_by(email = email)\
        .first()
    if not user:
        # database ORM object
        user = User(
            public_id = str(uuid.uuid4()),
            name = name,
            email = email,
            password = generate_password_hash(password)
        )
        # insert user
        db.session.add(user)
        db.session.commit()
  
        return make_response('Successfully registered.', 201)
    else:
        # returns 202 if user already exists
        return make_response('User already exists. Please Log in.', 202)



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
    #app.run(host='0.0.0.0', port=3000, debug=True)
