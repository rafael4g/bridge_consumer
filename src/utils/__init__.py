from decouple import config

DB_PATH = config('DB_PATH')
PATH_BUCKET = config('PATH_BUCKET')
SECRET_KEY = config('SECRET_KEY')
JWT_EXPIRATION_DELTA = config('JWT_EXPIRATION_DELTA')
