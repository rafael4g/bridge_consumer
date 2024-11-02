# Projeto de Importação com Flask

Este projeto, desenvolvido em Python com o framework Flask, permite a importação de arquivos do tipo CSV e XLSX contendo dados dimensionais que serão salvos no banco de dados DuckDB. O projeto possui autenticação via JWT, com um limite de sessão de 30 minutos, após o qual o usuário é automaticamente deslogado.

## Funcionalidades

- Importação de arquivos CSV e XLSX contendo dimensões.
- Persistência dos dados em banco de dados DuckDB.
- Autenticação JWT com controle de tempo de sessão.
- Logout automático após 30 minutos de inatividade.

## Tecnologias Utilizadas

- Python 3.10.12
- Flask
- DuckDB
- JWT para autenticação segura

## Instalação

1. Clone o repositório:

    ```bash
    git clone https://github.com/rafael4g/bridge_consumer.git
    cd bridge_consumer
    ```

2. Crie um ambiente virtual e ative-o:

    ```bash
    python -m venv venv
    source venv/bin/activate  # Para Linux/Mac
    venv\Scripts\activate     # Para Windows
    ```

3. Instale as dependências:

    ```bash
    pip install -r requirements.txt
    ```

## Configuração

1. Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

    ```env
    SECRET_KEY="sua_chave_secreta"
    JWT_EXPIRATION_DELTA=1800  # Tempo de sessão em segundos (30 minutos)
    ```

2. Configure o caminho para o banco de dados DuckDB, conforme necessário.

## Uso

1. Inicie o servidor Flask:

    ```bash
    flask run
    ```

2. Acesse o aplicativo em [http://localhost:5000](http://localhost:5000).

### Rotas Principais

- **`POST /login`** - Realiza a autenticação do usuário, gerando um token JWT.
- **`POST /import`** - Permite a importação de arquivos CSV e XLSX (necessita de autenticação).
- **`GET /logout`** - Finaliza a sessão do usuário, invalidando o token.

### Exemplos de Uso

- Para acessar as rotas protegidas, envie o token JWT no cabeçalho `Authorization`:

    ```http
    Authorization: Bearer <seu_token_jwt>
    ```

## Segurança

Este projeto utiliza o JWT para autenticação e mantém o usuário logado por até 30 minutos de inatividade. Após esse período, o usuário precisará se autenticar novamente.

## Contribuição

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou pull requests.

## Licença

Este projeto está licenciado sob a MIT License.
