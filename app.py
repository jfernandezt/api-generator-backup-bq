from flask import Flask

app = Flask(__name__)

@app.route('/health')
def health():
    return {'status': 'ok'}, 200

# El resto de tu aplicación...

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080))) 