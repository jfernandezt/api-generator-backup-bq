import os
from flask import Flask, jsonify
import main  # Importa tu archivo main.py

app = Flask(__name__)

@app.route('/health')
def health():
    return {'status': 'ok'}, 200

@app.route('/ejecutar-backup', methods=['POST'])
def ejecutar_backup():
    try:
        # Llama a la función main de tu script
        main.main()
        return jsonify({"status": "success", "message": "Proceso de backup ejecutado correctamente"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# El resto de tu aplicación...

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080))) 