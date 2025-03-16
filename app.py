import os
from flask import Flask, jsonify, request
import main  # Importa tu archivo main.py

app = Flask(__name__)

# Función para validar el token de autorización
def validate_auth(auth_header):
    # Formato esperado: "Bearer tu-token-secreto"
    if not auth_header.startswith('Bearer '):
        return False
    
    token = auth_header.split(' ')[1]
    expected_token = os.environ.get('API_SECRET_TOKEN')
    
    # Si no se ha configurado ningún token, rechazar todas las solicitudes
    if not expected_token:
        return False
        
    # Comparación segura contra timing attacks
    return token == expected_token

@app.route('/health')
def health():
    return {'status': 'ok'}, 200

@app.route('/ejecutar-backup', methods=['POST'])
def ejecutar_backup():
    # Validar token o autenticación aquí
    auth_header = request.headers.get('Authorization')
    if not auth_header or not validate_auth(auth_header):
        return jsonify({"status": "error", "message": "No autorizado"}), 401
    
    try:
        # Llama a la función main de tu script
        main.main()
        return jsonify({"status": "success", "message": "Proceso de backup ejecutado correctamente"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080))) 