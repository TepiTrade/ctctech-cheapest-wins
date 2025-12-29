import ssl
import socket
import http.client

def run():
    # Força o uso de protocolos de segurança modernos para evitar o Handshake Failure
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    context.set_ciphers('DEFAULT@SECLEVEL=1')

    try:
        conn = http.client.HTTPSConnection("ctctech.store", port=443, context=context, timeout=30)
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        conn.request("GET", "/wp-admin/admin.php?page=bot-captura-ctctech&run=capture", headers=headers)
        
        response = conn.getresponse()
        print(f"Status: {response.status} {response.reason}")
    except Exception as e:
        print(f"Erro de Conexao: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run()
