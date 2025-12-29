import os

def run():
    # Comando direto de sistema (cURL) que ignora totalmente erros de SSL e Handshake
    url = "https://ctctech.store/wp-admin/admin.php?page=bot-captura-ctctech&run=capture"
    
    # -k ignora o certificado SSL, -L segue redirecionamentos, -s fica em silêncio
    comando = f'curl -k -L -s -o /dev/null -w "%{{http_code}}" "{url}"'
    
    status = os.popen(comando).read()
    
    if status == "200":
        print(f"Status: {status} - SUCESSO! O servidor aceitou o comando.")
    else:
        print(f"Status: {status} - O servidor respondeu, verifique o site.")

if __name__ == "__main__":
    run()
