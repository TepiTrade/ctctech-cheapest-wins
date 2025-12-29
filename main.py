import requests
import urllib3

# Desabilita avisos de certificados inseguros
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def run():
    url = "https://ctctech.store/wp-admin/admin.php?page=bot-captura-ctctech&run=capture"
    try:
        # verify=False ignora o erro de handshake do SSL
        response = requests.get(url, timeout=60, verify=False)
        print(f"Status: {response.status_code}")
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    run()
