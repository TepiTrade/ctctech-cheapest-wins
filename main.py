import urllib3

def run():
    # Cria um gerenciador de conexões que ignora erros de certificado e força TLS moderno
    http = urllib3.PoolManager(
        cert_reqs='CERT_NONE',
        assert_hostname=False
    )
    
    url = "https://ctctech.store/wp-admin/admin.php?page=bot-captura-ctctech&run=capture"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': '*/*'
    }

    try:
        response = http.request('GET', url, headers=headers, timeout=30.0)
        print(f"Status: {response.status} {response.reason}")
        print(f"Resposta: {response.data.decode('utf-8')[:100]}")
    except Exception as e:
        print(f"Erro de Conexao Final: {e}")

if __name__ == "__main__":
    run()
