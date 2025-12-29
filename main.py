import urllib3

def run():
    # Forçamos o urllib3 a ignorar completamente o SSL e não tentar redirecionar para HTTPS
    http = urllib3.PoolManager(cert_reqs='CERT_NONE', assert_hostname=False)
    
    # Usamos o IP ou o domínio direto em HTTP simples
    url = "http://ctctech.store/wp-admin/admin.php?page=bot-captura-ctctech&run=capture"
    
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Connection': 'close'
    }

    try:
        # O parâmetro redirect=False impede que o site te jogue de volta para o erro de SSL
        response = http.request('GET', url, headers=headers, timeout=30.0, redirect=False)
        print(f"Status: {response.status}")
        print("Conexão estabelecida com sucesso via HTTP.")
    except Exception as e:
        print(f"Erro detectado: {e}")

if __name__ == "__main__":
    run()
