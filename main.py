import urllib3

def run():
    # Usando PoolManager para gerenciar a requisição de forma simples
    http = urllib3.PoolManager()
    
    # Mudamos para http:// para evitar o erro de SSL/Handshake do seu servidor
    url = "http://ctctech.store/wp-admin/admin.php?page=bot-captura-ctctech&run=capture"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Host': 'ctctech.store'
    }

    try:
        # Aumentamos o timeout para garantir que o servidor processe a captura
        response = http.request('GET', url, headers=headers, timeout=60.0)
        print(f"Status: {response.status}")
        print(f"Sucesso: O comando foi entregue ao site.")
    except Exception as e:
        print(f"Erro ao conectar via HTTP: {e}")

if __name__ == "__main__":
    run()
