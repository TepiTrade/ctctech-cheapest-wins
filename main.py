import cloudscraper

def run():
    # O cloudscraper simula um navegador real e ignora bloqueios de SSL/V3
    scraper = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'desktop': True
        }
    )
    
    url = "https://ctctech.store/wp-admin/admin.php?page=bot-captura-ctctech&run=capture"
    
    try:
        # verify=False e a emulação do Chrome forçam a entrada no servidor
        response = scraper.get(url, timeout=60, verify=False)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            print("Sucesso: O servidor aceitou a conexão do Bot!")
        else:
            print(f"Aviso: O servidor respondeu, mas com status {response.status_code}")
    except Exception as e:
        print(f"Erro de Conexão: {e}")

if __name__ == "__main__":
    run()
