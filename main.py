import urllib.request
import ssl

def run():
    # Cria um contexto SSL que aceita cifras antigas e modernas, ignorando erros de certificado
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.set_ciphers('DEFAULT@SECLEVEL=1') # Reduz o nível de exigência para compatibilidade total

    url = "https://ctctech.store/wp-admin/admin.php?page=bot-captura-ctctech&run=capture"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    }

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, context=ctx, timeout=60) as response:
            status = response.getcode()
            print(f"Status: {status}")
            print("Sucesso: Bot conectado e comando enviado ao site.")
    except Exception as e:
        print(f"Erro de Conexão Robusta: {e}")

if __name__ == "__main__":
    run()
