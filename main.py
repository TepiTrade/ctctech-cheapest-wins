import requests

def run():
    url = "https://ctctech.store/wp-admin/admin.php?page=bot-captura-ctctech&run=capture"
    try:
        response = requests.get(url, timeout=30)
        print(f"Status: {response.status_code}")
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    run()
