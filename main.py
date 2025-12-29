import http.client
import ssl

def run():
    context = ssl._create_unverified_context()
    conn = http.client.HTTPSConnection("ctctech.store", context=context)
    try:
        conn.request("GET", "/wp-admin/admin.php?page=bot-captura-ctctech&run=capture")
        response = conn.getresponse()
        print(f"Status: {response.status} {response.reason}")
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run()
