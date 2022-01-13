
import requests

def get_public_ip() -> str:
    url = 'https://api.ipify.org'
    response = requests.get(url)

    if response.status_code != 200:
        raise RuntimeError('API request failed.')

    return response.text

def main():
    print(f'Your public IP is {get_public_ip()}')

if __name__ == '__main__':
    main()
