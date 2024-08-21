import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

def ping_api(url):
    try:
        start_time = time.time()
        response = requests.get(url)
        end_time = time.time()
        return {
            'status_code': response.status_code,
            'response_time': end_time - start_time
        }
    except requests.RequestException as e:
        return {
            'status_code': None,
            'response_time': None,
            'error': str(e)
        }

def main():
    start_time = time.time()

    url = "https://etherfi-dot-internal-website-427620.uc.r.appspot.com/user_balances?blockNumber=8438970&addresses=0x0347F0867b9B28a34fE6c044c4d48C2E5F71C528"
    num_requests = 50

    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(ping_api, url): url for _ in range(num_requests)}
        for future in as_completed(future_to_url):
            results.append(future.result())

    successful_requests = sum(1 for result in results if result['status_code'] == 200)
    failed_requests = sum(1 for result in results if result['status_code'] != 200)
    total_response_time = sum(result['response_time'] for result in results if result['response_time'] is not None)
    avg_response_time = total_response_time / successful_requests if successful_requests > 0 else 0

    print(f"Total requests: {num_requests}")
    print(f"Successful requests: {successful_requests}")
    print(f"Failed requests: {failed_requests}")
    print(f"Average response time: {avg_response_time:.4f} seconds")

    end_time = time.time()

    print('Finished in: ', end_time - start_time)
    
if __name__ == "__main__":
    main()