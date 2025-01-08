from DrissionPage import Chromium
import hashlib
import sys
import pymysql
from concurrent.futures import ThreadPoolExecutor

def page_save(link, html):
    save_path = r"C:\Nirmal\Personal\page_save\blk"
    file_name = f'{save_path}\{hashlib.sha256(link.encode()).hexdigest()}.html'
    with open(file_name, 'wb') as file:
        file.write(html.encode())
    print(f"Saved page to: {file_name}")

def fetch_page(tab, urls):
    tab.set.blocked_urls('*.css*')
    tab.set.blocked_urls('*.jpg*')
    tab.set.blocked_urls('*.webp*')
    tab.set.blocked_urls('*.js*')
    tab.set.blocked_urls('*.png*')
    tab.set.blocked_urls('*.jpeg*')

    for url in urls:
        tab.get(url)
        page_save(url, tab.html)

def fetch_all(browser, urls, num_tabs):
    url_batches = [urls[i::num_tabs] for i in range(num_tabs)]

    tabs = [browser.new_tab() for _ in range(num_tabs)]

    with ThreadPoolExecutor(max_workers=num_tabs) as executor:
        for i in range(num_tabs):
            executor.submit(fetch_page, tabs[i], url_batches[i])

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Usage: python script.py <port> <start_id> <end_id> <num_tabs>")
        sys.exit(1)

    port = sys.argv[1]
    start_id = sys.argv[2]
    end_id = sys.argv[3]
    try:
        num_tabs = int(sys.argv[4])
    except ValueError:
        print("num_tabs must be an integer.")
        sys.exit(1)

    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='actowiz',
        database='world_postal_codes'
    )

    cur = conn.cursor()

    cur.execute(f'SELECT url FROM pincode_urls WHERE id BETWEEN {start_id} AND {end_id}')
    results = cur.fetchall()

    urls = [row[0] for row in results]

    browser = Chromium(f'127.0.0.1:{port}')

    fetch_all(browser, urls, num_tabs)
