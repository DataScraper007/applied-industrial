import os
from DrissionPage import Chromium, ChromiumOptions
import hashlib
import sys
import pymysql
from concurrent.futures import ThreadPoolExecutor

co = ChromiumOptions()
co.incognito()
co.set_argument('--no-sandbox')

def page_save(conn, page_id, link, html):
    file_name = f'{save_path}\{hashlib.sha256(link.encode()).hexdigest()}.html'
    
    # Save the HTML content to a file
    with open(file_name, 'wb') as file:
        file.write(html.encode())
    print(f"Saved page to: {file_name}")
    
    # # Update the status in the database (set 'status' to 'done' for the given page_id)
    # try:
    #     cur = conn.cursor()
    #     cur.execute(f"UPDATE links SET status = 'done' WHERE id = %s", (page_id,))
    #     conn.commit()
    #     print(f"Updated status for page id: {page_id}")
    # except Exception as e:
    #     print(f"Error updating database: {e}")

def fetch_page(tab, urls, conn):
    tab.set.blocked_urls('*.css*')
    tab.set.blocked_urls('*.jpg*')
    tab.set.blocked_urls('*.webp*')
    # tab.set.blocked_urls('*.js*')
    tab.set.blocked_urls('*.png*')
    tab.set.blocked_urls('*.jpeg*')
    
    for url, page_id in urls:
        if not os.path.exists(f'{save_path}\{hashlib.sha256(url.encode()).hexdigest()}.html'):
            tab.get(url)
            page_save(conn, page_id, url, tab.html)
        else:
            print('Skipping..')

def fetch_all(browser, urls, conn, num_tabs):
    # Split the URLs into batches with their corresponding IDs
    url_batches = [urls[i::num_tabs] for i in range(num_tabs)]

    tabs = [browser.new_tab() for _ in range(num_tabs)]

    with ThreadPoolExecutor(max_workers=num_tabs) as executor:
        for i in range(num_tabs):
            executor.submit(fetch_page, tabs[i], url_batches[i], conn)

if __name__ == '__main__':
    save_path = r"C:\Nirmal\R&D\page_save\applied"
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
        database='applied_com'
    )

    cur = conn.cursor()

    # Fetch the links and their IDs from the database
    cur.execute(f'SELECT id, url FROM links WHERE id BETWEEN {start_id} AND {end_id} and status="pending"')
    results = cur.fetchall()

    # Prepare a list of tuples (url, id)
    urls = [(row[1], row[0]) for row in results]  # Extracting URL and ID from query results

    browser = Chromium(f'127.0.0.1:{port}', co)

    # Fetch all pages and save them
    fetch_all(browser, urls, conn, num_tabs)
