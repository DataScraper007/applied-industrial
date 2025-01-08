import hashlib
from DrissionPage import Chromium

def page_save(link, html):
    save_path = r"C:\Nirmal\Personal\page_save"
    with open(f'{save_path}\{hashlib.sha256(link.encode()).hexdigest()}.html', 'wb') as file:
        file.write(html.encode())
    print(f"Save page to: {save_path}")

browser = Chromium('127.0.0.1:9331')

# Get the latest tab
tab = browser.latest_tab


def fetch(url):
    tab.get(url)
    elements = tab.eles('x=//article[@class="product_pod"]//a')

    links = [ele.attr('href') for ele in elements]
    next_page = tab.ele('x=//li[@class="next"]//a').attr('href')

    for link in links:
        tab.get(link)
        page_save(link, tab.html)

    if next_page:
        fetch(next_page)


fetch('https://books.toscrape.com/')

