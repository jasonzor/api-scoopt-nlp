import requests
from bs4 import BeautifulSoup

def fixTypo(target):
    r = requests.get("https://bing.com/search", params={"q": target})
    pageSoup = BeautifulSoup(r.content, 'html.parser')
    print(pageSoup.prettify())

fixTypo("appl")