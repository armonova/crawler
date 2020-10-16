from bs4 import BeautifulSoup
from threading import Thread
import requests
from urllib.parse import urlparse, urljoin


class PageFetcher(Thread):
    def __init__(self, obj_scheduler):
        self.obj_scheduler = obj_scheduler

    def request_url(self, obj_url):
        """
            Faz a requisição e retorna o conteúdo em binário da URL passada como parametro
            obj_url: Instancia da classe ParseResult com a URL a ser requisitada.
        """
        url = obj_url.scheme + '://' + obj_url.netloc + obj_url.path + obj_url.params + obj_url.query + obj_url.fragment
        headers = {'user-agent': 'arthur-crawler'}
        r = requests.get(url, headers=headers)
        r.status_code
        content = r.headers['content-type']
        if 'html' in content:
            response = r
            return response.content
        else:
            return None

    def discover_links(self, obj_url, int_depth, bin_str_content):
        """
        Retorna os links do conteúdo bin_str_content da página já requisitada obj_url
        """
        soup = BeautifulSoup(bin_str_content, features="lxml")
        print('\n')
        print(soup)
        print('\n')
        for link in soup.select("a[href]"):
            obj_new_url = urlparse(link.get("href"))
            print(link.get("href"))
            if obj_new_url.netloc == obj_url.netloc:
                int_new_depth = int_depth + 1
            else:
                int_new_depth = 0
            yield obj_new_url, int_new_depth

    def crawl_new_url(self):
        """
            Coleta uma nova URL, obtendo-a do escalonador
        """
        next_url = self.obj_scheduler.get_next_url()
        content = self.request_url(next_url)
        print(content)
        depth = 3
        self.discover_links(next_url, depth, content)
        pass

    def run(self):
        """
            Executa coleta enquanto houver páginas a serem coletadas
        """
        pass
