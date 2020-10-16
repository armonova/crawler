from urllib.parse import urlparse, urljoin
from .scheduler import Scheduler
from bs4 import BeautifulSoup
from threading import Thread
import requests
import time


class PageFetcher(Thread):
    def __init__(self, obj_scheduler):
        super(PageFetcher, self).__init__()
        self.obj_scheduler = obj_scheduler

    def request_url(self, obj_url):
        """
            Faz a requisição e retorna o conteúdo em binário da URL passada como parametro
            obj_url: Instancia da classe ParseResult com a URL a ser requisitada.
        """
        url = obj_url.geturl()
        print(url)
        headers = {'user-agent': self.obj_scheduler.str_usr_agent}
        r = requests.get(url, headers=headers)
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
        for link in soup.select("a[href]"):
            obj_new_url = urlparse(link.get("href"))
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
        if next_url is None:
            time.sleep(Scheduler.TIME_LIMIT_BETWEEN_REQUESTS)
            return
        content = self.request_url(next_url[0])
        depth = next_url[1]
        urls = self.discover_links(next_url[0], depth, content)

        aux = []
        links = []
        for url in urls:
            if not url[0].netloc in aux:
                aux.append(url[0].netloc)
                links.append(url)
        for index, (url_link, depth) in enumerate(links):
            if url_link is not None:
                self.obj_scheduler.add_new_page(url_link, depth)
        return True

    def run(self):
        """
            Executa coleta enquanto houver páginas a serem coletadas
        """
        [self.obj_scheduler.add_new_page(url) for url in self.obj_scheduler.arr_urls_seeds]
        while not self.obj_scheduler.has_finished_crawl():
            self.crawl_new_url()
            self.obj_scheduler.count_fetched_page()
