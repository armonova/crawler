from urllib import robotparser
from util.threads import synchronized
from collections import OrderedDict
from .domain import Domain
from urllib.parse import urlparse
import urllib.robotparser
import time


class Scheduler:
    # tempo (em segundos) entre as requisições
    TIME_LIMIT_BETWEEN_REQUESTS = 30

    def __init__(self, str_usr_agent, int_page_limit, int_depth_limit, arr_urls_seeds):
        """
            Inicializa o escalonador. Atributos:
                - `str_usr_agent`: Nome do `User agent`. Usualmente, é o nome do navegador, em nosso caso,  será o nome do coletor (usualmente, terminado em `bot`)
                - `int_page_limit`: Número de páginas a serem coletadas
                - `int_depth_limit`: Profundidade máxima a ser coletada
                - `int_page_count`: Quantidade de página já coletada
                - `dic_url_per_domain`: Fila de URLs por domínio (explicado anteriormente)
                - `set_discovered_urls`: Conjunto de URLs descobertas, ou seja, que foi extraída em algum HTML e já adicionadas na fila - mesmo se já ela foi retirada da fila. A URL armazenada deve ser uma string.
                - `dic_robots_per_domain`: Dicionário armazenando, para cada domínio, o objeto representando as regras obtidas no `robots.txt`
        """
        self.str_usr_agent = str_usr_agent
        self.int_page_limit = int_page_limit
        self.int_depth_limit = int_depth_limit
        self.int_page_count = 0

        self.dic_url_per_domain = OrderedDict()
        self.set_discovered_urls = set()
        self.dic_robots_per_domain = {}

        self.arr_urls_seeds = arr_urls_seeds

    @synchronized
    def count_fetched_page(self):
        """
            Contabiliza o número de paginas já coletadas
        """
        self.int_page_count += 1

    @synchronized
    def has_finished_crawl(self):
        """
            Verifica se finalizou a coleta
        """
        if (self.int_page_count > self.int_page_limit):
            return True
        return False

    @synchronized
    def can_add_page(self, obj_url, int_depth):
        if int_depth <= self.int_depth_limit:  # depth less than or equal
            # url = str(obj_url.scheme) + '://' + str(obj_url.netloc) + str(obj_url.path)
            url = obj_url.geturl()
            if url in self.set_discovered_urls:  # page already added
                return False
            else:
                self.set_discovered_urls.add(url)
                return True
        else:
            False

    @synchronized
    def add_new_page(self, obj_url, int_depth=0):
        domain = obj_url.netloc
        if self.can_add_page(obj_url, int_depth):
            # add url by domain
            if not Domain(obj_url.netloc, self.TIME_LIMIT_BETWEEN_REQUESTS) in self.dic_url_per_domain.keys():
                self.dic_url_per_domain[Domain(obj_url.netloc, 10)] = []
            self.dic_url_per_domain[Domain(obj_url.netloc, 10)].append((obj_url, int_depth))
            return True
        else:
            return False

    @synchronized
    def get_next_url(self):
        """
        Obtem uma nova URL por meio da fila. Essa URL é removida da fila.
        Logo após, caso o servidor não tenha mais URLs, o mesmo também é removido.
        """

        if not self.dic_url_per_domain:
            return None

        domains_to_remove = []
        url_to_remove = None
        url_depth = None

        for domain in self.dic_url_per_domain:
            if domain.is_accessible():
                if not self.dic_url_per_domain[domain]:
                    domains_to_remove.append(domain)
                    continue
                domain.accessed_now()
                url_to_remove = domain
                break

        # remove empty domains
        for domain in domains_to_remove:
            del self.dic_url_per_domain[domain]
        # remove url
        if url_to_remove:
            url_depth = self.dic_url_per_domain[url_to_remove][0]

        # # wait and call next url again if no url is provided
        if not url_depth:
            time.sleep(self.TIME_LIMIT_BETWEEN_REQUESTS + 1)
            url_depth = self.get_next_url()
        return url_depth

    def can_fetch_page(self, obj_url):
        """
        Verifica, por meio do robots.txt se uma determinada URL pode ser coletada
        """
        # url_robots = obj_url.scheme + '://' + obj_url.netloc + '/robots.txt'
        # full_url = obj_url.scheme + '://' + obj_url.netloc + obj_url.path + obj_url.params + obj_url.query + obj_url.fragment
        full_url = obj_url.geturl()
        if obj_url.netloc in self.dic_robots_per_domain:
            return False
        else:
            rp = urllib.robotparser.RobotFileParser()
            # rp.set_url(url_robots)
            rp.set_url(obj_url.geturl())
            rp.read()
            self.dic_robots_per_domain[obj_url.netloc] = rp
            return rp.can_fetch("*", full_url)
