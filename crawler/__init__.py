from datetime import datetime
from .page_fetcher import *
from .scheduler import *
from multiprocessing import Process
import timeit

if __name__ == "__main__":
    arr_urls_seeds = [
        "https://www.uol.com.br/",
        "https://www.globo.com/",
        "https://www.terra.com.br/",
        "https://www.msn.com/pt-br",
        "https://www.techtudo.com.br/",
        "https://canaltech.com.br/",
        "https://github.com/",
        "https://docs.python.org/3/",
        "https://pt.wix.com/",
        "https://br.wordpress.com/",
        "https://www.pathofexile.com/",
        "https://www.reddit.com",
        "https://www.r7.com/",
        "https://www.twitch.tv",
        "https://www.estadao.com.br/",
        "https://www.ig.com.br/",
        "https://www.portaldoholanda.com.br/",
        "https://www.gazetadopovo.com.br/",
        "https://www.naosalvo.com.br/",
        "https://www.Facebook.com",
        "https://www.Baidu.com",
        "https://www.Wikipedia.org",
        "https://www.Yahoo.com",
        "https://www.Qq.com",
        "https://www.Taobao.com",
        "https://www.Twitter.com",
        "https://www.Amazon.com",
        "https://www.Google.co.jp",
        "https://www.Tmall.com",
        "https://www.Vk.com",
    ]

    arr_urls_seeds = [urlparse(str_url) for str_url in arr_urls_seeds]

    scheduler = Scheduler(str_usr_agent="armonova",
                          int_page_limit=50000,
                          int_depth_limit=6,
                          arr_urls_seeds=arr_urls_seeds)

    fetchers = []
    pages_fetchers = []
    num_of_threads = 100

    d_initial_time = time.time()

    print("Configuração")
    print("Utilizadas {} seeds".format(len(arr_urls_seeds)))
    print("Limite de 50000 páginas")
    print("Profundidade limite de 6")
    print("Running...")

    for a in range(num_of_threads):
        pages_fetchers.append(PageFetcher(scheduler))

    for pages_fetcher in pages_fetchers:
        p = Process(target=pages_fetcher.run())
        p.start()
        fetchers.append()

    # inicia todos as threads (PageFetchers)
    # for index in range(1):
    #     fetchers.append(PageFetcher(scheduler))
    #     fetchers[index].start()

    # aguarda pela execução de todas as threads
    for fetcher in fetchers:
        fetcher.join()

    print("Tempo final de execução: {}s".format(time.time() - d_initial_time))
