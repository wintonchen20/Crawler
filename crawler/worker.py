from threading import Thread

from utils.download import download
from utils import get_logger
from scraper import scraper
import time


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.crc_dict = {}
        self.sim_hash_dict = {}
        self.already_found_urls = {}
        self.robots_txt = {}
        self.max_word_page = ("",0)
        self.most_common_words = {}
        self.ics_subdomains = set()
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()

            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls, self.crc_dict, self.sim_hash_dict, self.already_found_urls, self.robots_txt,self.max_word_page,self.most_common_words,self.ics_subdomains = scraper( 
                tbd_url, resp, self.crc_dict, self.sim_hash_dict, self.already_found_urls,self.robots_txt,self.max_word_page,self.most_common_words,self.ics_subdomains)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)

        #Prints out the amount of unique urls total
        self.logger.info(f"The amount of unique urls total is: {len(self.already_found_urls)}")

        #Prints out the page with the most amount of words 
        self.logger.info(f"The url: {self.max_word_page[0]} with the greatest amount of words is {self.max_word_page[1]}")

        #Prints out the top 50 common words excluding stop words
        #https://gist.github.com/sebleier/554280, got the stop words from this link
        english_stop_words = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", 
        "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", 
        "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", 
        "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", 
        "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", 
        "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", 
        "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", 
        "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", 
        "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]

        common_words = sorted(self.most_common_words,key=lambda x:self.most_common_words[x],reverse=True)

        for stop_word in english_stop_words:
            if stop_word in common_words:
                common_words.remove(stop_word)

        common_words_dict = {}

        for word in common_words[:50]:
            common_words_dict[word] = self.most_common_words[word]

        self.logger.info(f"The top fifty common words are: {common_words_dict}")

        #Print out the number of ics.uci.edu subdomains
        self.logger.info(f"The number of ics subdomains are: {len(self.ics_subdomains)} and {self.ics_subdomains}")