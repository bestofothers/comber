from sites.goobing import PyBingIt, PyGoogleIt
from const import GOOGLE_API_KEY, GOOGLE_APP_ID, BING_API_KEY
from rubbish.db import DB
from rubbish.blacklist import remove_blacklist_links
from rubbish.ex import ExtractEmails
from threading import Thread

try:
    from urllib.parse import parse_qs, urlparse
except ImportError:
    from urlparse import parse_qs, urlparse


class WebSearcher(object):

    def __init__(self, query, sources, google_next=4, bing_next=4):

        self.query = query
        self.sources = sources
        self.google_next = google_next
        self.bing_next = bing_next

    def search(self, verbose=False):  # would work on verbose

        data = []
        for source in self.sources:
            if source.lower() == 'google.com':
                index = 1
                while self.google_next > 0:
                    gs = PyGoogleIt(GOOGLE_API_KEY, GOOGLE_APP_ID, index)
                    rest, index = gs.google_search(self.query)
                    j = self._pack(rest, 'google.com')
                    data.extend(j)

                    if not index:
                        break
                    self.google_next -= 1
            if source.lower() == 'bing.com':
                offset = 0
                while self.bing_next > 0:
                    bs = PyBingIt(BING_API_KEY)
                    rest, next_link = bs.bing_search(self.query, offset=offset)
                    offset = int(parse_qs(next_link)['$skip'][0])
                    j = self._pack(rest, 'bing.com')
                    data.extend(j)
                    if not offset:
                        break
                    self.bing_next -= 1
        info = {'Data': data, 'Query': self.query}
        return info

    def _pack(self, results, source):
        content = []
        for result in results:
            url = urlparse(result.url)
            web = url.scheme + "://" + url.netloc

            d = {'Title': result.title, 'Website': web,
                 'Description': result.description, 'Source': source}
            content.append(d)
        return content


def web_main(item):
    for i in item:
        ws = WebSearcher(i, ['bing.com', 'google.com'])
        res = ws.search()
        yield res

def go(terms):

    global add
    for item in web_main(terms):
        for b in item['Data']:
            if remove_blacklist_links([b['Website']]):
                store = DB(item['Query'], b['Source'])
                ttl = b['Title']
                desc = b['Description']
                website = b['Website']
                ew = ExtractEmails(website)
                emails = ew.fetch_emails()

                add.extend(emails)

                store.store_data(ttl, desc, [website], emails)
                print(emails)
                print("Email: {} emails......".format(len(emails)))
            else:
                continue

    print(list(set(add)))
    print(len(list(set(add))), "Total Emails........")

    print("All done ")

if __name__ == '__main__':

    print("""
    Python Hope
                             _________
                                | |
                           _   __|__   _
                          (_)-/__|__\-(_)
      ..                     /\_____/\                     ..
     (__)^^_________________(  ( . )  )_________________^^(__) -mo
                             \_______/
                               ./ \.
    """)
    term = ["chiffon fabric", "airplane parts", "kitchen faucets", "Electrical machinery", "Power generation"]
    add = []
    for t in term:
        Thread(target=go, args=([t],)).start()


