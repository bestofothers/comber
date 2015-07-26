from __future__ import absolute_import

import requests
from lxml import html, etree
import socket
import re
import unicodedata
from .blacklist import remove_blacklist_links
from pprint import pprint
import time

# !!! NOTE: Export external links to file - not implemented

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

class ExtractEmails(object):

    def __init__(self, website, get_external=False):
        self._website = website
        self.get_external = get_external


    def _extract_emails(self, source):

        regex = re.compile(r'([\w\-\.]{1,100}@(\w[\w\-]+\.)+[\w\-]+)')
        emails = list(set(regex.findall(source)))
        # all_emails = [email[0].lower() for email in emails if not email[0].endswith('.png') and not email[0].endswith('.jpg')]
        all_emails = []
        for email in emails:
            if not email[0].endswith('.png') and not email[0].endswith('.jpg') and not email[0].endswith('.gif'):
                all_emails.append(email[0].lower())

        return list(set(all_emails))

    def _extract_links(self, website):
        """
        Extract unique links and removes bad urls, mainly social networks and directories
        :return:
        """
        source = self._get_documment(website)
        try:
            links = source.cssselect('a') + source.cssselect('area')
            hr = []
            for l in links:
                if l.get('href'):
                    if l.get('href').startswith('http') or l.get('href').startswith('www.'):
                        hr.append(l.get('href'))
            return remove_blacklist_links(hr)
        except AttributeError:
            raise


    def fetch_emails(self):

        emails = []
        urls = ''
        try:
            urls = self._extract_links(self._website)
        except AttributeError:
            print("Error creaped in")
        for url in urls:
            element = self._get_documment(url)

            if element is None:
                continue
            else:
                source = html.tostring(element).replace('%20', ' ')
                if source is None:
                    continue
                else:
                    ems = self._extract_emails(source)
                    emails.extend(ems)
        return list(set(emails))

    def fetch_external_links(self):

        other_sites = []
        try:

            urls = self._extract_links(self._website)
            for url in urls:
                ae = self._seperate_links(self._extract_links(url))
                other_sites.extend(ae['others'])
        except AttributeError:
            print("001")
        return list(set(other_sites))


    def _get_documment(self, url):

        header = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0'}
        doc = None
        try:
            response = requests.get(url, headers=header, timeout=10)
            content = unicodedata.normalize('NFD', response.text).encode('ascii','replace')
            doc = html.fromstring(content)
            doc.make_links_absolute(url, resolve_base_href=True)

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError,
                requests.exceptions.TooManyRedirects,requests.exceptions.InvalidSchema,
                requests.exceptions.ContentDecodingError, requests.exceptions.ConnectTimeout) as e:
            print("Url not reachable or too many redirects: {0}\n".format(e))
        except (etree.ParserError, socket.timeout, etree.XMLSyntaxError) as e:
            print(url, e)
        except AttributeError as e:
            print(e)
        return doc

    @staticmethod
    def is_redirected(source):
        """
        Check of page has meta (html) redirects and return the URL if true else return original
        :param url:
        :return:
        """
        try:
            refresh = source.cssselect('meta[http-equiv="refresh"]')
            if refresh:
                x = refresh[0].attrib['content'].find('http')
                web = refresh[0].attrib['content'][x:]
                return web
        except AttributeError:
            raise

    def _seperate_links(self, links):
        """
        Collects external links from main link

        Export external links to csv with main as file name
        :return:
        """
        m = urlparse(self._website)
        main_link = []
        others = []

        for link in links:
            w = urlparse(link)

            if m.netloc == w.netloc:
                main_link.append(link)
            else:
                others.append(link)

        return {'main': main_link, 'others': others}

