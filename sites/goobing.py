import requests
from apiclient.discovery import build, HttpError

try:
    import urllib2 as urllib
except:
    import urllib

import time

class PyBingItException(Exception):
        pass


class PyBingIt(object):

    """Bing Search API limited
    Big is limited to 5k requests per month. i.e 166 request dail
       -
    """
    _api_url = "https://api.datamarket.azure.com/Bing/Search/Web?Query={}&$top={}&$skip={}&$format={}"

    def __init__(self, api_key, safe=False):
        """
        :param api_key: key of api obtained from Microsoft
        :param safe:
        :return:
        """
        self.api_key = api_key
        self.safe = safe

    def bing_search(self, query, limit=50, offset=0, format='json'):
        base_url = self._api_url.format(urllib.quote("'{}'".format(query)), limit, offset, format)
        r = requests.get(base_url, auth=("", self.api_key))

        try:
            json_results = r.json()
        except ValueError:
            if not self.safe:
                raise PyBingItException("Request error code -> {} message -> {}"
                                        .format(r.status_code, r.text))
            else:
                print("[ERROR] Request error code -> {} message -> {}"
                      + "\nContinue in 5 seconds".format(r.status_code, r.text))
                time.sleep(5)
        try:
            next_link = json_results['d']['__next']

        except KeyError as kE:
            print("Couldn't extract next_link: KeyError: {}".format(kE))
            next_link = ''

        return [BResult(single_json_result) for single_json_result in json_results['d']['results']], next_link


class BResult(object):

    class _Meta(object):
        '''
        Holds the meta info for the result.
        '''
        def __init__(self, meta):
            self.type = meta['type']
            self.uri = meta['uri']

    def __init__(self, result):
        self.url = result['Url']
        self.title = result['Title']
        self.description = result['Description']
        self.id = result['ID']

        self.meta = self._Meta(result['__metadata'])

class PyGoogleIt(object):

    def __init__(self, api_key, app_id, start_index=1):
        self.api_key = api_key
        self.app_id = app_id
        self.start_index = start_index

    def google_search(self, query, limit=10):

        service = build("customsearch", "v1",
                developerKey=self.api_key)
        res = ''

        try:
            res = service.cse().list(
                 q=query,
                 cx=self.app_id,
                 num=limit,
                 start=self.start_index,
            ).execute()
        except HttpError as err:
            error_codes = [400, 403, 500, 503]

            if err.resp.status in error_codes:
                print(err)
                print("[ERROR]: Continuing in 5 seconds.")
                time.sleep(5)
            else:
                raise
        try: # next page
            next_index = self.start_index = res['queries']['nextPage'][0]['startIndex']

        except KeyError as kE:
            print("Couldn't extract next_link: KeyError: {}".format(kE))
            next_index = ''
        return [GResult(result) for result in res['items']], next_index

class GResult(object):

    def __init__(self, result):
        self.url = result['link']
        self.title = result['title']
        self.description = result['htmlSnippet']
        # self.id = result['cacheId']
