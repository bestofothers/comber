import re


def remove_blacklist_links(urls):
    """This function is to remove blackisted url
    as they have too many links and also reduce
    time for getting emails
    from source. Takes a list of urls
    """
    # add more to this regex
    blacklist = re.compile('\
.alibaba.com|.1688.com|.trade2cn.com|.vk.com\
.qq.com|.aliexpress.com|.taobao.com|.made-in-china.com|\
.globalsources.com|.indiamart.com|.tradeindia.com|.youtube.com|\
.ecvv.com|.ec21.com|.diytrade.com|.alibaba-inc.com|.tmall.com|.pinterest.com|\
.facebook.com|.linkedin.com|.google.com|.twitter.com|.instagram.com|\
.microsoft.com|.xing.com|.ebay.com|.amazon.com|.wikipedia.com\
//alibaba.com|//1688.com|//trade2cn.com|//vk.com|\
//qq.com|//aliexpress.com|//taobao.com|//made-in-china.com|\
//globalsources.com|//indiamart.com|//tradeindia.com|//youtube.com|\
//ecvv.com|//ec21.com|//diytrade.com|//alibaba-inc.com|//tmall.com|//pinterest.com|\
//facebook.com|//linkedin.com|//google.com|//twitter.com|//instagram.com|\
//microsoft.com|//xing.com|//ebay.com|//amazon.com|//wikipedia.com')
    remove_url = []

    for x in urls:
        try:
            x = str(x)
            for y in blacklist.findall(x):
                if y:
                    remove_url.append(x)
        except:
            pass
    fixed_urls = list(set(urls) - set(remove_url))
    return fixed_urls
