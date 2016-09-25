#返回网页正确的源码
def return_source_html(url):
    r=requests.get(url)
    header=r.headers
    if 'Content-Type' in header and 'charset' in header.get('Content-Type'):
        encode=re.search(r'[^=]+$',header['Content-Type']).group()
    else:
        encode='gbk'
    r.encoding=encode
    return r.text
