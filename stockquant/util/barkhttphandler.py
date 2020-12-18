from logging.handlers import HTTPHandler
from urllib.parse import quote
import certifi
import ssl

ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
ctx.load_verify_locations(certifi.where())


class BarkHTTPHandler(HTTPHandler):
    """
    docstring
    """

    def __init__(self, host, key):
        self.host = host
        self.url = "/"
        self.method = "POST"
        self.secure = True
        self.credentials = None
        self.context = ctx
        self.key = key

        super().__init__(self.host, self.url, method=self.method, secure=self.secure, credentials=None, context=ctx)

    def mapLogRecord(self, record):
        # URL 组成: 第一个部分是 key , 之后有三个匹配
        # /:key/:body
        # /:key/:title/:body
        # /:key/:category/:title/:body

        # title 推送标题 比 body 字号粗一点
        # body 推送内容 换行请使用换行符 '\n'
        # category 另外的功能占用的字段，还没开放 忽略就行
        # post 请求 参数名也是上面这些

        msg = record.msg

        url = quote(msg, safe="/", encoding="utf-8")

        data = self.key + "/" + url
        return data

    def emit(self, record):
        """
        Emit a record.

        Send the record to the Web server as a percent-encoded dictionary
        """
        try:
            import http.client

            host = self.host
            h = http.client.HTTPSConnection(host, context=self.context)
            data = self.mapLogRecord(record)
            h.request("POST", data)
            h.getresponse()  # can't do anything with the result
        except Exception:
            self.handleError(record)
