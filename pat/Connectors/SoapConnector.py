from fsspec import AbstractFileSystem

try:
    from zeep import Client, Transport
    from requests.auth import HTTPBasicAuth  # or HTTPDigestAuth, or OAuth1, etc.
    from requests import Session
except:
    print("No zeep or requests libraries installed for SOAP Connection")
import fsspec

# SOAP
try:
    class MyTrasp(Transport):
        res_xml = None

        def __init__(self, cache=None, timeout=300, operation_timeout=None, session=None):
            super(MyTrasp, self).__init__(cache, timeout, operation_timeout, session)

        def post_xml(self, address, envelope, headers):
            res = super(MyTrasp, self).post_xml(address, envelope, headers)
            self.res_xml = res
            return res
except:
    pass


class Soap(AbstractFileSystem):
    # tempdir = str(tempfile.gettempdir())
    protocol = "soap"

    def __init__(
            self,
            request_url,
            request_method,
            request_username=None,
            request_password=None,
            trasport=None,
            request_payload=None,
            **kwargs,
    ):
        if request_payload is None:
            request_payload = [""]
        if self._cached:
            return
        super(Soap, self).__init__(**kwargs)

        self.request_url = request_url
        self.request_username = request_username
        self.request_password = request_password
        self.request_method = request_method
        self.request_payload = request_payload
        self.trasport = trasport
        self._connect()

    def _connect(self):

        session = Session()
        if self.request_username and self.request_password:
            session.auth = HTTPBasicAuth(self.request_username, self.request_password)
        if self.trasport:
            self.my_transp = MyTrasp(session=session)
            wsdl = self.request_url
            client = Client(wsdl=wsdl, transport=self.my_transp)
        else:
            wsdl = self.request_url
            client = Client(wsdl=wsdl)
        self.method = getattr(client.service, self.request_method)

    def _open(self, path, **kwargs):
        if self.request_payload is None:
            obj_ = self.method("")  # oppure self.method(Payload().s)
        else:
            obj_ = self.method(*self.request_payload)

        if self.trasport and obj_:
            data_ = self.my_transp.res_xml.content
        else:
            data_ = obj_
        return SoapMsg(data_)

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        protocol = Soap.protocol + '://'
        url = urlpath[len(protocol):]
        return {"request_url": url}


class SoapMsg:
    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data

    def close(self):
        pass


fsspec.register_implementation("soap", Soap, clobber=True, errtxt=None)
