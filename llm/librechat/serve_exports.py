"""CSV export 정적 서빙 — 디렉토리 목록만 차단한다.

파일명이 무작위(uuid 8자리)라 목록만 막으면 링크를 아는 사람만 받을 수 있다.
존재 여부를 노출하지 않도록 목록 요청에는 403이 아니라 404를 준다.
"""
import functools
from http.server import HTTPServer, SimpleHTTPRequestHandler


class NoListingHandler(SimpleHTTPRequestHandler):
    def list_directory(self, path):
        self.send_error(404)
        return None


if __name__ == "__main__":
    HTTPServer(
        ("0.0.0.0", 8098),
        functools.partial(NoListingHandler, directory="/exports"),
    ).serve_forever()
