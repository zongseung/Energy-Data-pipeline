"""파일데이터 페이지에서 atchFileId 뽑기."""
from pipeline.fetch_data.nambu.file_download import DATASETS, MASTERS, extract_file_id


def test_페이지에서_첨부파일id를_뽑는다():
    html = """<a href="#" onclick="fn_fileDataDown('15043410','uddi:a2a4230d');">
              <span>/cmm/cmm/fileDownload.do?atchFileId=FILE_000000003625795&fileDetailSn=1</span>"""
    assert extract_file_id(html) == "FILE_000000003625795"


def test_없으면_None():
    assert extract_file_id("<html>내려받기 링크가 없다</html>") is None


def test_데이터셋_번호는_중복되지_않는다():
    assert len(DATASETS) == 25
    assert len(set(DATASETS.values())) == 25


def test_마스터는_실적과_번호가_겹치지_않는다():
    assert len(MASTERS) == 4
    assert not (DATASETS.keys() & MASTERS.keys())
