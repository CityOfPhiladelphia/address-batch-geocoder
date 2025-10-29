import pytest, requests, utils.ais_lookup as ais_lookup, polars as pl
from types import SimpleNamespace

def test_ais_lookup_creates_address_search_url(monkeypatch):
    created = {}

    class FakeResponse:
        def __init__(self, data, status_code=200):
            self._data = data
            self.status_code = status_code
        def __getitem__(self, k): return self._data[k]
        def __bool__(self): return True

    class FakeSession:
        def get(self, *a, **k): raise AssertionError("should be patched")

    def fake_get(self, url, params=None, timeout=None, **kwargs):
        created["url"] = url
        created["params"] = params
        return FakeResponse({"features":[{"street_address":"1234 MARKET ST"}]}, 200)

    monkeypatch.setattr(FakeSession, "get", fake_get)
    sess = FakeSession()

    result = ais_lookup.ais_lookup(sess, "1234", "1234 mkt st")

    assert created["url"] == "https://api.phila.gov/ais/v1/search/1234 mkt st"
    assert created["params"] == {"gatekeeperKey": "1234"}
    assert result == ("1234 MARKET ST", True, True)

def test_false_address_returns_none_if_bad_address(monkeypatch):

    class FakeResponse:
        def __init__(self, data, status_code=404):
            self._data = data
            self.status_code = status_code
        def __getitem__(self, k): return self._data[k]
        def __bool__(self): return True
    
    class FakeSession:
        def get(self, *a, **k): raise AssertionError("Should be patched")
    
    def fake_get(self, url, params=None, timeout=None, **kwargs):
        return FakeResponse({"features":[{"street_address":"123 fake st"}]}, 404)
    
    monkeypatch.setattr(FakeSession, "get", fake_get)
    sess = FakeSession()

    result = ais_lookup.ais_lookup(sess, "1234", "1234 fake st")

    assert result == (None, False, False)

def stream_null_geos_batches_correctly():

    df = pl.DataFrame({'id': [1,2,3,4], 
                       'street': ['1234 MARKET ST', None, None, '123 FAKE ST']})
    lf = df.lazy()

    count = 0

    for batch in ais_lookup.stream_null_geos(lf, 1):
        count += 1

    assert count == 2

def test_ais_append_raises_value_error_on_bad_dataframe():

    df = pl.DataFrame({'id': [1, 2], 'street': ['1234 MARKET ST', '123 FAKE ST']})
    lf = df.lazy()
    sess = requests.Session()

    with pytest.raises(ValueError):
        ais_lookup.ais_append(sess, '123', lf)



