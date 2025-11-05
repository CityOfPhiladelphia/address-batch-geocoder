import utils.ais_lookup as ais_lookup

def test_ais_lookup_creates_address_search_url(monkeypatch):
    created = {}

    class FakeResponse:
        def __init__(self, data, status_code=200):
            self._data = data
            self.status_code = status_code

        def __getitem__(self, k):
            return self._data[k]

        def __bool__(self):
            return True

        def json(self):
            return self._data

    class FakeSession:
        def get(self, *a, **k):
            raise AssertionError("should be patched")

    def fake_get(self, url, params=None, timeout=None, **kwargs):
        created["url"] = url
        created["params"] = params
        return FakeResponse(
            {
                "features": [
                    {
                        "properties": {"street_address": "1234 MARKET ST"},
                        "geometry": {"coordinates": [-75.16, 39.95]},
                    }
                ]
            },
            200,
        )

    monkeypatch.setattr(FakeSession, "get", fake_get)
    sess = FakeSession()

    result = ais_lookup.ais_lookup(sess, "1234", "1234 mkt st", [])

    assert created["url"] == "https://api.phila.gov/ais/v1/search/1234 mkt st"
    assert created["params"] == {"gatekeeperKey": "1234"}
    assert result == {
        "geocode_lat": "39.95",
        "geocode_lon": "-75.16",
        "is_addr": True,
        "is_philly_addr": True,
        "output_address": "1234 MARKET ST",
    }


def test_false_address_returns_none_if_bad_address(monkeypatch):

    class FakeResponse:
        def __init__(self, data, status_code=404):
            self._data = data
            self.status_code = status_code

        def __getitem__(self, k):
            return self._data[k]

        def __bool__(self):
            return True

        def json(self):
            return self._data

    class FakeSession:
        def get(self, *a, **k):
            raise AssertionError("Should be patched")

    def fake_get(self, url, params=None, timeout=None, **kwargs):
        return FakeResponse(
            {"features": [{"properties": {"street_address": "123 fake st"}}]}, 404
        )

    monkeypatch.setattr(FakeSession, "get", fake_get)
    sess = FakeSession()

    result = ais_lookup.ais_lookup(sess, "1234", "1234 fake st", [])

    assert result == {
        "geocode_lat": None,
        "geocode_lon": None,
        "is_addr": False,
        "is_philly_addr": False,
        "output_address": "",
    }
