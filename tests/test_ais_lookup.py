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
                "search_type": "address",
                "features": [
                    {
                        "properties": {
                            "street_address": "1234 MARKET ST",
                            "zip_code": "19107",
                        },
                        "geometry": {"coordinates": [-75.16, 39.95]},
                    },
                    {
                        "properties": {
                            "street_address": "1234 MARKET ST",
                            "zip_code": "11111",
                        },
                        "geometry": {"coordinates": [-75.16, 39.95]},
                    },
                ],
            },
            200,
        )

    monkeypatch.setattr(FakeSession, "get", fake_get)
    sess = FakeSession()

    result = ais_lookup.ais_lookup(
        sess,
        "1234",
        "1234 mkt st",
        "19107",
        [],
        existing_is_addr=True,
        existing_is_philly_addr=True,
        original_address="1234 mkt st",
    )

    assert created["url"] in("https://api.phila.gov/ais/v1/search/1234%20mkt%20st?gatekeeperKey=1234&srid=4326&max_range=0",\
                              "https://api.phila.gov/ais/v1/search/1234%20MARKET%20ST?gatekeeperKey=1234&srid=2272&max_range=0")
    assert result == {
        "geocode_lat": "39.95",
        "geocode_lon": "-75.16",
        "geocode_x": "-75.16",
        "geocode_y": "39.95",
        "is_addr": True,
        "is_philly_addr": True,
        "output_address": "1234 MARKET ST",
        "match_type": "ais",
        "is_multiple_match": False,
    }


def test_ais_lookup_tiebreaks(monkeypatch):
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
                "search_type": "address",
                "features": [
                    {
                        "properties": {
                            "street_address": "1234 N MARKET ST",
                            "zip_code": "19107",
                        },
                        "geometry": {"coordinates": [-75.16, 39.95]},
                    },
                    {
                        "properties": {
                            "street_address": "1234 S MARKET ST",
                            "zip_code": "11111",
                        },
                        "geometry": {"coordinates": [-75.16, 39.95]},
                    },
                ],
            },
            200,
        )

    monkeypatch.setattr(FakeSession, "get", fake_get)
    sess = FakeSession()

    result = ais_lookup.ais_lookup(
        sess,
        "1234",
        "1234 mkt st",
        "19107",
        [],
        existing_is_addr=True,
        existing_is_philly_addr=True,
        original_address="1234 mkt st",
    )

    assert created["url"] in("https://api.phila.gov/ais/v1/search/1234%20mkt%20st?gatekeeperKey=1234&srid=4326&max_range=0",\
                              "https://api.phila.gov/ais/v1/search/1234%20N%20MARKET%20ST?gatekeeperKey=1234&srid=2272&max_range=0")
    assert result == {
        "geocode_lat": "39.95",
        "geocode_lon": "-75.16",
        "geocode_x": "-75.16",
        "geocode_y": "39.95",
        "is_addr": True,
        "is_philly_addr": True,
        "output_address": "1234 N MARKET ST",
        "match_type": "ais",
        "is_multiple_match": False,
    }


def test_ais_lookup_returns_no_match_if_tiebreak_fails(monkeypatch):
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
                "search_type": "address",
                "features": [
                    {
                        "properties": {
                            "street_address": "1234 N MARKET ST",
                            "zip_code": "22222",
                        },
                        "geometry": {"coordinates": [-75.16, 39.95]},
                    },
                    {
                        "properties": {
                            "street_address": "1234 S MARKET ST",
                            "zip_code": "11111",
                        },
                        "geometry": {"coordinates": [-75.16, 39.95]},
                    },
                ],
            },
            200,
        )

    monkeypatch.setattr(FakeSession, "get", fake_get)
    sess = FakeSession()

    result = ais_lookup.ais_lookup(
        sess,
        "1234",
        "1234 mkt st",
        "19107",
        [],
        existing_is_addr=True,
        existing_is_philly_addr=True,
        original_address="1234 mkt st",
    )

    assert created["url"] == "https://api.phila.gov/ais/v1/search/1234%20mkt%20st?gatekeeperKey=1234&srid=4326&max_range=0"
    assert result == {
        "geocode_lat": None,
        "geocode_lon": None,
        "geocode_x": None,
        "geocode_y": None,
        "is_addr": False,
        "is_philly_addr": True,
        "output_address": "1234 mkt st",
        "match_type": "ais",
        "is_multiple_match": True,
    }


def test_false_address_returns_input_address_if_bad_address(monkeypatch):
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
            {
                "search_type": "address",
                "features": [{"properties": {"street_address": "123 fake st"}}],
            },
            404,
        )

    monkeypatch.setattr(FakeSession, "get", fake_get)
    sess = FakeSession()

    address = "123 fake st"
    result = ais_lookup.ais_lookup(
        sess,
        "1234",
        address,
        zip=None,
        enrichment_fields=[],
        existing_is_addr=False,
        existing_is_philly_addr=False,
        original_address=address,
    )

    assert result == {
        "geocode_lat": None,
        "geocode_lon": None,
        "geocode_x": None,
        "geocode_y": None,
        "is_addr": False,
        "is_philly_addr": False,
        "output_address": "123 fake st",
        "is_multiple_match": False,
        "match_type": None,
    }
