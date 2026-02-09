import utils.ais_lookup as ais_lookup


def test_ais_lookup_creates_address_search_url(monkeypatch):
    created = {}
    call_count = {"count": 0}

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
        call_count["count"] += 1
        created["url"] = url
        created["params"] = params
        
        # First call (SRID 4326)
        if call_count["count"] == 1:
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
        # Second call (SRID 2272)
        else:
            return FakeResponse(
                {
                    "features": [
                        {
                            "geometry": {"coordinates": [2694393.35, 235982.72]},
                        }
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
        fetch_4326=True,
        fetch_2272=True,
    )

    assert "1234%20mkt%20st" in created["url"] or "1234%20MARKET%20ST" in created["url"]
    assert result == {
        "geocode_lat": "39.95",
        "geocode_lon": "-75.16",
        "geocode_x": "2694393.35",
        "geocode_y": "235982.72",
        "is_addr": True,
        "is_philly_addr": True,
        "output_address": "1234 MARKET ST",
        "match_type": "ais",
        "is_multiple_match": False,
    }


def test_ais_lookup_only_fetches_4326(monkeypatch):
    created = {}

    class FakeResponse:
        def __init__(self, data, status_code=200):
            self._data = data
            self.status_code = status_code

        def json(self):
            return self._data

    class FakeSession:
        def get(self, *a, **k):
            raise AssertionError("should be patched")

    def fake_get(self, url, params=None, timeout=None, **kwargs):
        created["url"] = url
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
                    }
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
        fetch_4326=True,
        fetch_2272=False,
    )

    assert result == {
        "geocode_lat": "39.95",
        "geocode_lon": "-75.16",
        "is_addr": True,
        "is_philly_addr": True,
        "output_address": "1234 MARKET ST",
        "match_type": "ais",
        "is_multiple_match": False,
    }
    # Ensure geocode_x and geocode_y are not in result
    assert "geocode_x" not in result
    assert "geocode_y" not in result


def test_ais_lookup_only_fetches_2272(monkeypatch):
    created = {}
    call_count = {"count": 0}

    class FakeResponse:
        def __init__(self, data, status_code=200):
            self._data = data
            self.status_code = status_code

        def json(self):
            return self._data

    class FakeSession:
        def get(self, *a, **k):
            raise AssertionError("should be patched")

    def fake_get(self, url, params=None, timeout=None, **kwargs):
        call_count["count"] += 1
        created["url"] = url
        
        # First call (initial lookup, always 4326)
        if call_count["count"] == 1:
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
                        }
                    ],
                },
                200,
            )
        # Second call (SRID 2272)
        else:
            return FakeResponse(
                {
                    "features": [
                        {
                            "geometry": {"coordinates": [2694393.35, 235982.72]},
                        }
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
        fetch_4326=False,
        fetch_2272=True,
    )

    assert result == {
        "geocode_x": "2694393.35",
        "geocode_y": "235982.72",
        "is_addr": True,
        "is_philly_addr": True,
        "output_address": "1234 MARKET ST",
        "match_type": "ais",
        "is_multiple_match": False,
    }
    # Ensure geocode_lat and geocode_lon are not in result
    assert "geocode_lat" not in result
    assert "geocode_lon" not in result


def test_ais_lookup_tiebreaks(monkeypatch):
    created = {}
    call_count = {"count": 0}

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
        call_count["count"] += 1
        created["url"] = url
        created["params"] = params
        
        # First call (SRID 4326)
        if call_count["count"] == 1:
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
        # Second call (SRID 2272)
        else:
            return FakeResponse(
                {
                    "features": [
                        {
                            "geometry": {"coordinates": [2694393.35, 235982.72]},
                        }
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
        fetch_4326=True,
        fetch_2272=True,
    )

    assert "1234%20mkt%20st" in created["url"] or "1234%20N%20MARKET%20ST" in created["url"]
    assert result == {
        "geocode_lat": "39.95",
        "geocode_lon": "-75.16",
        "geocode_x": "2694393.35",
        "geocode_y": "235982.72",
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
        fetch_4326=True,
        fetch_2272=True,
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
        fetch_4326=True,
        fetch_2272=True,
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