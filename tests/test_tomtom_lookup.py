import utils.tomtom_lookup as tomtom_lookup
from passyunk.parser import PassyunkParser

p = PassyunkParser()

json_response_match = {
    "spatialReference": {"wkid": 4326, "latestWkid": 4326},
    "candidates": [
        {
            "address": "1234 Market St, Philadelphia, Pennsylvania, 19107",
            "location": {"x": -75.16047189802985, "y": 39.951918251135154},
            "score": 100,
            "attributes": {},
            "extent": {
                "xmin": -75.16147189802986,
                "ymin": 39.95091825113516,
                "xmax": -75.15947189802985,
                "ymax": 39.95291825113515,
            },
        },
        {
            "address": "1234 Market St, Gloucester City, New Jersey, 08030",
            "location": {"x": -75.11192847164241, "y": 39.88775918851947},
            "score": 97.26,
            "attributes": {},
            "extent": {
                "xmin": -75.11292847164242,
                "ymin": 39.88675918851947,
                "xmax": -75.11092847164241,
                "ymax": 39.888759188519465,
            },
        },
    ],
}

json_response_match_2272 = {
    "spatialReference": {"wkid": 2272, "latestWkid": 2272},
    "candidates": [
        {
            "address": "1234 Market St, Philadelphia, Pennsylvania, 19107",
            "location": {"x": 2694393.35, "y": 235982.72},
            "score": 100,
            "attributes": {},
        }
    ],
}

json_response_nonmatch = {
    "spatialReference": {"wkid": 4326, "latestWkid": 4326},
    "candidates": [],
}


def test_tomtom_lookup_fetches_both_srids(monkeypatch):
    call_count = {"count": 0}

    class FakeResponse:
        def __init__(self, data, status_code=200):
            self._data = data
            self.status_code = status_code

        def json(self):
            return self._data

    class FakeSession:
        def get(self, *a, **k):
            raise AssertionError("Should be patched")

    def fake_get(self, url, params=None, timeout=None, **kwargs):
        call_count["count"] += 1
        # First call - SRID 4326
        if call_count["count"] == 1:
            return FakeResponse(json_response_match, 200)
        # Second call - SRID 2272
        else:
            return FakeResponse(json_response_match_2272, 200)

    monkeypatch.setattr(FakeSession, "get", fake_get)
    sess = FakeSession()

    result = tomtom_lookup.tomtom_lookup(
        sess, p, ["19107"], "1234 Market St", "1234 MARKET ST",
        fetch_4326=True, fetch_2272=True
    )

    assert result == {
        "geocode_lat": "39.951918251135154",
        "geocode_lon": "-75.16047189802985",
        "geocode_x": "2694393.35",
        "geocode_y": "235982.72",
        "is_addr": True,
        "is_philly_addr": True,
        "output_address": "1234 MARKET ST",
        "match_type": "tomtom",
    }


def test_tomtom_lookup_only_fetches_4326(monkeypatch):
    class FakeResponse:
        def __init__(self, data, status_code=200):
            self._data = data
            self.status_code = status_code

        def json(self):
            return self._data

    class FakeSession:
        def get(self, *a, **k):
            raise AssertionError("Should be patched")

    def fake_get(self, url, params=None, timeout=None, **kwargs):
        return FakeResponse(json_response_match, 200)

    monkeypatch.setattr(FakeSession, "get", fake_get)
    sess = FakeSession()

    result = tomtom_lookup.tomtom_lookup(
        sess, p, ["19107"], "1234 Market St", "1234 MARKET ST",
        fetch_4326=True, fetch_2272=False
    )

    assert result == {
        "geocode_lat": "39.951918251135154",
        "geocode_lon": "-75.16047189802985",
        "is_addr": True,
        "is_philly_addr": True,
        "output_address": "1234 MARKET ST",
        "match_type": "tomtom",
    }
    # Ensure geocode_x and geocode_y are not in result
    assert "geocode_x" not in result
    assert "geocode_y" not in result


def test_tomtom_lookup_only_fetches_2272(monkeypatch):
    call_count = {"count": 0}

    class FakeResponse:
        def __init__(self, data, status_code=200):
            self._data = data
            self.status_code = status_code

        def json(self):
            return self._data

    class FakeSession:
        def get(self, *a, **k):
            raise AssertionError("Should be patched")

    def fake_get(self, url, params=None, timeout=None, **kwargs):
        call_count["count"] += 1
        # First call - initial lookup (always uses 4326 for address matching)
        if call_count["count"] == 1:
            return FakeResponse(json_response_match, 200)
        # Second call - SRID 2272
        else:
            return FakeResponse(json_response_match_2272, 200)

    monkeypatch.setattr(FakeSession, "get", fake_get)
    sess = FakeSession()

    result = tomtom_lookup.tomtom_lookup(
        sess, p, ["19107"], "1234 Market St", "1234 MARKET ST",
        fetch_4326=False, fetch_2272=True
    )

    assert result == {
        "geocode_x": "2694393.35",
        "geocode_y": "235982.72",
        "is_addr": True,
        "is_philly_addr": True,
        "output_address": "1234 MARKET ST",
        "match_type": "tomtom",
    }
    # Ensure geocode_lat and geocode_lon are not in result
    assert "geocode_lat" not in result
    assert "geocode_lon" not in result


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
        return FakeResponse({}, 404)

    monkeypatch.setattr(FakeSession, "get", fake_get)
    sess = FakeSession()

    result = tomtom_lookup.tomtom_lookup(
        sess, p, ["1111"], "1234 Fake St", "1234 FAKE ST",
        fetch_4326=True, fetch_2272=True
    )

    assert result == {
        "geocode_lat": None,
        "geocode_lon": None,
        "geocode_x": None,
        "geocode_y": None,
        "is_addr": False,
        "is_philly_addr": False,
        "output_address": "1234 FAKE ST",
        "match_type": None,
    }


def test_tomtom_lookup_handles_non_philly_address(monkeypatch):
    call_count = {"count": 0}

    class FakeResponse:
        def __init__(self, data, status_code=200):
            self._data = data
            self.status_code = status_code

        def json(self):
            return self._data

    class FakeSession:
        def get(self, *a, **k):
            raise AssertionError("Should be patched")

    def fake_get(self, url, params=None, timeout=None, **kwargs):
        call_count["count"] += 1
        # Return the NJ address (second candidate)
        if call_count["count"] == 1:
            return FakeResponse({
                "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                "candidates": [
                    {
                        "address": "1234 Market St, Gloucester City, New Jersey, 08030",
                        "location": {"x": -75.11192847164241, "y": 39.88775918851947},
                        "score": 100,
                        "attributes": {},
                    }
                ],
            }, 200)
        else:
            return FakeResponse({
                "spatialReference": {"wkid": 2272, "latestWkid": 2272},
                "candidates": [
                    {
                        "address": "1234 Market St, Gloucester City, New Jersey, 08030",
                        "location": {"x": 2700000.00, "y": 240000.00},
                        "score": 100,
                        "attributes": {},
                    }
                ],
            }, 200)

    monkeypatch.setattr(FakeSession, "get", fake_get)
    sess = FakeSession()

    result = tomtom_lookup.tomtom_lookup(
        sess, p, ["19107"], "1234 Market St", "1234 MARKET ST",
        fetch_4326=True, fetch_2272=True
    )

    assert result["is_addr"] == True
    assert result["is_philly_addr"] == False
    assert result["match_type"] == "tomtom"
    assert result["geocode_lat"] == "39.88775918851947"
    assert result["geocode_lon"] == "-75.11192847164241"
    assert result["geocode_x"] == "2700000.0"
    assert result["geocode_y"] == "240000.0"