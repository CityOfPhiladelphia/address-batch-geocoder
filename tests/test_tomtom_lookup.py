import utils.tomtom_lookup as tomtom_lookup

json_response_match =  {'spatialReference': {'wkid': 4326, 'latestWkid': 4326},
    'candidates': [{'address': '1234 Market St, Philadelphia, Pennsylvania, 19107',
    'location': {'x': -75.16047189802985, 'y': 39.951918251135154},
    'score': 100,
    'attributes': {},
    'extent': {'xmin': -75.16147189802986,
        'ymin': 39.95091825113516,
        'xmax': -75.15947189802985,
        'ymax': 39.95291825113515}},
    {'address': '1234 Market St, Gloucester City, New Jersey, 08030',
    'location': {'x': -75.11192847164241, 'y': 39.88775918851947},
    'score': 97.26,
    'attributes': {},
    'extent': {'xmin': -75.11292847164242,
        'ymin': 39.88675918851947,
        'xmax': -75.11092847164241,
        'ymax': 39.888759188519465}}
    ]}

json_response_nonmatch = {'spatialReference': {'wkid': 4326, 'latestWkid': 4326},
    'candidates': []}

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
            {}, 404
        )

    monkeypatch.setattr(FakeSession, "get", fake_get)
    sess = FakeSession()

    result = tomtom_lookup.tomtom_lookup(sess, "1234 Fake St")

    assert result == {
        "geocode_lat": None,
        "geocode_lon": None,
        "output_address": "",
    }
