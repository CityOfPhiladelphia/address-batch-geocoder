import requests
from retrying import retry

# Code adapted from Alex Waldman and Roland MacDavid
# https://github.com/CityOfPhiladelphia/databridge-etl-tools/blob/master/databridge_etl_tools/ais_geocoder/ais_request.py

@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
def ais_lookup(sess: requests.Session, api_key: str, address: str) -> tuple[str, bool, bool]:
    """
    Given a passyunk-normalized address, looks up whether or not it is in the
    database. 
    
    Args:
        sess (requests Session object): A requests library session object
        api_key (str): An AIS api key
        address (str): The address to query
    
    Returns:
        The standardized address
    """

    ais_url = 'https://api.phila.gov/ais/v1/search/' + address
    params = {}
    params['gatekeeperKey'] = api_key

    response = sess.get(ais_url, params=params, timeout=10)

    if response.status_code >= 500:
        raise Exception('5xx response')
    elif response.status_code == 429:
        raise Exception('429 response')
    elif response.status_code != 200:
        return None
    
    if response:
        address = response['features'][0]['street_address']
        is_addr = True
        is_philly_addr = True

        return(address, is_addr, is_philly_addr)

    return (None, False, False)