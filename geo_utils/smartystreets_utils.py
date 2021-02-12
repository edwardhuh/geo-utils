from dataclasses import dataclass
from typing import Dict, List, Optional, Union
from smartystreets_python_sdk import ClientBuilder, StaticCredentials, exceptions
from smartystreets_python_sdk.us_street import Lookup as StreetLookup

@dataclass(frozen=True)
class SmartyStreetResult:
    type: Optional[str]
    rdi: Optional[str]
    match: Optional[str]
    active: Optional[str]
    vacant: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]

    @classmethod
    def from_metadata(cls, result):
        return SmartyStreetResult(
            type=result.metadata.record_type,
            rdi=result.metadata.rdi,
            match=result.analysis.dpv_match_code,
            active=result.analysis.active,
            vacant=result.analysis.vacant,
            latitude=result.metadata.latitude,
            longitude=result.metadata.longitude,
        )


def smarty_api(
    flat_address_list: List[str]
) -> Optional[SmartyStreetResult]:
  """
  Run addresses through SmartyStreets API, returning a `SmartyStreetsResult`
  object with all the information we get from the API.
  If any errors occur, we'll return None.
  Args:
      flat_address_list: a flat list of addresses that will be read as follows:
        _ : an unused arguement for ID
        street: The street address, e.g., 250 OCONNOR ST
        state: The state (probably RI)
        zipcode: The zipcode to look up
        smartystreets_auth_id: Your SmartyStreets auth_id
        smartystreets_auth_token: Your SmartyStreets auth_token
  Returns:
      The result if we find one, else None
  """
  # Authenticate to SmartyStreets API
  [_, street, state, zipcode, smartystreets_auth_id,
   smartystreets_auth_token] = flat_address_list
  credentials = StaticCredentials(smartystreets_auth_id, smartystreets_auth_token)
  client = ClientBuilder(credentials).build_us_street_api_client()

  # Lookup the Address with inputs by indexing from input `row`
  lookup = StreetLookup()
  lookup.street = street
  lookup.state = state
  lookup.zipcode = zipcode

  lookup.candidates = 1
  lookup.match = "invalid"  # "invalid" always returns at least one match

  try:
    client.send_lookup(lookup)
  except exceptions.SmartyException:
    return None

  res = lookup.result
  if not res:
    # if we have exceptions, just return the inputs to retry later
    return None

  result = res[0]
  return SmartyStreetResult.from_metadata(result)






