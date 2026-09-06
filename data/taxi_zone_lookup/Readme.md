# Data Description
- A taxi zone is "roughly based on NYC Department of City Planning’s Neighborhood Tabulation Areas (NTAs) and are meant to approximate neighborhoods"
    - https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/8meu-9t5y/about_data
    - check [analysis](./analysiz.ipynb)

- <b>LocationID</b> -> Taxi zone's unique ID

- <b>Borough (Categorical)</b> -> The Borough that the taxi zone is located in (a borough is a distinct administrative division of NYC (Manhattan, Brooklyn, Queens, The Bronx, and Staten Island))
    - https://thelegalguide.org/what-is-borough-york-city/
    - https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/8meu-9t5y/about_data


- <b>Zone (text)</b> -> Taxi zone's name
    - https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/8meu-9t5y/about_data

- <b>service_zone (categorical)</b> -> The Kind of zone (Yellow, Boro, Airports) that this location is considered
    - https://en.wikipedia.org/wiki/Taxis_of_New_York_City
    - https://en.wikipedia.org/wiki/Boro_taxi
    - https://www.nyc.gov/assets/tlc/downloads/pdf/newly_passed_rule_boro_taxi_decal.pdf

## Primary entity
A taxi zone

## Primary key
- LocationID

## Attributes for join
- `LocationID` is joined from Taxi Trips as `PULocationID` and `DOLocationID`.

## Temporal attributes
None of the attributes are temporal

## Categorical attributes
- Borough
    - see references above
- service_zone
    - see references above
- even though the `Zone` attribute has a finite set of values that are pre-defined, since every location is mapped to a specific zone with an (almost) 1-1 relationship, we will treat is a non-categorical value

## Growing attributes over time
- Zone?
    - probably not, because the zones are pre-defined