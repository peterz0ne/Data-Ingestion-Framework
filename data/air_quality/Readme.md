# Data Description
- https://aqs.epa.gov/aqsweb/airdata/FileFormats.html#_format_5

## Primary entity
Hourly air quality indicator for a specific location

## Primary key
- Longitude
- Latitude
- Datum?
- Time GMT
- Date GMT
(or create a new one)

TODO: create a question on canvas

- If we believe that a primary key cannot be guaranteed unique for a dataset, can we generate our own primary key?

## Attributes for join
- Date, Time GMT/Local: Can be joined with the taxi rides (tpep_pickup_datetime, tpep_dropoff_datetime), after the times in the taxi rides table get transformed to date-hour
- (Latitude, Longitude, Datum?): Can be joined with the taxi rides, to get the air quality data in a specific location. To do that, we probably need to add a new column to the air quality table, that contains the LocationID of each (lat,long) location, in order for it to be able to join with the taxi rides.

## Temporal attributes
- Date GMT/Local
- Time GMT/Local
- Date of Last Change

## Categorical attributes
- State Code
- County Code
- Site Num
- Parameter Code
- POC
- Parameter Name
- Units of Measure
- Qualifier
- Method Code
- Method Type
- State Name
- County Name

Note: The reasoning can be found in the [analysis](./analysis.ipynb) script. For example, `Site Num` can clearly be considered a categorical variable since the same values are repeated to often. But, long,lat,Datum,time and MDL are not considered categories, even though they are repeated so often as well (this means that probably the monitors are in constant locations, but that shouldn't encourage us to mark the long,lat variables as categories)

## Growing attributes over time
- Date GMT/Local
- Time GMT/Local
- Latitude
- Longitude
- Datum?
- Date of Last Change
- Uncertainty
- Sample Measurement
- MDL
- Uncertainty