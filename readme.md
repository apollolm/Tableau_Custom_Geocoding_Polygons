This is a sample Python script that will modify a Tableau Custom Geocoding .hyper file that was created using Tableau's Import Geocoding UI.

See https://help.tableau.com/current/pro/desktop/en-us/custom_geocoding.htm for more details about Tableau Custom Geocoding.

There are hard-coded references in this sample to files that are assumed to exist:
municipalities.csv - A csv containing a WKT column with Polygon WKT values created in QGIS.  It also contains other text values that aren't used by this sample.

The Python script also assumes that the imported custom geographic role is called municipalities - it searches inside of the .Hyper file for a table with this name.


To run:
`run -i GeocodingData.hyper -o GeocodingData_out.hyper -w municipalities.csv `

...where

`-i ` is the Custom Geocoding Hyper file created by running the Import Custom Geocoding operation in Tableau.

`-o ` is the output hyper file that will be created

`-w` is the .csv file continaing the WKT polygons that will be added to the .hyper file. A `WKT` column must exist in this file.

