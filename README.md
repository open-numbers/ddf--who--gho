# Global Health Observatory (GHO)

source: [data query api from WHO](http://apps.who.int/gho/data/node.resources.api?lang=en)


## some indicators use a range of year in the year column

example: the datapoint for mdg_0000000013. the year column shows a
range of years.

These rows are removed from the indicators for now, we may need to
change this later.


## indicators not imported

- Some indicators' data are not numeric, just string. We didn't
  imported them.
- Some indicators' primary keys are not (country/year), for example
  there are indicators by (country,age_group,year). They are skipped
  for now.
