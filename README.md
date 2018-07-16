# Global Health Observatory (GHO)

source: [data query api from WHO](http://apps.who.int/gho/data/node.resources.api?lang=en)


## some indicators use a range of year in the year column

example: the source file for [MDG_0000000013][1]. Some rows in the
year column shows a range of years.

These rows are removed from the indicators for now.

[1]: https://github.com/open-numbers/ddf--who--gho/blob/master/etl/source/MDG_0000000013.csv#L50

## some indicators are not imported

- Some indicators' data are not numeric, just string. We didn't
  import them.
- Some indicators' primary keys are not (country,year), for example
  there are indicators by (country,age_group,year). They are skipped
  for now.
