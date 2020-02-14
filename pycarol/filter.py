from enum import Enum

class Filter:
    """

    Class responsible for creating the json queries to be used in `pycarol.Query`

    Usage:

    .. code:: python

        from pycarol.filter import TYPE_FILTER, Filter, TERM_FILTER
        json_query = Filter.Builder()\\
            .must(TYPE_FILTER(value='medicalform' + "Golden"))\\
            .must(TERM_FILTER(key='mdmGoldenFieldAndValues.status.raw',value='pending'))\\
            .must_not(TERM_FILTER(key='mdmGoldenFieldAndValues.auditedbycarol',value=True))\\
            .build().to_json()

    This will create the following json query.

    .. code:: json

        {
          'mustList': [
            {
              'mdmFilterType': 'TYPE_FILTER',
              'mdmValue': 'medicalformGolden'
            },
            {
              'mdmFilterType': 'TERM_FILTER',
              'mdmKey': 'mdmGoldenFieldAndValues.status.raw',
              'mdmValue': 'pending'
            }
          ],
          'mustNotList': [
            {
              'mdmFilterType': 'TERM_FILTER',
              'mdmKey': 'mdmGoldenFieldAndValues.auditedbycarol',
              'mdmValue': True
            }
          ],
          'shouldList': [

          ],
          'aggregationList': [

          ],
          'minimumShouldMatch': 1
        }

    Using with Aggregations:

    .. code:: python

        from pycarol.filter import MINIMUM, MAXIMUM, TYPE_FILTER, Filter, TERM_FILTER
        json_query = Filter.Builder() \\
                            .type('datamodel') \\
                            .aggregation_list([MINIMUM(name='MINIMUM', params='mdm_key'),
                                               MAXIMUM(name='MAXIMUM', params='mdm_key')]) \\
                            .build().to_json()

    .. code:: json

        {
          'mustList': [
            {
              'mdmFilterType': 'TYPE_FILTER',
              'mdmValue': 'datamodel'
            }
          ],
          'mustNotList': [

          ],
          'shouldList': [

          ],
          'aggregationList': [
            {
              'type': 'MINIMUM',
              'name': 'MINIMUM',
              'params': 'mdm_key',
              'size': 10,
              'shardSize': 10,
              'minDocCount': 0
            },
            {
              'type': 'MAXIMUM',
              'name': 'MAXIMUM',
              'params': 'mdm_key',
              'size': 10,
              'shardSize': 10,
              'minDocCount': 0
            }
          ],
          'minimumShouldMatch': 1
        }


    """
    def __init__(self, builder):
        self.must_list = builder._must_list
        self.must_not_list = builder._must_not_list
        self.should_list = builder._should_list
        self.aggregation_list = builder._aggregation_list
        self.minimum_should_match = builder._minimum_should_match

    def to_json(self):
        json = {}
        json['mustList'] = [elt.to_json() for elt in self.must_list]
        json['mustNotList'] = [elt.to_json() for elt in self.must_not_list]
        json['shouldList'] = [elt.to_json() for elt in self.should_list]
        json['aggregationList'] = [elt.to_json() for elt in self.aggregation_list]
        json['minimumShouldMatch'] = self.minimum_should_match

        return json

    class Builder:
        def __init__(self, key_prefix=""):
            self._minimum_should_match = 1
            self._must_list = []
            self._must_not_list = []
            self._should_list = []
            self._aggregation_list = []
            self.key_prefix = key_prefix

        def type(self, value):
            self._must_list.append(TYPE_FILTER(value=value))
            return self

        def must(self, must):
            must.set_key_prefix(self.key_prefix)
            self._must_list.append(must)
            return self

        def must_list(self, must_list):
            assert isinstance(must_list, list)
            for must in must_list:
                must.set_key_prefix(self.key_prefix)
            self._must_list.extend(must_list)
            return self

        def must_not(self, must_not):
            must_not.set_key_prefix(self.key_prefix)
            self._must_not_list.append(must_not)
            return self

        def must_not_list(self, must_not_list):
            assert isinstance(must_not_list, list)
            for must_not in must_not_list:
                must_not.set_key_prefix(self.key_prefix)
            self._must_not_list.extend(must_not_list)
            return self

        def should(self, should):
            should.set_key_prefix(self.key_prefix)
            self._should_list.append(should)
            return self

        def should_list(self, should_list):
            assert isinstance(should_list, list)
            for should in should_list:
                should.set_key_prefix(self.key_prefix)
            self._should_list.extend(should_list)
            return self

        def aggregation(self, aggregation):
            self._aggregation_list.append(aggregation)
            return self

        def aggregation_list(self, aggregation_list):
            assert isinstance(aggregation_list, list)
            self._aggregation_list.extend(aggregation_list)
            return self

        def minimum_should_match(self, minimum_should_match):
            self._minimum_should_match = minimum_should_match
            return self

        def build(self):
            return Filter(self)

class FilterType:
    def __init__(self, filter_type, key = None, value = None, path = None, range_values = None, values_field = None,
                 mdm_format = None, flags = None, range_start = None, range_end = None, values_query = None):
        self.filter_type = filter_type
        self.key = key
        self.value = value
        self.path = path
        if range_values is not None:
            assert isinstance(range_values, list)
        self.range_values = range_values
        self.values_field = values_field
        self.mdm_format = mdm_format
        self.flags = flags
        self.range_start = range_start
        self.range_end = range_end
        if values_query is not None:
            assert values_field is not None
            assert isinstance(values_query, FilterType)
        self.values_query = values_query

    def set_key_prefix(self, key_prefix):
        if key_prefix:
            if self.key:
                self.key = key_prefix + '.' + self.key
            if self.values_field:
                self.values_field = key_prefix + '.' + self.values_field

    def to_json(self):
        json = {'mdmFilterType': self.filter_type.value}
        if self.key:
            json['mdmKey'] = self.key
        if self.value is not None:
            json['mdmValue'] = self.value
        if self.path:
            json['mdmPath'] = self.path
        if self.range_values:
            json['mdmRangeValues'] = self.range_values
        if self.values_field:
            json['mdmValuesField'] = self.values_field
        if self.mdm_format:
            json['mdmFormat'] = self.mdm_format
        if self.flags is not None:
            json['mdmFlags'] = self.flags
        if self.range_start is not None:
            json['mdmRangeStart'] = self.range_start
        if self.range_end is not None:
            json['mdmRangeEnd'] = self.range_end
        if self.values_query is not None:
            json['mdmValuesQuery'] = self.values_query.to_json()

        return json

class TYPE_FILTER(FilterType):
    def __init__(self, value, values_field = None, values_query = None):
        super().__init__(filter_type = FT.TYPE_FILTER, value=value, values_field=values_field, values_query=values_query)

class BOOL_FILTER(FilterType):
    def __init__(self, key, value, values_field = None, values_query = None):
        super().__init__(filter_type = FT.BOOL_FILTER, key=key, value=value, values_field=values_field, values_query=values_query)

class TERM_FILTER(FilterType):
    def __init__(self, key, value, values_field = None, values_query = None):
        super().__init__(filter_type = FT.TERM_FILTER, key=key, value=value, values_field=values_field, values_query=values_query)

class TERMS_FILTER(FilterType):
    def __init__(self, key, value, values_field = None, values_query = None):
        super().__init__(filter_type = FT.TERMS_FILTER, key=key, value=value, values_field=values_field, values_query=values_query)

class RANGE_FILTER(FilterType):
    def __init__(self, key, value, values_field = None, values_query = None):
        super().__init__(filter_type = FT.RANGE_FILTER, key=key, value=value, values_field=values_field, values_query=values_query)

class MATCH_FILTER(FilterType):
    def __init__(self, key, value, values_field = None, values_query = None):
        super().__init__(filter_type = FT.MATCH_FILTER, key=key, value=value, values_field=values_field, values_query=values_query)

class MATCH_ALL_TERMS_FILTER(FilterType):
    def __init__(self, key, value, values_field = None, values_query = None):
        super().__init__(filter_type = FT.MATCH_ALL_TERMS_FILTER, key=key, value=value, values_field=values_field, values_query=values_query)

class MATCH_ANY_TERM_FILTER(FilterType):
    def __init__(self, key, value, values_field = None, values_query = None):
        super().__init__(filter_type = FT.MATCH_ALL_TERMS_FILTER, key=key, value=value, values_field=values_field, values_query=values_query)

class TERM_FUZZY_FILTER(FilterType):
    def __init__(self, key, value, values_field = None, values_query = None):
        super().__init__(filter_type = FT.TERM_FUZZY_FILTER, key=key, value=value, values_field=values_field, values_query=values_query)

class TERMS_FUZZY_FILTER(FilterType):
    def __init__(self, key, value, values_field = None, values_query = None):
        super().__init__(filter_type = FT.TERMS_FUZZY_FILTER, key=key, value=value, values_field=values_field, values_query=values_query)

class WILDCARD_FILTER(FilterType):
    def __init__(self, key, value, values_field = None, values_query = None):
        super().__init__(filter_type = FT.WILDCARD_FILTER, key=key, value=value, values_field=values_field, values_query=values_query)

class WILDCARD_CUSTOM_FILTER(FilterType):
    def __init__(self, key, value, values_field = None, values_query = None):
        super().__init__(filter_type = FT.WILDCARD_CUSTOM_FILTER, key=key, value=value, values_field=values_field, values_query=values_query)

class EXISTS_FILTER(FilterType):
    def __init__(self, key, values_field = None, values_query = None):
        super().__init__(filter_type = FT.EXISTS_FILTER, key=key, values_field=values_field, values_query=values_query)

class SIMPLE_QUERY_STRING(FilterType):
    def __init__(self, key, value, values_field = None, values_query = None):
        super().__init__(filter_type = FT.SIMPLE_QUERY_STRING, key=key, value=value, values_field=values_field, values_query=values_query)

class MISSING_FILTER(FilterType):
    def __init__(self, key, values_field = None, values_query = None):
        super().__init__(filter_type = FT.MISSING_FILTER, key=key, values_field=values_field, values_query=values_query)

class GEODISTANCE_FILTER(FilterType):
    def __init__(self, path, key, range_values, values_field = None, values_query = None):
        super().__init__(filter_type = FT.GEODISTANCE_FILTER, path=path, key=key, range_values=range_values, values_field=values_field, values_query=values_query)

class NESTED(FilterType):
    def __init__(self, key, value, path, values_field = None, values_query = None):
        super().__init__(filter_type = FT.NESTED, key=key, value=value, path=path, values_field=values_field, values_query=values_query)

class NESTED_TERM_FILTER(FilterType):
    def __init__(self, key, value, path, values_field = None, values_query = None):
        super().__init__(filter_type = FT.NESTED_TERM_FILTER, key=key, value=value, path=path, values_field=values_field, values_query=values_query)

class NESTED_TERMS_FILTER(FilterType):
    def __init__(self, key, value, path, values_field = None, values_query = None):
        super().__init__(filter_type = FT.NESTED_TERMS_FILTER, key=key, value=value, path=path, values_field=values_field, values_query=values_query)

class NESTED_RANGE_FILTER(FilterType):
    def __init__(self, key, value, path, values_field = None, values_query = None):
        super().__init__(filter_type = FT.NESTED_RANGE_FILTER, key=key, value=value, path=path, values_field=values_field, values_query=values_query)

class NESTED_MATCH_ALL_TERMS_FILTER(FilterType):
    def __init__(self, key, value, path, values_field = None, values_query = None):
        super().__init__(filter_type = FT.NESTED_MATCH_ALL_TERMS_FILTER, key=key, value=value, path=path, values_field=values_field, values_query=values_query)

class NESTED_MATCH_ANY_TERM_FILTER(FilterType):
    def __init__(self, key, value, path, values_field = None, values_query = None):
        super().__init__(filter_type = FT.NESTED_MATCH_ANY_TERM_FILTER, key=key, value=value, path=path, values_field=values_field, values_query=values_query)

class NESTED_TERM_FUZZY_FILTER(FilterType):
    def __init__(self, key, value, path, values_field = None, values_query = None):
        super().__init__(filter_type = FT.NESTED_TERM_FUZZY_FILTER, key=key, value=value, path=path, values_field=values_field, values_query=values_query)

class NESTED_TERMS_FUZZY_FILTER(FilterType):
    def __init__(self, key, value, path, values_field = None, values_query = None):
        super().__init__(filter_type = FT.NESTED_TERMS_FUZZY_FILTER, key=key, value=value, path=path, values_field=values_field, values_query=values_query)

class NESTED_WILDCARD_FILTER(FilterType):
    def __init__(self, key, value, path, values_field = None, values_query = None):
        super().__init__(filter_type = FT.NESTED_WILDCARD_FILTER, key=key, value=value, path=path, values_field=values_field, values_query=values_query)

class NESTED_WILDCARD_CUSTOM_FILTER(FilterType):
    def __init__(self, key, value, path, values_field = None, values_query = None):
        super().__init__(filter_type = FT.NESTED_WILDCARD_CUSTOM_FILTER, key=key, value=value, path=path, values_field=values_field, values_query=values_query)

class NESTED_EXISTS_FILTER(FilterType):
    def __init__(self, key, path, values_field = None, values_query = None):
        super().__init__(filter_type = FT.NESTED_EXISTS_FILTER, key=key, path=path, values_field=values_field, values_query=values_query)

class NESTED_SIMPLE_QUERY_STRING(FilterType):
    def __init__(self, key, value, path, values_field = None, values_query = None):
        super().__init__(filter_type = FT.NESTED_SIMPLE_QUERY_STRING, key=key, value=value, path=path, values_field=values_field, values_query=values_query)

class NESTED_MISSING_FILTER(FilterType):
    def __init__(self, key, value, path, values_field = None, values_query = None):
        super().__init__(filter_type = FT.NESTED_MISSING_FILTER, key=key, value=value, path=path, values_field=values_field, values_query=values_query)

class NESTED_GEODISTANCE_FILTER(FilterType):
    def __init__(self, path, key, range_values, values_field = None, values_query = None):
        super().__init__(filter_type = FT.NESTED_GEODISTANCE_FILTER, path=path, key=key, range_values=range_values, values_field=values_field, values_query=values_query)

class Aggregation:
    def __init__(self, agg_type, name, params=None, sub_aggregations=None, size=10, shard_size=10, min_doc_count=0, sort_by = None, sort_order = None, query_param = None):
        self.agg_type = agg_type
        self.name = name
        self.params = params
        if sub_aggregations is not None:
            assert isinstance(sub_aggregations,list), 'sub_aggregations must be a list'
        self.sub_aggregations = sub_aggregations
        self.size = size
        self.shard_size = shard_size
        self.min_doc_count = min_doc_count
        self.sort_by = sort_by
        self.sort_order = sort_order
        self.query_param = query_param

    def to_json(self):
        json = {}
        json['type'] = self.agg_type.value
        json['name'] = self.name
        if self.params:
            json['params'] = self.params
        if self.size:
            json['size'] = self.size
        if self.shard_size:
            json['shardSize'] = self.shard_size
        if self.min_doc_count is not None:
            json['minDocCount'] = self.min_doc_count
        if self.sort_by:
            json['sortBy'] = self.sort_by
        if self.sort_order:
            json['sortOrder'] = self.sort_order
        if self.sub_aggregations:
            json['subAggregations'] = [agg.to_json() for agg in self.sub_aggregations]
        if self.query_param is not None:
            json['queryParam'] = self.query_param.to_json()

        return json

class TERM(Aggregation):
    def __init__(self, name='term', params=None, sub_aggregations=None, size=10, shard_size=10, min_doc_count=0, sort_by=None, sort_order=None):
        super().__init__(agg_type= AGG.TERM, name=name, params=params, sub_aggregations=sub_aggregations, size=size, shard_size=shard_size, min_doc_count=min_doc_count, sort_by=sort_by, sort_order=sort_order)

class TERMS(Aggregation):
    def __init__(self, name='terms', params=None, sub_aggregations=None, size=10, shard_size=10, min_doc_count=0, sort_by=None, sort_order=None):
        super().__init__(agg_type= AGG.TERMS, name=name, params=params, sub_aggregations=sub_aggregations, size=size, shard_size=shard_size, min_doc_count=min_doc_count, sort_by=sort_by, sort_order=sort_order)

class NESTED(Aggregation):
    def __init__(self, name='nested', params=None, sub_aggregations=None, size=10, shard_size=10, min_doc_count=0, sort_by=None, sort_order=None):
        super().__init__(agg_type= AGG.NESTED, name=name, params=params, sub_aggregations=sub_aggregations, size=size, shard_size=shard_size, min_doc_count=min_doc_count, sort_by=sort_by, sort_order=sort_order)

class FILTER(Aggregation):
    def __init__(self, name='filter', params=None, sub_aggregations=None, size=10, shard_size=10, min_doc_count=0, sort_by=None, sort_order=None, query_param=None):
        super().__init__(agg_type= AGG.FILTER, name=name, params=params, sub_aggregations=sub_aggregations, size=size, shard_size=shard_size, min_doc_count=min_doc_count, sort_by=sort_by, sort_order=sort_order, query_param=query_param)

class MINIMUM(Aggregation):
    def __init__(self, name='minimum', params=None, sub_aggregations=None, size=10, shard_size=10, min_doc_count=0, sort_by=None, sort_order=None):
        super().__init__(agg_type= AGG.MINIMUM, name=name, params=params, sub_aggregations=sub_aggregations, size=size, shard_size=shard_size, min_doc_count=min_doc_count, sort_by=sort_by, sort_order=sort_order)

class MAXIMUM(Aggregation):
    def __init__(self, name='maximum', params=None, sub_aggregations=None, size=10, shard_size=10, min_doc_count=0, sort_by=None, sort_order=None):
        super().__init__(agg_type= AGG.MAXIMUM, name=name, params=params, sub_aggregations=sub_aggregations, size=size, shard_size=shard_size, min_doc_count=min_doc_count, sort_by=sort_by, sort_order=sort_order)

class AVERAGE(Aggregation):
    def __init__(self, name='average', params=None, sub_aggregations=None, size=10, shard_size=10, min_doc_count=0, sort_by=None, sort_order=None):
        super().__init__(agg_type= AGG.AVERAGE, name=name, params=params, sub_aggregations=sub_aggregations, size=size, shard_size=shard_size, min_doc_count=min_doc_count, sort_by=sort_by, sort_order=sort_order)

class COUNT(Aggregation):
    def __init__(self, name='count', params=None, sub_aggregations=None, size=10, shard_size=10, min_doc_count=0, sort_by=None, sort_order=None):
        super().__init__(agg_type= AGG.COUNT, name=name, params=params, sub_aggregations=sub_aggregations, size=size, shard_size=shard_size, min_doc_count=min_doc_count, sort_by=sort_by, sort_order=sort_order)

class SUM(Aggregation):
    def __init__(self, name='sum', params=None, sub_aggregations=None, size=10, shard_size=10, min_doc_count=0, sort_by=None, sort_order=None):
        super().__init__(agg_type= AGG.SUM, name=name, params=params, sub_aggregations=sub_aggregations, size=size, shard_size=shard_size, min_doc_count=min_doc_count, sort_by=sort_by, sort_order=sort_order)

class STATS(Aggregation):
    def __init__(self, name='stats', params=None, sub_aggregations=None, size=10, shard_size=10, min_doc_count=0, sort_by=None, sort_order=None):
        super().__init__(agg_type= AGG.STATS, name=name, params=params, sub_aggregations=sub_aggregations, size=size, shard_size=shard_size, min_doc_count=min_doc_count, sort_by=sort_by, sort_order=sort_order)

class EXTENDED_STATS(Aggregation):
    def __init__(self, name='extendedStats', params=None, sub_aggregations=None, size=10, shard_size=10, min_doc_count=0, sort_by=None, sort_order=None):
        super().__init__(agg_type= AGG.EXTENDED_STATS, name=name, params=params, sub_aggregations=sub_aggregations, size=size, shard_size=shard_size, min_doc_count=min_doc_count, sort_by=sort_by, sort_order=sort_order)

class DATE_HISTOGRAM(Aggregation):
    def __init__(self, name='dateHistogram', params=None, sub_aggregations=None, size=10, shard_size=10, min_doc_count=0, sort_by=None, sort_order=None):
        super().__init__(agg_type= AGG.DATE_HISTOGRAM, name=name, params=params, sub_aggregations=sub_aggregations, size=size, shard_size=shard_size, min_doc_count=min_doc_count, sort_by=sort_by, sort_order=sort_order)

class HISTOGRAM(Aggregation):
    def __init__(self, name='histogram', params=None, sub_aggregations=None, size=10, shard_size=10, min_doc_count=0, sort_by=None, sort_order=None):
        super().__init__(agg_type= AGG.HISTOGRAM, name=name, params=params, sub_aggregations=sub_aggregations, size=size, shard_size=shard_size, min_doc_count=min_doc_count, sort_by=sort_by, sort_order=sort_order)

class RANGE(Aggregation):
    def __init__(self, name='range', params=None, sub_aggregations=None, size=10, shard_size=10, min_doc_count=0, sort_by=None, sort_order=None):
        super().__init__(agg_type= AGG.RANGE, name=name, params=params, sub_aggregations=sub_aggregations, size=size, shard_size=shard_size, min_doc_count=min_doc_count, sort_by=sort_by, sort_order=sort_order)

class CARDINALITY(Aggregation):
    def __init__(self, name='cardinality', params=None, sub_aggregations=None, size=10, shard_size=10, min_doc_count=0, sort_by=None, sort_order=None):
        super().__init__(agg_type= AGG.CARDINALITY, name=name, params=params, sub_aggregations=sub_aggregations, size=size, shard_size=shard_size, min_doc_count=min_doc_count, sort_by=sort_by, sort_order=sort_order)

class AGG(Enum):
    TERM = "TERM"
    TERMS = "TERMS"
    NESTED = "NESTED"
    FILTER = "FILTER"
    MINIMUM = "MINIMUM"
    MAXIMUM = "MAXIMUM"
    AVERAGE = "AVERAGE"
    COUNT = "COUNT"
    SUM = "SUM"
    STATS = "STATS"
    EXTENDED_STATS = "EXTENDED_STATS"
    DATE_HISTOGRAM = "DATE_HISTOGRAM"
    HISTOGRAM = "HISTOGRAM"
    RANGE = "RANGE"
    CARDINALITY = "CARDINALITY"


class FT(Enum):
    TERM_FILTER = "TERM_FILTER"
    BOOL_FILTER = "BOOL_FILTER"
    TERMS_FILTER = "TERMS_FILTER"
    MATCH_ALL_FILTER = "MATCH_ALL_FILTER"
    MATCH_FILTER = "MATCH_FILTER"
    MATCH_ALL_TERMS_FILTER = "MATCH_ALL_TERMS_FILTER"
    MATCH_ANY_TERM_FILTER = "MATCH_ANY_TERM_FILTER"
    TERM_FUZZY_FILTER = "TERM_FUZZY_FILTER"
    TERMS_FUZZY_FILTER = "TERMS_FUZZY_FILTER"
    TYPE_FILTER = "TYPE_FILTER"
    RANGE_FILTER = "RANGE_FILTER"
    WILDCARD_FILTER = "WILDCARD_FILTER"
    WILDCARD_CUSTOM_FILTER = "WILDCARD_CUSTOM_FILTER"
    EXISTS_FILTER = "EXISTS_FILTER"
    SIMPLE_QUERY_STRING = "SIMPLE_QUERY_STRING"
    MISSING_FILTER = "MISSING_FILTER"
    GEODISTANCE_FILTER = "GEODISTANCE_FILTER"
    NESTED = "NESTED"
    NESTED_MATCH_ALL_TERMS_FILTER = "NESTED_MATCH_ALL_TERMS_FILTER"
    NESTED_MATCH_ANY_TERM_FILTER = "NESTED_MATCH_ANY_TERM_FILTER"
    NESTED_TERM_FILTER = "NESTED_TERM_FILTER"
    NESTED_TERMS_FILTER = "NESTED_TERMS_FILTER"
    NESTED_MATCH_ALL_FILTER = "NESTED_MATCH_ALL_FILTER"
    NESTED_MATCH_FILTER = "NESTED_MATCH_FILTER"
    NESTED_TERM_FUZZY_FILTER = "NESTED_TERM_FUZZY_FILTER"
    NESTED_TERMS_FUZZY_FILTER = "NESTED_TERMS_FUZZY_FILTER"
    NESTED_RANGE_FILTER = "NESTED_RANGE_FILTER"
    NESTED_WILDCARD_FILTER = "NESTED_WILDCARD_FILTER"
    NESTED_WILDCARD_CUSTOM_FILTER = "NESTED_WILDCARD_CUSTOM_FILTER"
    NESTED_SIMPLE_QUERY_STRING = "NESTED_SIMPLE_QUERY_STRING"
    NESTED_GEODISTANCE_FILTER = "NESTED_GEODISTANCE_FILTER"
    NESTED_EXISTS_FILTER = "NESTED_EXISTS_FILTER"
    NESTED_MISSING_FILTER = "NESTED_MISSING_FILTER"
