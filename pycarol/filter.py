from enum import Enum

class Filter:
    def __init__(self, builder):
        self.must_list = builder._must_list
        self.must_not_list = builder._must_not_list
        self.should_list = builder._should_list
        self.aggregation_list = builder._aggregation_list
        self.minimum_should_match = builder._minimum_should_match

    def json(self):
        json = {}
        json['mustList'] = [elt.json() for elt in self.must_list]
        json['mustNotList'] = [elt.json() for elt in self.must_not_list] 
        json['shouldList'] = [elt.json() for elt in self.should_list]
        json['aggregationList'] = [elt.json() for elt in self.aggregation_list]
        json['minimumShouldMatch'] = self.minimum_should_match

        return json

    class Builder:
        def __init__(self):
            self._minimum_should_match = 1
            self._must_list = []
            self._must_not_list = []
            self._should_list = []
            self._aggregation_list = []

        def must(self, must):
            self._must_list.append(must)
            return self

        def must_list(self, must_list):
            self._must_list.extend(must_list)
            return self

        def must_not(self, must_not):
            self._must_not_list.append(must_not)
            return self

        def must_not_list(self, must_not_list):
            self._must_not_list.extend(must_not_list)
            return self

        def should(self, should):
            self._should_list.append(should)
            return self

        def should_list(self, should_list):
            self._should_list.extend(should_list)
            return self

        def aggregation(self, aggregation):
            self._aggregation_list.append(aggregation)
            return self

        def aggregation_list(self, aggregation_list):
            self._aggregation_list.extend(aggregation_list)
            return self

        def minimum_should_match(self, minimum_should_match):
            self._minimum_should_match = minimum_should_match
            return self

        def build(self):
            return Filter(self)

class FilterType:
    def __init__(self, filter_type, key = None, value = None, path = None, range_values = None, values_field = None,
                 mdm_format = None, flags = None, range_start = None, range_end = None, mdm_golden_fields = False):
        self.filter_type = filter_type
        self.key = key
        self.value = value
        self.path = path
        self.range_values = range_values
        self.values_field = values_field
        self.mdm_format = mdm_format
        self.flags = flags
        self.range_start = range_start
        self.range_end = range_end
        self.mdm_golden_fields = mdm_golden_fields

    def json(self):
        json = {}
        json['mdmFilterType'] = self.filter_type.value
        if self.mdm_golden_fields:
            json['mdmKey'] = "mdmGoldenFieldAndValues." + self.key
        elif self.key:
            json['mdmKey'] = self.key
        if self.value:
            json['mdmValue'] = self.value
        if self.path:
            json['mdmPath'] = self.path
        if self.range_values:
            json['mdmRangeValues'] = self.range_values
        if self.values_field:
            json['mdmValuesField'] = self.values_field
        if self.mdm_format:
            json['mdmFormat'] = self.mdm_format
        if self.flags:
            json['mdmFlags'] = self.flags
        if self.range_start:
            json['mdmRangeStart'] = self.range_start
        if self.range_end:
            json['mdmRangeEnd'] = self.range_end

        return json

class Aggregation:
    def __init__(self, agg_type, name, params=None, sub_aggregations=None, size=10, shard_size=10, min_doc_count=0,
                 sort_by = None, sort_order = None):

        assert isinstance(sub_aggregations,list)
        assert isinstance(params, list)

        self.agg_type = agg_type
        self.name = name
        self.params = params
        self.sub_aggregations = sub_aggregations
        self.size = size
        self.shard_size = shard_size
        self.min_doc_count = min_doc_count
        self.sort_by = sort_by
        self.sort_order = sort_order

    def json(self):
        json = {}
        json['type'] = self.agg_type.value
        json['name'] = self.name
        if self.params:
            json['params'] = self.params
        if self.size:
            json['size'] = self.size
        if self.shard_size:
            json['shardSize'] = self.shard_size
        if self.min_doc_count:
            json['minDocCount'] = self.min_doc_count
        if self.sort_by:
            json['sortBy'] = self.sort_by
        if self.sort_order:
            json['sortOrder'] = self.sort_order
        if self.sub_aggregations:
            json['subAggregations'] = [agg.json() for agg in self.sub_aggregations]

        return json

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
    NESTED_MATCH_ALL_TERMS_FILTER = "NESTED_MATCH_ALL_TERMS_FILTER"
    MATCH_ANY_TERM_FILTER = "MATCH_ANY_TERM_FILTER"
    NESTED_MATCH_ANY_TERM_FILTER = "NESTED_MATCH_ANY_TERM_FILTER"
    TERM_FUZZY_FILTER = "TERM_FUZZY_FILTER"
    TERMS_FUZZY_FILTER = "TERMS_FUZZY_FILTER"
    NESTED = "NESTED"
    TYPE_FILTER = "TYPE_FILTER"
    RANGE_FILTER = "RANGE_FILTER"
    WILDCARD_FILTER = "WILDCARD_FILTER"
    WILDCARD_CUSTOM_FILTER = "WILDCARD_CUSTOM_FILTER"
    EXISTS_FILTER = "EXISTS_FILTER"
    SIMPLE_QUERY_STRING = "SIMPLE_QUERY_STRING"
    MISSING_FILTER = "MISSING_FILTER"
    GEODISTANCE_FILTER = "GEODISTANCE_FILTER"
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
