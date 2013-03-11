from __future__ import absolute_import

from nose.tools import ok_, eq_, raises, set_trace

from sunspear.aggregators.property import PropertyAggregator

from itertools import groupby


class TestPropertyAggregator(object):
    def setUp(self):
        self._aggregator = PropertyAggregator()

    def test_process(self):
        group_by_attributes = ['b']
        aggregator = PropertyAggregator(properties=group_by_attributes, activity_key='b', activity_value=3)

        data_dict = [{'a': 1, 'b': 2, 'c': {'d': 3, 'e': 4}
        }, {'a': 3, 'b': 2,  'c': {'d': 5, 'e': 4}
        }, {'a': 4, 'b': 3, 'c': {'d': 6, 'e': 4}
        }, {'a': 5, 'b': 3, 'c': {'d': 6, 'e': 4}
        }]
        expected = [
            {'a': 1, 'c': {'e': 4, 'd': 3}, 'b': 2},
            {'a': 3, 'c': {'e': 4, 'd': 5}, 'b': 2},
            {'a': [4, 5], 'c': [{'e': 4, 'd': 6}, {'e': 4, 'd': 6}], 'b': 3,
                'grouped_by_attributes': ['b'], 'grouped_by_values': [3]}
        ]

        actual = aggregator.process(data_dict, data_dict, [aggregator])
        eq_(actual, expected)

    def test_process_with_regex(self):
        group_by_attributes = ['b']
        aggregator = PropertyAggregator(properties=group_by_attributes, activity_key='b', activity_value=r"foo|bar")

        data_dict = [{'a': 1, 'b': 2, 'c': {'d': 3, 'e': 4}
        }, {'a': 3, 'b': 2,  'c': {'d': 5, 'e': 4}
        }, {'a': 4, 'b': "bar", 'c': {'d': 6, 'e': 4}
        }, {'a': 5, 'b': "bar", 'c': {'d': 6, 'e': 4}
        }]
        expected = [
            {'a': 1, 'c': {'e': 4, 'd': 3}, 'b': 2}, {'a': 3, 'c': {'e': 4, 'd': 5}, 'b': 2},
            {'a': [4, 5], 'c': [{'e': 4, 'd': 6}, {'e': 4, 'd': 6}], 'b': 'bar',
                'grouped_by_attributes': ['b'], 'grouped_by_values': ['bar']}
        ]
        actual = aggregator.process(data_dict, data_dict, [aggregator])
        eq_(actual, expected)

    def test__aggregate_activities_with_activity_key_filter(self):
        aggregator = PropertyAggregator(activity_key='b', activity_value=3)
        group_by_attributes = ['b']

        data_dict = [{'a': 1, 'b': 2, 'c': {'d': 3, 'e': 4}
        }, {'a': 3, 'b': 2,  'c': {'d': 5, 'e': 4}
        }, {'a': 4, 'b': 3, 'c': {'d': 6, 'e': 4}
        }, {'a': 5, 'b': 3, 'c': {'d': 6, 'e': 4}
        }]
        expected = [
            {'a': 1, 'c': {'e': 4, 'd': 3}, 'b': 2},
            {'a': 3, 'c': {'e': 4, 'd': 5}, 'b': 2},
            {'a': [4, 5], 'c': [{'e': 4, 'd': 6}, {'e': 4, 'd': 6}], 'b': 3,
                'grouped_by_attributes': ['b'], 'grouped_by_values': [3]}
        ]

        _raw_group_actvities = groupby(data_dict, aggregator._group_by_aggregator(group_by_attributes))
        actual = aggregator._aggregate_activities(group_by_attributes=group_by_attributes,
            grouped_activities=_raw_group_actvities)
        eq_(actual, expected)

    def test__aggregate_activities(self):
        group_by_attributes = ['b', 'c.e']

        data_dict = [{'a': 1, 'b': 2, 'c': {'d': 3, 'e': 4}
        }, {'a': 3, 'b': 2,  'c': {'d': 5, 'e': 4}
        }, {'a': 4, 'b': 2, 'c': {'d': 6, 'e': 4}
        }, {'a': 5, 'b': 3, 'c': {'d': 6, 'e': 4}
        }]
        expected = [{'a': [1, 3, 4], 'c': {'e': 4, 'd': [3, 5, 6]}, 'b': 2, 'grouped_by_attributes': ['b', 'c.e'],
            'grouped_by_values': [2, 4]}, {'a': 5, 'c': {'e': 4, 'd': 6}, 'b': 3}]

        _raw_group_actvities = groupby(data_dict, self._aggregator._group_by_aggregator(group_by_attributes))
        actual = self._aggregator._aggregate_activities(group_by_attributes=group_by_attributes, grouped_activities=_raw_group_actvities)
        eq_(actual, expected)

    def test__listify_attributes(self):
        data_dict = {
            'a': 1,
            'b': 2,
            'c': {
                'd': 3,
                'e': 4
            }
        }
        group_by_attributes = ['a', 'a.c.f', 'c.e']
        expected = {
            'a': 1,
            'b': [2],
            'c': {
                'd': [3],
                'e': 4
            }
        }

        actual = self._aggregator._listify_attributes(group_by_attributes=group_by_attributes, activity=data_dict)
        eq_(actual, (['c'], expected,))

    def test_group_by_aggregator(self):
        data_dict = {
            'a': 1,
            'b': 2,
            'c': {
                'd': 3,
                'e': 4
            }
        }
        expected = [1, 2, 4]
        actual = self._aggregator._group_by_aggregator(group_by_attributes=['a', 'b', 'a.c.f', 'c.e'])(data_dict)
        eq_(expected, actual)
