from __future__ import absolute_import, division, print_function, unicode_literals

from nose.tools import eq_, ok_, raises

from sunspear.lib.dotdict import dotdictify


class TestDotDictify(object):
    def setUp(self):
        self._test_dict = dotdictify({
            'a': 1,
            'b': 2,
            'c': 3,
            'd': {
                'e': 4,
                'f': {
                    'g': 6
                }
            }
        })

    def test_get_dict(self):
        eq_(self._test_dict["a"], 1)
        eq_(self._test_dict["b"], 2)
        eq_(self._test_dict["d.e"], 4)
        eq_(self._test_dict["d.f.g"], 6)

    @raises(KeyError)
    def test_get_dict_key_error(self):
        self._test_dict["z"]

    @raises(KeyError)
    def test_get_dict_attribute_error_nested(self):
        self._test_dict["a.z"]

    @raises(KeyError)
    def test_get_dict_key_error_multi_nested(self):
        self._test_dict["d.f.z"]

    def test_set_item_non_existing_key_succeeds(self):
        self._test_dict.z = "zed"
        eq_(self._test_dict.z, "zed")

    def test_contains(self):
        ok_("a" in self._test_dict)
        ok_("d.e" in self._test_dict)
        ok_("d.f.g" in self._test_dict)

        ok_(not ("z" in self._test_dict))
        ok_(not ("a.z" in self._test_dict))
        ok_(not ("d.f.g.z" in self._test_dict))

    def test_get(self):
        eq_(self._test_dict.get("a"), 1)
        eq_(self._test_dict.get("b"), 2)
        eq_(self._test_dict.get("d.e"), 4)
        eq_(self._test_dict.get("d.f.g"), 6)

    def test_get_default(self):
        eq_(self._test_dict.get("z"), None)
        eq_(self._test_dict.get("a.z"), None)
        eq_(self._test_dict.get("d.f.z"), None)

        eq_(self._test_dict.get("z", "zed"), "zed")
        eq_(self._test_dict.get("a.z", "zed"), "zed")
        eq_(self._test_dict.get("d.f.z", "zed"), "zed")

    def test_setdefault(self):
        eq_(self._test_dict.setdefault("a", "one"), 1)
        eq_(self._test_dict.setdefault("b", "two"), 2)
        eq_(self._test_dict.setdefault("d.e", "four"), 4)
        eq_(self._test_dict.setdefault("d.f.g", "six"), 6)

        eq_(self._test_dict.setdefault("z", "one"), "one")
        eq_(self._test_dict.setdefault("d.z", "four"), "four")
        eq_(self._test_dict.setdefault("d.f.z", "six"), "six")

        eq_(self._test_dict["z"], "one")
        eq_(self._test_dict["d.z"], "four")
        eq_(self._test_dict["d.f.z"], "six")
