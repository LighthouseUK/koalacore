import unittest
from google.appengine.ext import testbed
import koalacore

__author__ = 'Matt'


class TestResource(koalacore.Resource):
    def __init__(self, uid, prop1, prop2, prop3, **kwargs):
        super(TestResource, self).__init__(**kwargs)

        self.uid = uid
        self.prop1 = prop1
        self.prop2 = prop2
        self.prop3 = prop3


class TestResourceWithMultivalueProperties(koalacore.Resource):
    def __init__(self, uid, prop1, prop2, prop3, list_of_values, **kwargs):
        super(TestResourceWithMultivalueProperties, self).__init__(**kwargs)

        self.uid = uid
        self.prop1 = prop1
        self.prop2 = prop2
        self.prop3 = prop3
        self.list_of_values = list_of_values


class TestSearchInterface(koalacore.GAESearchInterface):
    _index_name = 'test_index'
    _check_duplicates = False

    @classmethod
    def _convert_resource_object_to_search_record(cls, resource_object):
        search_doc = cls._search_document_model(
            doc_id=resource_object.uid,
            fields=[
                cls.atom_field(name='prop1', value=resource_object.prop1),
                cls.text_field(name='prop2', value=resource_object.prop2),
                cls.number_field(name='prop3', value=resource_object.prop3),
            ]
        )
        return search_doc


class TestSearchInterfaceWithDuplicateProperties(koalacore.GAESearchInterface):
    _index_name = 'test_index'
    _check_duplicates = True

    @classmethod
    def _convert_resource_object_to_search_record(cls, resource_object):
        search_doc = cls._search_document_model(
            doc_id=resource_object.uid,
            fields=[
                cls.atom_field(name='prop1', value=resource_object.prop1),
                cls.text_field(name='prop2', value=resource_object.prop2),
                cls.number_field(name='prop3', value=resource_object.prop3),
            ] + [cls.atom_field(name='category', value=list_value) for list_value in resource_object.list_of_values]
        )
        return search_doc


class TestGAESearchInterface(unittest.TestCase):
    def setUp(self):
        # First, create an instance of the Testbed class.
        self.testbed = testbed.Testbed()
        # Then activate the testbed, which prepares the service stubs for use.
        self.testbed.activate()
        # Next, declare which service stubs you want to use.
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_search_stub()
        # Remaining setup needed for test cases

    def tearDown(self):
        self.testbed.deactivate()

    def test_insert_search_doc(self):
        test_resource = TestResource(uid='testuid', prop1='Atom', prop2='Text field', prop3=231)
        result = TestSearchInterface.insert(test_resource)
        self.assertEqual(result[0].code, 'OK', u'Insert search doc failed')

    def test_query_search_doc(self):
        test_resource1 = TestResource(uid='testuid1', prop1='Atom', prop2='Text field1', prop3=2311)
        test_resource2 = TestResource(uid='testuid2', prop1='Atom', prop2='Text field2', prop3=2312)
        test_resource3 = TestResource(uid='testuid3', prop1='Atom', prop2='Text field3', prop3=2313)

        result = TestSearchInterface.insert_multi([test_resource1, test_resource2, test_resource3])
        self.assertEqual(result[0].code, 'OK', u'Insert search doc failed')
        self.assertEqual(result[1].code, 'OK', u'Insert search doc failed')
        self.assertEqual(result[2].code, 'OK', u'Insert search doc failed')

        search_result = TestSearchInterface.search('prop1: Atom')
        self.assertEqual(search_result.results_count, 3, u'Query returned incorrect count')
        self.assertEqual(len(search_result.results), 3, u'Query returned incorrect number of results')

    def test_duplicate_search_doc_properties(self):
        list_of_values = ['test', 'Could be anything', 'one']
        test_resource = TestResourceWithMultivalueProperties(uid='testuid1', prop1='Atom', prop2='Text field1', prop3=2311, list_of_values=list_of_values)

        result = TestSearchInterfaceWithDuplicateProperties.insert(test_resource)
        self.assertEqual(result[0].code, 'OK', u'Insert search doc failed')

        search_result = TestSearchInterfaceWithDuplicateProperties.search('prop1: Atom')
        self.assertEqual(search_result.results_count, 1, u'Query returned incorrect count')
        self.assertEqual(len(search_result.results), 1, u'Query returned incorrect number of results')
        self.assertEqual(search_result.results[0].category, list_of_values, u'Duplicate property mismatch')
