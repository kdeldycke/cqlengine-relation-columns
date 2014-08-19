# -*- coding: utf-8 -*-
""" Test relation columns.
"""

import math
import uuid
from unittest import TestCase

import arrow
from cqlengine import columns
from cqlengine.models import Model

import .cqlengine_relation_columns


def truncate_to_milliseconds(date_time):
    """ Truncate datetime to milliseconds and return an arrow object.

    Used to emulate cqlengine's datetime conversion from Python to Cassandra.
    """
    date_time = arrow.get(date_time)
    milliseconds = math.trunc(date_time.microsecond / 1000.0) * 1000
    return date_time.replace(microsecond=milliseconds)


class TestCompositeRelationColumn(TestCase):

    def test_column_io(self):

        class ForeignModel(Model):
            organization = columns.Text(partition_key=True)
            start_date = columns.DateTime(primary_key=True)
            key = columns.UUID(primary_key=True, default=uuid.uuid4)
            info = columns.Text()

        class TestCompositeRelationColumn(Model):
            key = columns.UUID(primary_key=True, default=uuid.uuid4)
            foreign_key = columns.CompositeRelation(model='ForeignModel')

        self.assertEquals(ForeignModel.objects.count(), 0)
        self.assertEquals(TestCompositeRelationColumn.objects.count(), 0)

        now = arrow.utcnow()

        # Check initial value
        test_instance = TestCompositeRelationColumn.create()
        self.assertEquals(test_instance.foreign_key, {})

        # Check proper transformation of instance's primary key component's
        # values into a dict.
        foreign_instance = ForeignModel.create(
            organization='Dummy organization',
            start_date=now,
        )
        test_instance.update(foreign_key=foreign_instance)
        self.assertIsInstance(test_instance.foreign_key, dict)
        # Test serialization to raw strings.
        self.assertEquals(test_instance.foreign_key, {
            'organization': foreign_instance.organization,
            'start_date': str(long(truncate_to_milliseconds(
                foreign_instance.start_date).float_timestamp * 1000)),
            'key': str(foreign_instance.key),
        })
        # CompositeRelation column doesn't cast primary keys components on the
        # fly. It needs reload.
        test_instance = TestCompositeRelationColumn.get(test_instance.key)
        self.assertIsInstance(test_instance.foreign_key, dict)
        self.assertEquals(test_instance.foreign_key, {
            'organization': foreign_instance.organization,
            'start_date': truncate_to_milliseconds(
                foreign_instance.start_date),
            'key': foreign_instance.key,
        })

        # Check that CompositeRelation deserialize primary key components into
        # their original types.
        self.assertIsInstance(test_instance.foreign_key['organization'],
                              basestring)
        self.assertIsInstance(test_instance.foreign_key['start_date'],
                              arrow.arrow.Arrow)
        self.assertIsInstance(test_instance.foreign_key['key'],
                              uuid.UUID)

        # Check that a related object can be fetched from the raw dict
        fetched_model = ForeignModel.filter(**test_instance.foreign_key).get()
        self.assertDictContainsSubset({
            'organization': foreign_instance.organization,
            'start_date': truncate_to_milliseconds(
                foreign_instance.start_date),
            'key': foreign_instance.key,
        }, dict(fetched_model.items()))
