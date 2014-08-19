# -*- coding: utf-8 -*-
""" Patch CQLengine to add new relation columns.

Related issue: https://github.com/cqlengine/cqlengine/issues/230
"""

from __future__ import (unicode_literals, print_function, absolute_import,
                        division)

from importlib import import_module

from cqlengine import columns, ValidationError
from cqlengine.models import Model


# Name of the module where all CQLengine models are defined
MODELS_MODULE = 'lib.backend.cassandra'


class ModelRefMixin(object):
    """ Utility class adding a new ``model`` parameter to any column.

    ``model`` is stored as a string in the ``related_model_name`` property.
    """

    related_model_name = None

    def __init__(self, *args, **kwargs):
        """ Registers the name of model this field is related to. """
        self.related_model_name = kwargs.pop('model', None)
        if not self.related_model_name:
            raise ValidationError("No model provided.")
        super(ModelRefMixin, self).__init__(*args, **kwargs)


class Relation(ModelRefMixin, columns.UUID):
    """ Point to a Cassandra object whose primary key is a simple UUID.
    """


class SQLRelation(ModelRefMixin, columns.UUID):
    """ Point to an external SQL-persisted object with a UUID primary key.
    """

    def to_python(self, value):
        """ Returns a string instead of a UUID object.

        As SQLAlchemy doesn't transform UUID objects at query time.

        We might ditch this UUID special transformation once
        https://github.com/zzzeek/sqlalchemy/pull/68 is settled.
        """
        return str(super(SQLRelation, self).to_python(value))


class CompositeRelation(ModelRefMixin, columns.Map):
    """ Point to a Cassandra object whose primary key is composed of fields.

    Store all components of a composed primary key as a text-based dict.
    """

    def __init__(self, *args, **kwargs):
        """ Force column type to a mapping of ASCII keys to Text values.
        """
        if kwargs.get('index', None):
            # While secondary index on mappings are supported by Cassandra,
            # s2i does not work as you might expect in this use case: the
            # column value matching is not strict. Which make them unfit for
            # primary keys.
            raise ValueError(
                'Secondary indexes on composite relations are not allowed.')
        # Columns ID must match ``[a-zA-Z0-9_]*`` regexp according CQL3 specs.
        # See: https://cassandra.apache.org/doc/cql3/CQL.html#identifiers
        # So force map keys to ASCII type.
        key_type = columns.Ascii
        # The value of primary keys components can be anything. Save them as
        # Text.
        value_type = columns.Text
        super(CompositeRelation, self).__init__(
            key_type, value_type, *args, **kwargs)

    # Cache variable storing the resolved related model class
    _related_model = None

    @property
    def related_model(self):
        """ Returns the model class from the locally stored model name.

        This is required for extra CQLengine column validation while
        serializing composed primary keys into a dict.
        """
        if not self._related_model:
            # TODO: replace direct module lookup by a proper model registry to
            # provide cleaner model registration and access.
            model_module = import_module(MODELS_MODULE)
            self._related_model = getattr(
                model_module, self.related_model_name)
        return self._related_model

    def to_python(self, value):
        """ Deserialize Cassandra record into a dict of Python objects.

        Dict's text values will be transformed into the same Python objects
        produced by related_model's columns.
        """
        # Deserialize Cassandra record into a standard Python text dict.
        value = super(CompositeRelation, self).to_python(value)

        if value:
            deserialized_values = {}

            # Transform each component of the primary key into Python objects
            for column_id, column in self.related_model._primary_keys.items():
                column_value = value[column_id]

                # First, we need to reverse the string casting we applied in
                # the ``to_database()`` / ``validate()`` methods below. We're
                # lucky, there is nothing to do here to undo the brutal string
                # casting: all columns we currently use to compose our primary
                # keys natively supports strings as natural input for their
                # ``to_python()`` method.

                # Still, we need to handle the special case of timestamp-based
                # columns.
                #
                # These columns (i.e. Date and DateTime) expect
                # to be fed with the untampered query results produced by the
                # underlaying Cassandra driver. And the latter returns a float
                # timestamp in seconds, with millisecond precision. See:
                # https://code.google.com/a/apache-extras.org/p
                # /cassandra-dbapi2/source/browse/cql/cqltypes.py#479
                #
                # But in the other direction, when saving rows into the
                # database, the Cassandra driver ask for a timestamp in
                # milliseconds, truncated to a long integer. CQLengine know
                # this so that's what is produced out of column's
                # ``to_database()`` methods. And that's what we are brutally
                # casting to a string in the ``validate()`` method below.
                #
                # Here lies our main issue with timestamp-based columns. The
                # asymmetry of Cassandra driver requires us to emulates its
                # deserialization, to please CQLengine's ``to_python()``.
                if column.db_type == 'timestamp' and isinstance(
                        column_value, basestring):
                    column_value = float(column_value) / 1000.0

                # Let the column deserialize the value as if it came directly
                # from the underlaying Cassandra driver. This will produce the
                # proper Python object, as per CQLengine implementation.
                deserialized_values[column_id] = column.to_python(column_value)

            value = deserialized_values

        return value

    def to_database(self, value):
        """ Normalize to a Cassandra-friendly text-based dict mapping.
        """
        if value:
            # Delegate serialization to validate: it already does the proper
            # job of normalizing value into a text-based dict.
            value = self.validate(value)

        return super(CompositeRelation, self).to_database(value)

    def validate(self, value):
        """ Normalize value into a dict of ASCII keys and Text values.
        """
        if value:

            # Extract primary key components from CQLengine instance.
            if isinstance(value, Model):
                value = {key: value[key]
                         for key in value._primary_keys.keys()}

            # Normalize dict values to Text values.
            if isinstance(value, dict):
                serialized_values = {}
                model = self.related_model

                # Serialize each primary key component into a Text
                for column_id, column in model._primary_keys.items():
                    column_value = value[column_id]

                    # If the column value is not already a string, transform it
                    # into its database-friendly version, to normalize data
                    # before applying our custom serialization to string.
                    if not isinstance(column_value, basestring):
                        column_value = column.to_database(column_value)
                        # Values needs to be Text in the mapping, so cast
                        # them to strings. This simple casting is enough for
                        # all the columns we currently use to compose our
                        # primary keys. In the future, If we enrich our
                        # composed keys with new types, we'll probably reach
                        # the limits of string casting and then need to add
                        # some extra manual data massaging here to properly
                        # serialize data to Text. In this case, do not forget
                        # to update the `to_python()`` method above to handle
                        # custom deserialization.
                        column_value = str(column_value) if column_value \
                            else None

                    serialized_values[column_id] = column_value

                value = serialized_values

        return super(CompositeRelation, self).validate(value)


# Register new columns
columns.Relation = Relation
columns.SQLRelation = SQLRelation
columns.CompositeRelation = CompositeRelation
