cqlengine-relation-columns
==========================

Adds three new CQLengine columns to store pointers to other model instances:

* `cqlengine.columns.Relation`: for CQLengine instances whose primary key is a simple UUID, 
* `cqlengine.columns.columns.SQLRelation`: for SQLalchemy instances whose primary key is a simple UUID, 
* `cqlengine.columns.columns.CompositeRelation`: for CQLengine instances whose primary key is composed of several columns.


Example
-------

```python
import arrow
import uuid

from cqlengine import columns
from cqlengine.models import Model

import .cqlengine_relation_columns


class Dummy(Model):
    """ Dummy model, with arbitrarily complex primary key. """
    organization = columns.Text(partition_key=True)
    start_date = columns.DateTime(primary_key=True, default=arrow.utcnow)
    key = columns.UUID(primary_key=True, default=uuid.uuid4)
    info = columns.Text()


class PointingToDummy(Model):
    """ Model storing a pointer to a Dummy instance. """
    key = columns.UUID(primary_key=True, default=uuid.uuid4)
    dummy_key = columns.CompositeRelation(model='Dummy')


# Create dummy instance.
dummy_instance = Dummy.create(organization='Organization #1')

# Create a new instancepointing to the ``Dummy`` instance above.
pointing_instance = PointingToDummy.create(dummy_key=dummy_instance)

# Now the dummy instance can be fetched back from its reference stored in the
# pointing instance.
fetched_dummy = Dummy.get(**pointing_instance.dummy_key)
assert fetched_dummy == dummy_instance
```


History
-------

Originates from a private repository, to cover internal needs in my company.

This code was then released in the open while discussing issue
https://github.com/cqlengine/cqlengine/issues/230 in order to provide a proof of
concept for storing arbitrary pointers to CQLengine instances. 
