from sqlalchemy import Table, Column, DateTime, Integer, String, Text, MetaData, ForeignKey, UniqueConstraint
import types as custom_types


metadata = MetaData()

objects_table = Table('objects', metadata,
                      Column('id', String, primary_key=True),
                      Column('object_type', String, nullable=False),
                      Column('display_name', String),
                      Column('display_name', String),
                      Column('content', Text),
                      Column('published', DateTime, nullable=False),
                      Column('image'), custom_types.JSONSmallDict(4096),
                      Column('other_data'), custom_types.JSONDict)

activities_table = Table('activities', metadata,
                         Column('id', String, primary_key=True),
                         Column('verb', String, nullable=False),
                         Column('actor', ForeignKey('objects.id'), nullable=False),
                         Column('object', ForeignKey('objects.id')),
                         Column('target', ForeignKey('objects.id')),
                         Column('author', ForeignKey('objects.id')),
                         Column('generator', String),
                         Column('provider', String),
                         Column('content', Text),
                         Column('published', DateTime, nullable=False),
                         Column('updated', DateTime),
                         Column('icon'), custom_types.JSONSmallDict(4096),
                         Column('other_data'), custom_types.JSONDict)

subitem_fields = (Column('id', String, primary_key=True),
                  Column('in_reply_to', ForeignKey('activities.id'), nullable=False),
                  Column('published', DateTime, nullable=False),
                  Column('actor', ForeignKey('objects.id'), nullable=False),
                  Column('content', Text),
                  UniqueConstraint('actor', 'in_reply_to')
                  )

replies_table = Table('replies', metadata, **subitem_fields)

likes_table = Table('likes', metadata, **subitem_fields)

shared_with_fields = (Column('id', Integer, primary_key=True),
                      Column('object', ForeignKey('objects.id')),
                      Column('activity', ForeignKey('activities.id')),
                      UniqueConstraint('object', 'activity'))

to_table = Table('to', metadata, **shared_with_fields)

bto_table = Table('bto', metadata, **shared_with_fields)

cc_table = Table('cc', metadata, **shared_with_fields)

bcc_table = Table('bcc', metadata, **shared_with_fields)

