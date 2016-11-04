from sqlalchemy import Table, Column, DateTime, Integer, String, Text, MetaData, ForeignKey, UniqueConstraint
import types as custom_types


metadata = MetaData()

objects_table = Table('objects', metadata,
                      Column('id', String(32), primary_key=True),
                      Column('object_type', String(256, convert_unicode=True), nullable=False),
                      Column('display_name', String(256, convert_unicode=True)),
                      Column('content', Text),
                      Column('published', DateTime(timezone=True), nullable=False),
                      Column('updated', DateTime(timezone=True)),
                      Column('image', custom_types.JSONSmallDict(4096)),
                      Column('other_data', custom_types.JSONDict()))

activities_table = Table('activities', metadata,
                         Column('id', String(32), primary_key=True),
                         Column('verb', String(256, convert_unicode=True), nullable=False),
                         Column('actor', ForeignKey('objects.id', ondelete='CASCADE'), nullable=False),
                         Column('object', ForeignKey('objects.id', ondelete='SET NULL')),
                         Column('target', ForeignKey('objects.id', ondelete='SET NULL')),
                         Column('author', ForeignKey('objects.id', ondelete='SET NULL')),
                         Column('generator', ForeignKey('objects.id', ondelete='SET NULL')),
                         Column('provider', ForeignKey('objects.id', ondelete='SET NULL')),
                         Column('content', Text),
                         Column('published', DateTime(timezone=True), nullable=False),
                         Column('updated', DateTime(timezone=True)),
                         Column('icon', custom_types.JSONSmallDict(4096)),
                         Column('other_data', custom_types.JSONDict()))

replies_table = Table('replies', metadata,
                      Column('id', String(32), primary_key=True),
                      Column('in_reply_to', ForeignKey('activities.id', ondelete='CASCADE'), nullable=False),
                      Column('actor', ForeignKey('objects.id', ondelete='CASCADE'), nullable=False),
                      Column('published', DateTime(timezone=True), nullable=False),
                      Column('updated', DateTime(timezone=True)),
                      Column('content', Text),
                      Column('other_data', custom_types.JSONDict()))

likes_table = Table('likes', metadata,
                    Column('id', String(32), primary_key=True),
                    Column('in_reply_to', ForeignKey('activities.id', ondelete='CASCADE'), nullable=False),
                    Column('actor', ForeignKey('objects.id', ondelete='CASCADE'), nullable=False),
                    Column('published', DateTime(timezone=True), nullable=False),
                    Column('content', Text),
                    Column('other_data', custom_types.JSONDict()),
                    UniqueConstraint('actor', 'in_reply_to'))

shared_with_fields = (Column('id', Integer, primary_key=True),
                      Column('object', ForeignKey('objects.id', ondelete='CASCADE')),
                      Column('activity', ForeignKey('activities.id', ondelete='CASCADE')),
                      UniqueConstraint('object', 'activity'))

to_table = Table('to', metadata,
                 Column('id', Integer, primary_key=True),
                 Column('object', ForeignKey('objects.id', ondelete='CASCADE')),
                 Column('activity', ForeignKey('activities.id', ondelete='CASCADE')))

bto_table = Table('bto', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('object', ForeignKey('objects.id', ondelete='CASCADE')),
                  Column('activity', ForeignKey('activities.id', ondelete='CASCADE')))

cc_table = Table('cc', metadata,
                 Column('id', Integer, primary_key=True),
                 Column('object', ForeignKey('objects.id', ondelete='CASCADE')),
                 Column('activity', ForeignKey('activities.id', ondelete='CASCADE')))

bcc_table = Table('bcc', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('object', ForeignKey('objects.id', ondelete='CASCADE')),
                  Column('activity', ForeignKey('activities.id', ondelete='CASCADE')))

tables = {
    'objects': objects_table,
    'activities': activities_table,
    'replies': replies_table,
    'likes': likes_table,
    'to': to_table,
    'bto': bto_table,
    'cc': cc_table,
    'bcc': bcc_table,
}
