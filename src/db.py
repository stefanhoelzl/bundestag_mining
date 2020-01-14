from pathlib import Path
from enum import Enum
from contextlib import contextmanager
from peewee import SqliteDatabase, Model, ForeignKeyField, IntegerField, TextField, DateField, CompositeKey

Db = Path("data/bundestag.sqlite")

db = SqliteDatabase(
    str(Db),
    pragmas={
        'journal_mode': 'wal',
        'foreign_keys': 1,
        'ignore_check_constraints': 0,
        'synchronous': 0
    }
)


class DbModel(Model):
    class Meta:
        database = db


class Delegate(DbModel):
    id = IntegerField(unique=True, primary_key=True)
    party = TextField(null=True)
    gender = TextField()
    birthday = DateField()
    birthplace = TextField(null=True)
    deathday = DateField(null=True)
    native_country = TextField(null=True)
    familiy_status = TextField(null=True)
    religion = TextField(null=True)
    profession = TextField()
    publications = TextField(null=True)
    resume = TextField(null=True)

    def __eq__(self, other):
        if isinstance(other, str):
            return other in self.names
        return super().__eq__(other)


class DelegateName(DbModel):
    delegate = ForeignKeyField(Delegate, backref="names")
    first_name = TextField()
    last_name = TextField()
    full_name = TextField()
    site = TextField(null=True)
    title = TextField(null=True)
    prefix = TextField(null=True)
    nobility = TextField(null=True)
    used_from = DateField()
    used_until = DateField(null=True)

    @classmethod
    def create(cls, **kwargs):
        full_name = " ".join(
            kwargs.get(part)
            for part in [
                "title",
                "first_name",
                "nobility",
                "prefix",
                "last_name",
                "site",
            ]
            if kwargs.get(part)
        )
        return super().create(full_name=full_name, **kwargs)

    def __str__(self):
        return self.full_name


class DelegateTerm(DbModel):
    delegate = ForeignKeyField(Delegate, backref="terms")
    term = IntegerField()
    term_from = DateField()
    term_until = DateField(null=True)
    electoral_district_number = IntegerField(null=True)
    electoral_district_name = TextField(null=True)
    electoral_district_state = TextField(null=True)
    state_list = TextField(null=True)
    mandat_kind = TextField(null=True)


class Voting(DbModel):
    term = IntegerField()
    session = IntegerField()
    voting = IntegerField()
    date = DateField()
    title = TextField()


class Ballot(DbModel):
    voting = ForeignKeyField(Voting, backref="ballots")
    delegate = ForeignKeyField(Delegate, backref="ballots")
    group = TextField()
    result = TextField()


Tables = [Delegate, DelegateName, DelegateTerm, Voting, Ballot]


@contextmanager
def database(create=False):
    db.connect()
    if create:
        db.drop_tables(Tables)
        db.create_tables(Tables)

    try:
        yield db
    finally:
        db.close()
