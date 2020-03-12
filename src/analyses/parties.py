import pygal
from db import database, Ballot, Voting, Delegate, DelegateName, DelegateTerm, fn


template = """
Parteien
=================

Nach Geschlecht (in %)
----------------------
{{ plot("by_gender") }}

Nach Titel (in %)
------------------
{{ plot("by_title") }}

Nach Abstimmverhalten (in %)
----------------------------
{{ plot("by_voting_habit") }}
"""


def by_gender():
    current_term = DelegateTerm.select(fn.Max(DelegateTerm.term)).scalar()

    delegates = DelegateName.select(Delegate, fn.COUNT(DelegateName.id).alias('count')).join(Delegate).join(DelegateTerm).where(
        DelegateTerm.term == current_term and DelegateTerm.term_until == None).group_by(Delegate.party, Delegate.gender).order_by(Delegate.party)

    stats = {}
    for delegate in delegates:
        stats.setdefault(delegate.delegate.party, {})[
            delegate.delegate.gender] = delegate.count
    distinct_genders = set()
    for party, genders in stats.items():
        distinct_genders = distinct_genders | set(genders)
        total = sum(genders.values())
        stats[party] = {g: (t/total) for g, t in genders.items()}

    plot_data = {}
    for party, genders in stats.items():
        for gender in distinct_genders:
            percentage = genders.get(gender, 0)
            plot_data.setdefault(gender, {})[party] = percentage

    line_chart = pygal.StackedBar()
    line_chart.x_labels = stats.keys()
    for gender, parties in plot_data.items():
        line_chart.add(gender, [float(f"{v*100:.2f}")
                                for v in parties.values()])
    return line_chart


def by_title():
    current_term = DelegateTerm.select(fn.Max(DelegateTerm.term)).scalar()

    delegates = DelegateName.select(Delegate, DelegateName, fn.COUNT(DelegateName.id).alias('count')).join(Delegate).join(DelegateTerm).where(
        DelegateTerm.term == current_term and DelegateTerm.term_until == None).group_by(Delegate.party, DelegateName.title).order_by(Delegate.party)

    stats = {}
    for delegate in delegates:
        stats.setdefault(delegate.delegate.party, {})[
            delegate.title] = delegate.count
    distinct_titles = set()
    for party, titles in stats.items():
        distinct_titles = distinct_titles | set(titles)
        total = sum(titles.values())
        stats[party] = {g: (t/total) for g, t in titles.items()}

    plot_data = {}
    for party, titles in stats.items():
        for title in distinct_titles:
            percentage = titles.get(title, 0)
            plot_data.setdefault(title, {})[party] = percentage

    line_chart = pygal.StackedBar()
    line_chart.x_labels = stats.keys()
    for title, parties in plot_data.items():
        line_chart.add(title, [float(f"{v*100:.2f}")
                               for v in parties.values()])
    return line_chart


def by_voting_habit():
    current_term = DelegateTerm.select(fn.Max(DelegateTerm.term)).scalar()
    by_parties = Ballot().select(Ballot.group, Ballot.result, fn.COUNT(Ballot.id).alias("count")).join(Voting).where(
        Voting.term == current_term).group_by(Ballot.group, Ballot.result).order_by(Ballot.group)

    stats = {}
    for result in by_parties:
        stats.setdefault(result.group, {})[
            result.result] = result.count

    distinct_habits = set()
    for party, habits in stats.items():
        distinct_habits = distinct_habits | set(habits)
        total = sum(habits.values())
        stats[party] = {g: (t/total) for g, t in habits.items()}

    plot_data = {}
    for party, habits in stats.items():
        for habit in distinct_habits:
            percentage = habits.get(habit, 0)
            plot_data.setdefault(habit, {})[party] = percentage

    line_chart = pygal.StackedBar()
    line_chart.x_labels = stats.keys()
    for habit, parties in plot_data.items():
        line_chart.add(habit, [float(f"{v*100:.2f}")
                               for v in parties.values()])
    return line_chart
