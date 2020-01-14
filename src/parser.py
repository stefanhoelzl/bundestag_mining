import re
import itertools
from pathlib import Path
import xml.etree.ElementTree as ET
import xlrd
from db import database, Delegate, DelegateName, DelegateTerm, Voting, Ballot


DelegatesPath = Path("data/delegates/data.xml")
VotingsFolder = Path("data/votings")


def parse_delegates():
    delegates = ET.parse(DelegatesPath).getroot()
    for delegate_node in delegates.findall("MDB"):
        print(f"parse delegate: {delegate_node.find('ID').text}")
        delegate = Delegate.create(
            id=int(delegate_node.find("ID").text),
            party=delegate_node.find("BIOGRAFISCHE_ANGABEN/PARTEI_KURZ").text,
            birthday=delegate_node.find(
                "BIOGRAFISCHE_ANGABEN/GEBURTSDATUM").text,
            birthplace=delegate_node.find(
                "BIOGRAFISCHE_ANGABEN/GEBURTSORT").text,
            native_country=delegate_node.find(
                "BIOGRAFISCHE_ANGABEN/GEBURTSLAND").text,
            deathday=delegate_node.find(
                "BIOGRAFISCHE_ANGABEN/STERBEDATUM").text,
            gender=delegate_node.find(
                "BIOGRAFISCHE_ANGABEN/GESCHLECHT").text,
            familiy_status=delegate_node.find(
                "BIOGRAFISCHE_ANGABEN/FAMILIENSTAND").text,
            religion=delegate_node.find("BIOGRAFISCHE_ANGABEN/RELIGION").text,
            profession=delegate_node.find("BIOGRAFISCHE_ANGABEN/BERUF").text,
            resume=delegate_node.find("BIOGRAFISCHE_ANGABEN/VITA_KURZ").text,
            publications=delegate_node.find(
                "BIOGRAFISCHE_ANGABEN/VEROEFFENTLICHUNGSPFLICHTIGES").text,
        )
        for name_node in delegate_node.findall("NAMEN/NAME"):
            DelegateName.create(
                delegate=delegate,
                first_name=name_node.find("VORNAME").text,
                last_name=name_node.find("NACHNAME").text,
                prefix=name_node.find("PRAEFIX").text,
                nobility=name_node.find("ADEL").text,
                site=name_node.find("ORTSZUSATZ").text,
                title=name_node.find("ANREDE_TITEL").text,
                used_from=name_node.find("HISTORIE_VON").text,
                used_until=name_node.find("HISTORIE_BIS").text,
            )

        for term_node in delegate_node.findall("WAHLPERIODEN/WAHLPERIODE"):
            DelegateTerm.create(
                delegate=delegate,
                term=int(term_node.find("WP").text),
                term_from=term_node.find("MDBWP_VON").text,
                term_until=term_node.find("MDBWP_BIS").text,
                electoral_district_number=term_node.find("WKR_NUMMER").text,
                electoral_district_name=term_node.find("WKR_NAME").text,
                electoral_district_state=term_node.find("WKR_LAND").text,
                state_list=term_node.find("LISTE").text,
                mandat_kind=term_node.find("MANDATSART").text,
            )


DelegateNameFixes = {
    "  ": " ",
    "h.c.": "h. c.",
    "Dr. Bernd Fabritius": "Dr. Dr. h. c. Bernd Fabritius",
    "Andre ": "André ",
    "Aydan Özoguz": "Aydan Özoğuz",
    "Sevim Dagdelen": "Sevim Dağdelen",
    "Dr. h. c. Albert Weiler": "Dr. h. c. (NUACA) Albert H. Weiler",
    "Elvan Korkmaz-Emre": "Elvan Korkmaz",
    "Michael Link (Heilbronn)": "Michael Georg Link (Heilbronn)",
    "Eva-Maria Elisabeth Schreiber": "Eva-Maria Schreiber",
    "Joana Eleonora Cotar": "Joana Cotar",
    "Albrecht Heinz Erhard Glaser": "Albrecht Glaser",
    "Konstantin Elias Kuhle": "Konstantin Kuhle",
    "Michaela Engelmeier-Heite": "Michaela Engelmeier",
    "Siegfried Kauder (Villingen-Schwenningen)": "Siegfried Kauder (Villingen-Schw.)",
    "Dr. Andreas Scheuer": "Andreas Scheuer",
    "Agnes Brugger": "Agnieszka Brugger",
    "Ronja Schmitt (Althengstett)": "Ronja Schmitt",
}

NotYetListedDelegates = {
    "Dr. Joe Weingarten": 19,
    "Charlotte Schneidewind-Hartnagel": 19,
    "Reginald Hanke": 19,
    "Dr. Saskia Ludwig": 19,
    "Sylvia Lehmann": 19,
    "Sandra Bubendorfer-Licht": 19,
    "Dr. Eberhard Brecht": 19,
    "Markus Paschke": 19,
}


def parse_votings():
    def fix_delegate_name(name):
        for corrupt, correct in DelegateNameFixes.items():
            if name != correct:
                name = name.replace(corrupt, correct)
        return name

    def mapped(rows):
        headers_mapping = {
            "Bezeichnung": "full_name",
            "Fraktion/Gruppe": "group",
            "ja": "yes",
            "nein": "no",
            "Enthaltung": "abstention",
            "ungültig": "invalid",
            "nichtabgegeben": "missing",
        }
        headers = [header.value for header in next(rows)]
        for row in rows:
            yield {
                headers_mapping[header]: cell.value
                for header, cell in zip(headers, row)
                if header in headers_mapping
            }

    def get_choice(row):
        for choice in ["yes", "no", "abstention", "invalid", "missing"]:
            if row.get(choice) == 1:
                return choice
        raise Exception(f"invalid row: {row}")

    for voting_file in sorted(VotingsFolder.iterdir()):
        match = re.match(r"(\d\d\d\d-\d\d-\d\d)_(.+)\.xlsx?", voting_file.name)
        if match:
            print(f"parse: {voting_file}")
            sheet = xlrd.open_workbook(voting_file).sheet_by_index(0)
            voting = Voting.create(
                term=int(sheet.cell_value(rowx=1, colx=0)),
                session=int(sheet.cell_value(rowx=1, colx=1)),
                voting=int(sheet.cell_value(rowx=1, colx=2)),
                date=match.group(1),
                title=match.group(2)
            )
            for row in mapped(sheet.get_rows()):
                full_name = fix_delegate_name(row["full_name"])
                distinct_delegate_ids = {
                    delegate_name.delegate_id
                    for delegate_name in DelegateName
                    .select()
                    .join(Delegate)
                    .join(DelegateTerm)
                    .where(
                        (DelegateName.full_name % f"*{full_name}*")
                        & (DelegateTerm.term == voting.term)
                    )
                }
                if len(distinct_delegate_ids) == 0:
                    if (full_name, voting.term) in NotYetListedDelegates.items():
                        continue
                    raise Exception(f"delegate not found: {full_name}")
                if len(distinct_delegate_ids) > 1:
                    raise Exception(f"ambigous delegate name: {full_name}")

                Ballot.create(
                    voting=voting,
                    delegate=Delegate.get_by_id(distinct_delegate_ids.pop()),
                    result=get_choice(row),
                    group=row["group"],
                )


if __name__ == "__main__":
    with database(create=True) as db:
        parse_delegates()
        parse_votings()
