import itertools
import shutil
import hashlib
import re
from datetime import date
from typing import NamedTuple
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup
import requests

Domain = "https://www.bundestag.de"

VotingsListUrl = "{domain}/ajax/filterlist/de/parlament/plenum/abstimmung/liste/462112-462112/h_60ffc88993d8146490048cae8be92856?limit={limit}&noFilterSet=true&offset={offset}"
VotingsOverviewUrl = "{domain}/abstimmung"
VotingsLimit = 30

DataFolder = Path("data")
VotingsFolder = DataFolder / "votings"


class Topic(NamedTuple):
    index: int
    name: str


class VotingResultsFile(NamedTuple):
    url: str
    voting_date: date
    title: str

    def _file_type(self) -> str:
        return Path(self.url).suffix

    @property
    def filename(self) -> str:
        return f"{self.voting_date}_{self.title}{self._file_type()}"

    def download(self):
        with requests.get(self.url, stream=True) as req:
            with open(str(VotingsFolder / self.filename), mode="wb") as dest_file:
                shutil.copyfileobj(req.raw, dest_file)

        if req.status_code == 200:
            print(f"downloaded {self.url} -> {self.filename}", flush=True)
        else:
            print(f"failed {self.url}")


def make_soup(url: str):
    with requests.get(url) as req:
        assert req.status_code == 200, req
    return BeautifulSoup(req.text, 'html.parser')


def votings():
    def sanitize(text: str):
        missing_dates = {
            "Bundeswehreinsatz ACTIVE ENDEAVOUR (OAE)": "19.12.2014",
            "Bundeswehreinsatz in Afghanistan (RSM)": "19.12.2014",
            "Änderung des Bundesdatenschutzgesetzes": "19.12.2014",
            "Änderung des Bundesdatenschutzgesetzes - Änderungsantrag": "19.12.2014",
        }
        wrong_dates = {
            "28.04.206": "28.04.2016",
            "27.06.20130": "27.06.2013",
        }

        if text in missing_dates:
            return f"{missing_dates[text]}: {text}"
        for wrong_date, corecct_date in wrong_dates.items():
            if text.startswith(wrong_date):
                return text.replace(wrong_date, corecct_date, 1)
        return text

    for offset in itertools.count(0, VotingsLimit):
        url = VotingsListUrl.format(
            domain=Domain, offset=offset, limit=VotingsLimit
        )
        soup = make_soup(url)
        if soup.h3:
            return

        for div in soup.find_all("div", "bt-documents-description"):
            for link in div.find_all("a"):
                href = link.get('href')
                if href.endswith("xlsx") or href.endswith("xls"):
                    text = sanitize(div.p.strong.text.strip())
                    match = re.match(r"(\d\d).(\d\d).(\d\d\d\d): (.*)", text)

                    if match is None:
                        raise Exception(text, offset)

                    voting_date = date(
                        int(match.group(3)), int(
                            match.group(2)), int(match.group(1))
                    )
                    title = match.group(4)

                    yield VotingResultsFile(f"{Domain}{href}", voting_date, title)
                    break


def cleanup():
    if DataFolder.exists():
        shutil.rmtree(str(DataFolder))
    DataFolder.mkdir(exist_ok=True)
    VotingsFolder.mkdir(exist_ok=True)


if __name__ == "__main__":
    cleanup()
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(lambda v: v.download(), votings())
