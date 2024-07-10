import requests
import bs4


import collections
import os
import sqlite3
import time

######################################################################
## Helpers

# h/t https://stackoverflow.com/a/58106295/986793
def with_attrs(**kwargs):
    def decorator(func):
        for k, v in kwargs.items():
            setattr(func, k, v)
        return func
    return decorator

def fetch_if_needed(fetcher):
    if not os.path.isfile(fetcher.output):
        fetcher()
        time.sleep(2)

def make_fetcher(url, output):

    @with_attrs(url=url, output=output)
    def fetcher():
        print("fetching: ", fetcher.url)
        r = requests.get(fetcher.url, headers={'User-Agent': 'Mozilla/5.0'})
        with open(fetcher.output, "w") as out:
            out.write(r.text)

    return fetcher


######################################################################

scrape_538_quiet_caucus = make_fetcher(
        url= "https://github.com/fivethirtyeight/data/raw/master/quiet-caucus-2024/clustered_congress.csv",
        output= "data/raw/clustered_congress.csv"
)

######################################################################


scrape_cook_pvi = make_fetcher(
        url= "https://datawrapper.dwcdn.net/IERCx/2/dataset.csv",
        output= "data/raw/cook_pvi.csv"
)

######################################################################

scrape_govtrack_cosponsor_house = make_fetcher(
        url= "https://www.govtrack.us/congress/members/report-cards/2022/house/cosponsored-other-party.csv",
        output= "data/raw/govtrack_cosponsor_house.csv"
)

scrape_govtrack_cosponsor_senate = make_fetcher(
        url= "https://www.govtrack.us/congress/members/report-cards/2022/senate/cosponsored-other-party.csv",
        output= "data/raw/govtrack_cosponsor_senate.csv"
)

######################################################################

scrape_voteview = make_fetcher(
        url= "https://voteview.com/static/data/out/members/HSall_members.csv",
        output= "data/raw/vote_view.csv"
)

######################################################################

scrape_voteview = make_fetcher(
        url= "https://voteview.com/static/data/out/members/HSall_members.csv",
        output= "data/raw/vote_view.csv"
)

######################################################################

scrape_lugar_house = make_fetcher(
        url= "https://www.thelugarcenter.org/ourwork-86.html",
        output= "data/raw/lugar_house.html"
)

scrape_lugar_senate = make_fetcher(
        url= "https://www.thelugarcenter.org/ourwork-85.html",
        output= "data/raw/lugar_senate.html"
)

######################################################################

states = [
    ('ALABAMA', 'AL'),
    ('ALASKA', 'AK'),
    ('AMERICAN SAMOA', 'AS'),
    ('ARIZONA', 'AZ'),
    ('ARKANSAS', 'AR'),
    ('CALIFORNIA', 'CA'),
    ('COLORADO', 'CO'),
    ('CONNECTICUT', 'CT'),
    ('DELAWARE', 'DE'),
    ('DISTRICT OF COLUMBIA', 'DC'),
    ('FLORIDA', 'FL'),
    ('GEORGIA', 'GA'),
    ('GUAM', 'GU'),
    ('HAWAII', 'HI'),
    ('IDAHO', 'ID'),
    ('ILLINOIS', 'IL'),
    ('INDIANA', 'IN'),
    ('IOWA', 'IA'),
    ('KANSAS', 'KS'),
    ('KENTUCKY', 'KY'),
    ('LOUISIANA', 'LA'),
    ('MAINE', 'ME'),
    ('MARYLAND', 'MD'),
    ('MASSACHUSETTS', 'MA'),
    ('MICHIGAN', 'MI'),
    ('MINNESOTA', 'MN'),
    ('MISSISSIPPI', 'MS'),
    ('MISSOURI', 'MO'),
    ('MONTANA', 'MT'),
    ('NEBRASKA', 'NE'),
    ('NEVADA', 'NV'),
    ('NEW HAMPSHIRE', 'NH'),
    ('NEW JERSEY', 'NJ'),
    ('NEW MEXICO', 'NM'),
    ('NEW YORK', 'NY'),
    ('NORTH CAROLINA', 'NC'),
    ('NORTH DAKOTA', 'ND'),
    ('NORTHERN MARIANA IS', 'MP'),
    ('OHIO', 'OH'),
    ('OKLAHOMA', 'OK'),
    ('OREGON', 'OR'),
    ('PENNSYLVANIA', 'PA'),
    ('PUERTO RICO', 'PR'),
    ('RHODE ISLAND', 'RI'),
    ('SOUTH CAROLINA', 'SC'),
    ('SOUTH DAKOTA', 'SD'),
    ('TENNESSEE', 'TN'),
    ('TEXAS', 'TX'),
    ('UTAH', 'UT'),
    ('VERMONT', 'VT'),
    ('VIRGINIA', 'VA'),
    ('VIRGIN ISLANDS', 'VI'),
    ('WASHINGTON', 'WA'),
    ('WEST VIRGINIA', 'WV'),
    ('WISCONSIN', 'WI'),
    ('WYOMING', 'WY')
]

def commonground_is_congress(politician):
    return None is not (
       politician.find(class_="U.S. House")
       or politician.find(class_="U.S. Senate")
    )

def scrape_commonground_state_index(state_tup):
    state, abbr = state_tup
    url = f"https://commongroundscorecard.org/tag/{abbr}/"
    out_dir = f"data/raw/commonground/{abbr}/"
    out_index = f"{out_dir}/index.html"
    os.makedirs(out_dir, exist_ok=True)

    fetch_if_needed(make_fetcher(url=url, output=out_index))

    with open(out_index) as f:
        soup = bs4.BeautifulSoup(f.read())

    for politician in soup.find_all(class_="politician-wrap"):
        if not commonground_is_congress(politician):
            continue
        url = politician.find('a')['href']
        stub = url.split('/')[-2]
        output = f"{out_dir}/{stub}.html"
        fetch_if_needed(make_fetcher(url=url, output=output))









if __name__ == "__main__":
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/clean", exist_ok=True)

    fetch_if_needed(scrape_538_quiet_caucus)

    fetch_if_needed(scrape_cook_pvi)

    fetch_if_needed(scrape_govtrack_cosponsor_house)
    fetch_if_needed(scrape_govtrack_cosponsor_senate)

    fetch_if_needed(scrape_voteview)

    fetch_if_needed(scrape_lugar_house)
    fetch_if_needed(scrape_lugar_senate)

    # h/t https://docs.python.org/3/library/itertools.html#itertools-recipes
    collections.deque(map(scrape_commonground_state_index, states), 0)


