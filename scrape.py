import requests
import bs4


import collections
import csv
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

def write_538(con, row):
    con.execute("""
    insert into five_thirty_eight_quiet_caucus (
        icpsr, name, last_name , party , district , terms_pct , margin_2020 , cluster , agreement_score , progressive , new_dems , blue_dogs , problem_solvers , RMSP , governance , study , freedom , dw_nominate_dim1 , dw_nominate_dim2 , notes
    ) values (
         ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?, ?
    );
    """, row )

def load_538(con, filename):
    with con:
        con.execute("""

        CREATE TABLE five_thirty_eight_quiet_caucus (
            icpsr integer,
			name text,
			last_name text,
			party text,
			district text,
			terms_pct real,
			margin_2020 text,
			cluster text,
			agreement_score real,
			progressive integer,
			new_dems integer,
			blue_dogs integer,
			problem_solvers integer,
			RMSP integer,
			governance integer,
			study integer,
			freedom integer,
			dw_nominate_dim1 real,
			dw_nominate_dim2 real,
			notes text
        )
        """)

        with open(filename) as fp:
            reader = csv.reader(fp)
            next(reader) # throw out header
            for row in reader:
                row[0] = int(row[0])
                row[5] = float(row[5].replace("%", "")) if row[5] != "ƒis" else None #Absolutely bizzare data for Jim Himes
                row[8] = float(row[8].replace("%", "")) if row[8] != "" else None
                for i in range(9,17):
                    row[i] = 0 if row[i] == "" else 1
                row[17] = float(row[17])
                row[18] = float(row[18])
                write_538(con, row)

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

    con = sqlite3.connect("data/clean/bridge.sqlite")

    fetch_if_needed(scrape_538_quiet_caucus)
    load_538(con, scrape_538_quiet_caucus.output)
    con.close()

    fetch_if_needed(scrape_cook_pvi)

    fetch_if_needed(scrape_govtrack_cosponsor_house)
    fetch_if_needed(scrape_govtrack_cosponsor_senate)

    fetch_if_needed(scrape_voteview)

    fetch_if_needed(scrape_lugar_house)
    fetch_if_needed(scrape_lugar_senate)

    # h/t https://docs.python.org/3/library/itertools.html#itertools-recipes
    collections.deque(map(scrape_commonground_state_index, states), 0)


