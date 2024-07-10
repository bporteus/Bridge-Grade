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

def write_cook_pvi(con, row):
        con.execute("""
        insert into cook_pvi (
            Dist, First, Last, Party, PVI_2023, PVI_2023_raw, Rank
        ) values (
             ?,  ?,  ?,  ?,  ?,  ?, ?
        );
        """, row)


def load_cook_pvi(con, filename):
    with con:
        con.execute("""
        CREATE TABLE cook_pvi (
            Dist text,
            First text,
            Last text,
            Party text,
            PVI_2023 text,
            PVI_2023_raw text,
            Rank integer
            );
        """)
        with open(filename) as fp:
            reader = csv.reader(fp, delimiter='\t')
            next(reader) # throw out header
            for row in reader:
                row[6] = int(row[6])
                write_cook_pvi(con, row)
######################################################################

scrape_govtrack_cosponsor_house = make_fetcher(
        url= "https://www.govtrack.us/congress/members/report-cards/2022/house/cosponsored-other-party.csv",
        output= "data/raw/govtrack_cosponsor_house.csv"
)

scrape_govtrack_cosponsor_senate = make_fetcher(
        url= "https://www.govtrack.us/congress/members/report-cards/2022/senate/cosponsored-other-party.csv",
        output= "data/raw/govtrack_cosponsor_senate.csv"
)

def write_govtrack_cosponsor(con, row, chamber):
        con.execute("""
        insert into govtrack_cosponsor (
            chamber, rank_from_low, rank_from_high, percentile, cosponsored_other_party, id, bioguide_id, state, district, name
        ) values (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        );
        """, (chamber, *row))

def load_govtrack_cosponsor(con, filename, chamber):
    with con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS govtrack_cosponsor(
            chamber text,
            rank_from_low int,
			rank_from_high int,
			percentile int,
			cosponsored_other_party real,
			id int,
			bioguide_id text,
			state text,
			district int,
			name text
            );
        """)
        with open(filename) as fp:
            reader = csv.reader(fp)
            next(reader) # throw out header
            for row in reader:
                row[0] = int(row[0])
                row[1] = int(row[1])
                row[2] = int(row[2])
                row[3] = float(row[3])
                row[4] = int(row[4])
                row[7] = int(row[7]) if row[7] != "" else None
                row[8] = eval(row[8]) # convert bytes escaped to regular text
                write_govtrack_cosponsor(con, row, chamber)
######################################################################

scrape_voteview = make_fetcher(
        url= "https://voteview.com/static/data/out/members/HSall_members.csv",
        output= "data/raw/vote_view.csv"
)

def write_voteview(con, row):
        con.execute("""
        insert into voteview (
            congress, chamber, icpsr, state_icpsr, district_code, state_abbrev, party_code, occupancy, last_means, bioname , bioguide_id, born, died, nominate_dim1, nominate_dim2, nominate_log_likelihood, nominate_geo_mean_probability, nominate_number_of_votes, nominate_number_of_errors, conditional, nokken_poole_dim1, nokken_poole_dim2
        ) values (
           ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        );
        """, row)


def load_voteview(con, filename):
    with con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS voteview(
            congress int,
			chamber text,
			icpsr int,
			state_icpsr int,
			district_code int,
			state_abbrev text,
			party_code int,
			occupancy int,
			last_means int,
			bioname  text,
			bioguide_id text,
			born int,
			died int,
			nominate_dim1 real,
			nominate_dim2 real,
			nominate_log_likelihood real,
			nominate_geo_mean_probability real,
			nominate_number_of_votes int,
			nominate_number_of_errors int,
			conditional text,
			nokken_poole_dim1 real,
            nokken_poole_dim2 real
            );
        """)
        with open(filename) as fp:
            reader = csv.reader(fp)
            next(reader) # throw out header
            for row in reader:
                row[0] = int(row[0])

                if row[0] < 118: continue # only want most recent congress

                row[2] = int(row[2])
                row[3] = int(row[3])
                row[4] = int(row[4])
                row[6] = int(row[6]) # party code
                row[7] = int(row[7]) if row[7] != '' else None # occupancy
                row[8] = int(row[8]) if row[8] != '' else None # last_means
                row[11] = int(row[11][:4]) if row[11] != '' else None # born
                row[12] = int(row[12][:4]) if row[12] != '' else None # died
                for i in range(13, 17):
                    row[i] = float(row[i]) if row[i] else None
                row[17] = int(row[17]) if row[17] else None # num_votes
                row[18] = int(row[18]) if row[18] else None # of_errors
                row[20] = float(row[20])
                row[21] = float(row[21])
                write_voteview(con,row)
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

@with_attrs(base="data/raw/commonground")
def scrape_commonground_state_index(state_tup):
    state, abbr = state_tup
    url = f"https://commongroundscorecard.org/tag/{abbr}/"
    out_dir = f"{scrape_commonground_state_index.base}/{abbr}/"
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

    # 538
    fetch_if_needed(scrape_538_quiet_caucus)
    load_538(con, scrape_538_quiet_caucus.output)

    # Cook PVI
    fetch_if_needed(scrape_cook_pvi)
    load_cook_pvi(con, scrape_cook_pvi.output)

    # GovTrack
    fetch_if_needed(scrape_govtrack_cosponsor_house)
    fetch_if_needed(scrape_govtrack_cosponsor_senate)
    load_govtrack_cosponsor(con, scrape_govtrack_cosponsor_house.output, "house")
    load_govtrack_cosponsor(con, scrape_govtrack_cosponsor_senate.output, "senate")

    # VoteView
    fetch_if_needed(scrape_voteview)
    load_voteview(con, scrape_voteview.output)
    con.close()

    fetch_if_needed(scrape_lugar_house)
    fetch_if_needed(scrape_lugar_senate)

    # h/t https://docs.python.org/3/library/itertools.html#itertools-recipes
    collections.deque(map(scrape_commonground_state_index, states), 0)


