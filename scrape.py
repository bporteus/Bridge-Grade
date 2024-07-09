import requests
import bs4


import os

######################################################################

def fetch_if_needed(fetcher):
    if not os.path.isfile(fetcher.output):
        fetcher()

def make_fetcher(url, output):
    def fetcher():
        print("fetching: ", fetcher.url)
        r = requests.get(fetcher.url)
        with open(fetcher.output, "w") as out:
            out.write(r.text)

    fetcher.url = url
    fetcher.output = output

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



if __name__ == "__main__":
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/clean", exist_ok=True)

    fetch_if_needed(scrape_538_quiet_caucus)

    fetch_if_needed(scrape_cook_pvi)

    fetch_if_needed(scrape_govtrack_cosponsor_house)
    fetch_if_needed(scrape_govtrack_cosponsor_senate)

    fetch_if_needed(scrape_voteview)


