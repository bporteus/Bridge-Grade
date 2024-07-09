import requests
import bs4


import os

######################################################################

def fetch_if_needed(fetcher):
    if not os.path.isfile(fetcher.output):
        fetcher()

######################################################################

def scrape_538_quiet_caucus():
    r = requests.get(scrape_538_quiet_caucus.url)
    with open(scrape_538_quiet_caucus.output, "w") as out:
        out.write(r.text)

scrape_538_quiet_caucus.output = "data/raw/clustered_congress.csv"
scrape_538_quiet_caucus.url = "https://github.com/fivethirtyeight/data/raw/master/quiet-caucus-2024/clustered_congress.csv"

######################################################################



if __name__ == "__main__":
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/clean", exist_ok=True)

    fetch_if_needed(scrape_538_quiet_caucus)


