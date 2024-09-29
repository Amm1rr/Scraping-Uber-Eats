import requests
from bs4 import BeautifulSoup
import json
import os
import signal
import argparse
import sys
import random
import time
import logging
from typing import Dict, List

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

countries = ["uk", "au", "be", "ca", "cl", "cr", "do", "ec", "sv", "fr", "de", "gt", "ie", "jp", "ke", "mx", "nl", "nz", "pa", "pl", "pt", "za", "es", "lk", "se", "ch", "tw", "gb"]

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/91.0.4472.80 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59"
]

parser = argparse.ArgumentParser(description="Scrape Uber Eats data")
parser.add_argument("--country", "-c", type=str, nargs='+', help="Scrape data from specific countries. If not specified, all countries will be scraped.", metavar="<COUNTRYCODE>")
args = parser.parse_args()

def get_random_user_agent() -> str:
    return random.choice(user_agents)

def clear():
    os.system('cls' if os.name == 'nt' else 'clear')

def end(signal=None, frame=None):
    logger.info("Exiting...")
    sys.exit(0)

# Register the signal handler for Ctrl+C
signal.signal(signal.SIGINT, end)

def load_existing_data(file_path: str) -> Dict:
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)
    return {"country": "", "cities": []}

def save_data(file_path: str, data: Dict):
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)

def scrape_city(city_url: str, city_name: str, headers: Dict) -> List[Dict]:
    shops = []
    try:
        time.sleep(random.uniform(1, 3))
        city_response = requests.get(city_url, headers=headers, timeout=10)
        city_response.raise_for_status()
        city_soup = BeautifulSoup(city_response.content, "html.parser")
        shop_elements = city_soup.find_all('a', {"data-test": "store-link"})
        for shop in shop_elements:
            path = shop.get('href')
            page_link = "https://www.ubereats.com" + path
            names = shop.find_all('h3')
            for name in names:
                restaurant_name = name.get_text().strip()
                shop_data = {
                    "name": restaurant_name,
                    "link": page_link
                }
                shops.append(shop_data)
    except requests.exceptions.RequestException as e:
        logger.error(f"An error occurred while scraping {city_name}: {e}")
    return shops

def scrape_country(c: str):
    if c == "uk":
        c = "gb"  # Use "gb" for API calls if "uk" is provided
    
    headers = {
        "User-Agent": get_random_user_agent()
    }
    
    time.sleep(random.uniform(1, 3))
    
    try:
        response = requests.get(f"https://restcountries.com/v3.1/alpha/{c}?fields=name", headers=headers, timeout=10)
        response.raise_for_status()
        country_info = response.json()
        if isinstance(country_info, list) and len(country_info) > 0:
            country = country_info[0].get("name", {}).get("common", c.upper())
        else:
            country = country_info.get("name", {}).get("common", c.upper())
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching country info for {c}: {e}")
        country = c.upper()
    except (KeyError, IndexError, ValueError) as e:
        logger.error(f"Error parsing country info for {c}: {e}")
        country = c.upper()
    
    file_path = f"countries/{c}.json"
    data = load_existing_data(file_path)
    data["country"] = country

    logger.info(f"Scraping {country}...")

    url = f"https://www.ubereats.com/{c}/location"
    try:
        time.sleep(random.uniform(1, 3))
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status() 
    except requests.exceptions.RequestException as e:
        logger.error(f"An error occurred while scraping {country}: {e}")
        return

    soup = BeautifulSoup(response.content, "html.parser")

    links = soup.find_all('a')
    for link in links:
        href = link.get('href')
        name = link.get_text().strip()
        if href and href.startswith(f"/{c}/city"):
            city_url = f"https://www.ubereats.com{href}"
            
            # Check if the city has already been scraped
            existing_city = next((city for city in data["cities"] if city["city"] == name), None)
            if existing_city:
                logger.info(f"Skipping already scraped city: {name}")
                continue

            city_data = {
                "city": name,
                "shops": scrape_city(city_url, name, headers)
            }

            data["cities"].append(city_data)
            save_data(file_path, data)
            logger.info(f"Data for {name} in {country} has been saved.")

    logger.info(f"All data for {country} has been saved to {file_path}")

if __name__ == "__main__":
    os.makedirs('countries', exist_ok=True)
    
    if args.country is None:
        clear()
        logger.info("Scraping all countries...")
        for c in countries:
            scrape_country(c)
    else:
        for c in args.country:
            if c.lower() not in countries:
                logger.warning(f"Invalid country code: {c}")
                logger.info(f"Valid country codes are: {', '.join(sorted(set(countries)))}")
            else:
                clear()
                scrape_country(c.lower())

    end()