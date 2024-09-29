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
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_0_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_0_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36"
]

parser = argparse.ArgumentParser(description="Scrape Uber Eats data")
parser.add_argument("--country", "-c", type=str, nargs='+', help="Scrape data from specific countries. If not specified, all countries will be scraped.", metavar="")
parser.add_argument("--threads", "-t", type=int, default=5, help="Number of threads to use for scraping")
args = parser.parse_args()

# Global variables
current_file = None
cancel_requested = False

def get_random_user_agent() -> str:
    return random.choice(user_agents)

def clear():
    os.system('cls' if os.name == 'nt' else 'clear')

def cleanup_json_file():
    global current_file
    if current_file:
        try:
            with open(current_file, 'r+') as f:
                content = f.read().rstrip().rstrip(',')
                if content.endswith('{') or content.endswith('['):
                    content = content[:-1]
                elif not (content.endswith('}') or content.endswith(']')):
                    content += '}'
                f.seek(0)
                f.write(content)
                f.truncate()
            logger.info(f"Successfully cleaned up JSON file: {current_file}")
        except Exception as e:
            logger.error(f"Error cleaning up JSON file {current_file}: {e}")

def interrupt_handler(signum, frame):
    global cancel_requested
    cancel_requested = True
    print("\nInterrupt received. Cancelling the scraping process...")
    cleanup_json_file()
    sys.exit(0)

# Register the interrupt handler
signal.signal(signal.SIGINT, interrupt_handler)

def load_existing_data(file_path: str) -> Dict:
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)
    return {"country": "", "cities": []}

def save_data(file_path: str, data: Dict):
    global current_file
    current_file = file_path
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)

def get_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def scrape_city(city_url: str, city_name: str, headers: Dict) -> List[Dict]:
    global cancel_requested
    shops = []
    try:
        if cancel_requested:
            return shops

        time.sleep(random.uniform(0.5, 1.5))
        session = get_session()
        city_response = session.get(city_url, headers=headers, timeout=10)
        city_response.raise_for_status()
        city_soup = BeautifulSoup(city_response.content, "html.parser")
        shop_elements = city_soup.find_all('a', {"data-test": "store-link"})

        for shop in shop_elements:
            if cancel_requested:
                return shops
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
    global cancel_requested
    if cancel_requested:
        return

    if c == "uk":
        c = "gb"  # Use "gb" for API calls if "uk" is provided

    headers = {
        "User-Agent": get_random_user_agent()
    }

    time.sleep(random.uniform(0.5, 1.5))
    try:
        session = get_session()
        response = session.get(f"https://restcountries.com/v3.1/alpha/{c}?fields=name", headers=headers, timeout=10)
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
        time.sleep(random.uniform(0.5, 1.5))
        session = get_session()
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"An error occurred while scraping {country}: {e}")
        return

    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.find_all('a')
    cities_to_scrape = []

    for link in links:
        if cancel_requested:
            return
        href = link.get('href')
        name = link.get_text().strip()
        if href and href.startswith(f"/{c}/city"):
            city_url = f"https://www.ubereats.com{href}"
            # Check if the city has already been scraped
            existing_city = next((city for city in data["cities"] if city["city"] == name), None)
            if existing_city:
                logger.info(f"Skipping already scraped city: {name}")
                continue
            cities_to_scrape.append((city_url, name))

    try:
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            future_to_city = {executor.submit(scrape_city, city_url, name, headers): (city_url, name) for city_url, name in cities_to_scrape}
            for future in as_completed(future_to_city):
                if cancel_requested:
                    executor.shutdown(wait=False)
                    return
                city_url, name = future_to_city[future]
                try:
                    shops = future.result()
                    city_data = {
                        "city": name,
                        "shops": shops
                    }
                    data["cities"].append(city_data)
                    save_data(file_path, data)
                    logger.info(f"Data for {name} in {country} has been saved.")
                except Exception as exc:
                    logger.error(f"An error occurred while processing {name}: {exc}")
    except Exception as exc:
        logger.error(f"An error occurred while processing {country}: {exc}")
    finally:
        if cancel_requested:
            logger.info(f"Scraping for {country} was cancelled.")
        else:
            logger.info(f"All data for {country} has been saved to {file_path}")

if __name__ == "__main__":
    os.makedirs('countries', exist_ok=True)

    if args.country is None:
        clear()
        logger.info("Scraping all countries...")
        countries_to_scrape = countries
    else:
        countries_to_scrape = [c.lower() for c in args.country if c.lower() in countries]
        invalid_countries = [c for c in args.country if c.lower() not in countries]
        if invalid_countries:
            logger.warning(f"Invalid country code(s): {', '.join(invalid_countries)}")
            logger.info(f"Valid country codes are: {', '.join(sorted(set(countries)))}")
        if not countries_to_scrape:
            logger.error("No valid countries to scrape. Exiting.")
            sys.exit(1)

    clear()
    logger.info(f"Scraping the following countries: {', '.join(countries_to_scrape)}")

    try:
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = list(executor.map(scrape_country, countries_to_scrape))
            
            while not all(future.done() for future in futures):
                if cancel_requested:
                    break
                time.sleep(0.1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        cleanup_json_file()
        if cancel_requested:
            logger.info("Scraping process was cancelled by the user.")
        else:
            logger.info("Scraping completed for all specified countries.")

    sys.exit(0)