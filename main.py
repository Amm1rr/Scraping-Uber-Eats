import argparse
import json
import logging
import os
import random
import requests
import signal
import sys
import time
import datetime
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from typing import Dict, List
import logging.config


# Logging configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        },
        'simple': {
            'format': '%(levelname)s - %(message)s',
        },
    },
    'handlers': {
        'file_handler': {
            'class': 'logging.FileHandler',
            'filename': 'scraping.log',
            'formatter': 'detailed',
            'level': logging.INFO,
        },
        'console_handler': {
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
            'level': logging.INFO,
        },
    },
    'loggers': {
        '': {  # root logger
            'handlers': ['file_handler'],
            'level': logging.INFO,
            'propagate': False,
        },
        'console': {
            'handlers': ['console_handler'],
            'level': logging.INFO,
            'propagate': False,
        },
    },
}

def setup_logging(args):
    if args.debug:
        LOGGING_CONFIG['handlers']['file_handler']['level'] = logging.DEBUG
        LOGGING_CONFIG['loggers']['']['level'] = logging.DEBUG

    logging.config.dictConfig(LOGGING_CONFIG)


# Constants
COUNTRIES = [
    "uk", "au", "be", "ca", "cl", "cr", "do", "ec", "sv", "fr",
    "de", "gt", "ie", "jp", "ke", "mx", "nl", "nz", "pa", "pl",
    "pt", "za", "es", "lk", "se", "ch", "tw", "gb"
]

USER_AGENTS = [
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

# Command line argument parsing function
def parse_arguments():
    parser = argparse.ArgumentParser(description="Scrape Uber Eats data")
    parser.add_argument("--country", "-c", type=str, nargs='+', help="Scrape data from specific countries. If not specified, all countries will be scraped.", metavar="")
    parser.add_argument("--threads", "-t", type=int, default=5, help="Number of threads to use for scraping")
    parser.add_argument("--resume", "-r", action="store_true", help="Resume scraping from failed links")
    parser.add_argument("--verbose"  , "-v", action="store_true", help="Enable verbose output")
    parser.add_argument("--debug", "-d", action="store_true", help="Enable detailed logging and error tracing for debugging purposes.")
    return parser.parse_args()

# Global variables
# global args
# current_file = None
# cancel_requested = False
# start_time = None
# data = {"country": "", "cities": []}

# Global configuration
class GlobalConfig:
    current_file: str = None
    cancel_requested: bool = False
    start_time: float = None
    data: Dict[str, any] = {"country": "", "cities": []}


def exit_program(error_number = 0):
    # End time logging
    if error_number == 0:
        if GlobalConfig.start_time:
            end_time = time.time()
            elapsed_time = end_time - GlobalConfig.start_time
            logging.info(f"{'Total elapsed time: ':<30} {elapsed_time:.2f} seconds.")
            logging.info(f"{'Scraping ended at:':<30} {time.ctime(end_time)}")
        
        exit(error_number)
    else:
        print("Script ended unsuccessfully.")
        exit(error_number)
        

def get_random_user_agent() -> str:
    """Returns a random user agent from the list."""
    return random.choice(USER_AGENTS)

def clear_console():
    """Clears the console screen."""
    os.system('cls' if os.name == 'nt' else 'clear')

def cleanup_json_file():
    """Cleans up the current JSON file by removing trailing commas."""
    if GlobalConfig.current_file:
        try:
            with open(GlobalConfig.current_file, 'r+') as f:
                content = f.read().rstrip().rstrip(',')
                if content.endswith('{') or content.endswith('[') or content.endswith(','):
                    content = content[:-1]
                elif not (content.endswith('}') or content.endswith(']')):
                    content += '}'
                f.seek(0)
                f.write(content)
                f.truncate()
            
            logging.info(f"Successfully cleaned up JSON file: {GlobalConfig.current_file}")
            logging.info(f"----- Summary : ")
            
        except Exception as e:
            logging.error(f"Error cleaning up JSON file {GlobalConfig.current_file}: {e}")

def get_country_code(code: str) -> str:
    """Normalize country code, converting 'uk' to 'gb' if necessary."""
    return 'gb' if code.lower() == 'uk' else code.lower()

def log_summary():
    """Logs the summary of the scraping process."""
    if GlobalConfig.start_time is not None:
        end_time = time.time()
        elapsed_time = end_time - GlobalConfig.start_time
        
        # Format start and end times
        formatted_start_time = time.strftime("%H:%M:%S", time.localtime(GlobalConfig.start_time))
        formatted_end_time = time.strftime("%H:%M:%S", time.localtime(end_time))

        # Convert elapsed time to hours, minutes, seconds
        hours, remainder = divmod(int(elapsed_time), 3600)
        minutes, seconds = divmod(remainder, 60)
        formatted_elapsed_time = f"{hours:02}:{minutes:02}:{seconds:02}"

        # Aligning output
        logging.info(f"{'Estimating, Start Time:':<30} {formatted_start_time}")
        logging.info(f"{'Estimating, End Time:':<30} {formatted_end_time}")
        logging.info(f"{'Estimating, Duration:':<30} {formatted_elapsed_time}")

def interrupt_handler(signum, frame):
    """Handles keyboard interrupts (Ctrl+C) to gracefully stop the scraper."""
    GlobalConfig.cancel_requested = True
    logging.info("\n\nInterrupt received. Cancelling the scraping process...\n")

    cleanup_json_file()  # Cleanup any JSON files if necessary

    # Log the summary before exiting
    log_summary()  # Call the new summary logging function

    exit_program(0)  # Exit the program gracefully

# Register the interrupt handler
signal.signal(signal.SIGINT, interrupt_handler)

def log_failed_link(country_code: str, city_name: str, link: str = None):
    normalized_country_code = get_country_code(country_code)
    failed_links_file = f"failed_links_{normalized_country_code}.json"
    
    failed_data = {}
    if os.path.exists(failed_links_file):
        with open(failed_links_file, 'r') as f:
            failed_data = json.load(f)
    
    if normalized_country_code not in failed_data:
        failed_data[normalized_country_code] = {}
    
    if city_name not in failed_data[normalized_country_code]:
        failed_data[normalized_country_code][city_name] = []
    
    if link is not None:
        failed_data[normalized_country_code][city_name].append(link)

    # Now, also update the main data file to include the city with an empty shop list
    normalized_city_name = city_name.strip().lower()
    
    existing_city = next((city for city in GlobalConfig.data["cities"] if city["city"].strip().lower() == normalized_city_name), None)
    
    if not existing_city:
        # Add the city with an empty shops list in the data structure
        GlobalConfig.data["cities"].append({"city": city_name.strip(), "shops": []})
        save_data(f"countries/{normalized_country_code}.json", GlobalConfig.data)
    
    with open(failed_links_file, 'w') as f:
        json.dump(failed_data, f, indent=4)

def load_existing_data(file_path: str) -> Dict:
    """
    Loads existing data from the JSON file.
    If the file doesn't exist or contains invalid JSON, returns a default dictionary.
    """
    default_data = {"country": "", "cities": []}
    
    if not os.path.exists(file_path):
        logging.warning(f"File not found: {file_path}. Returning default data.")
        return default_data, None

    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
        logging.info(f"Successfully loaded data from {file_path}")
        return data, None

    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON in {file_path}: {str(e)}"
        logging.error(f"{error_msg}. Returning default data.")
        return None, error_msg

    except IOError as e:
        error_msg = f"IO error while reading {file_path}: {str(e)}"
        logging.error(f"{error_msg}. Returning default data.")
        return None, error_msg

    except Exception as e:
        error_msg = f"Unexpected error while loading {file_path}: {str(e)}"
        logging.error(f"{error_msg}. Returning default data.")
        return None, error_msg

def save_data(file_path: str, data: Dict):
    """Saves data to the specified JSON file."""
    try:
        GlobalConfig.current_file = file_path
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        return True
    except Exception as e:
        logging.error(f"Error saving data to {file_path}: {e}")
        return False

def resume_failed_scraping(country_code: str):
    if country_code == "uk":
        country_code = "gb"  # Use "gb" for UK
    failed_links_file = f"failed_links_{country_code}.json"
    
    if not os.path.exists(failed_links_file):
        logging.info(f"No failed links found for {country_code}")
        return
    
    with open(failed_links_file, 'r') as f:
        failed_data = json.load(f)

    headers = {"User-Agent": get_random_user_agent()}
    
    # Create a list of cities to avoid modifying the dict during iteration
    cities_to_retry = list(failed_data[country_code].items())
    
    for city_name, links in cities_to_retry:
        # Create a copy of the links to iterate over
        for link in links[:]:  # Using slicing to create a copy
            if GlobalConfig.cancel_requested:
                return
            
            logging.info(f"Resuming : {city_name:<30}: {link}")
            shops = scrape_city(link, city_name, headers, country_code)
            
            if shops:
                # Update the main GlobalConfig.data structure here, replacing empty shops list
                for city in GlobalConfig.data["cities"]:
                    if city["city"].strip().lower() == city_name.strip().lower():
                        city["shops"] = shops
                        break
                
                # Save the updated main GlobalConfig.data file
                save_data(f"countries/{country_code}.json", GlobalConfig.data)
                
                # Remove successful link from failed_data
                failed_data[country_code][city_name].remove(link)
                
                # Clean up if there are no more failed links for this city
                if not failed_data[country_code][city_name]:
                    del failed_data[country_code][city_name]
                
                # Update the failed links file
                with open(failed_links_file, 'w') as f:
                    json.dump(failed_data, f, indent=4)

                # Remove the file if no failed links remain
                if not failed_data[country_code]:
                    os.remove(failed_links_file)

def update_main_data(city_name: str, shops: List[Dict], country_code: str):
    """Updates the main GlobalConfig.data structure with new shop information."""
    
    try:
        normalized_city_name = city_name.strip().lower()
        
        existing_city = next((city for city in GlobalConfig.data["cities"] if city["city"].strip().lower() == normalized_city_name), None)
        
        if existing_city:
            # Update existing city with new shops
            existing_shops = {shop["link"]: shop for shop in existing_city["shops"]}
            for shop in shops:
                if shop["link"] not in existing_shops:
                    existing_city["shops"].append(shop)
            logging.debug(f"Updated existing city: {city_name} with {len(shops)} new shops.")
        else:
            # Create a new entry for the city
            new_city_data = {"city": city_name.strip(), "shops": shops}
            GlobalConfig.data["cities"].append(new_city_data)
            logging.debug(f"Added new city: {city_name} with {len(shops)} shops.")
        
        # Save the updated GlobalConfig.data
        save_data(f"countries/{country_code}.json", GlobalConfig.data)
        return True
    except Exception as e:
        logging.error(f"Error updating main GlobalConfig.data: {str(e)}")
        return False

def get_session():
    """Creates a session with retry logic."""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def scrape_city(city_url: str, city_name: str, headers: Dict, country_code: str) -> List[Dict]:
    shops = []

    try:
        if GlobalConfig.cancel_requested:
            return shops

        time.sleep(random.uniform(0.5, 1.5))  # Random sleep to avoid getting blocked
        session = get_session()

        city_response = session.get(city_url, headers=headers, timeout=10)
        city_response.raise_for_status()
        city_soup = BeautifulSoup(city_response.content, "html.parser")
        shop_elements = city_soup.find_all('a', {"data-test": "store-link"})

        for shop in shop_elements:
            if GlobalConfig.cancel_requested:
                return shops
            try:
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
            except Exception as e:
                logging.error(f"Error scraping shop in {city_name}: {e}")
                log_failed_link(country_code, city_name, shop.get('href'))

    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred while scraping {city_name}: {e}")
        log_failed_link(country_code, city_name, city_url)
        return shops

    return shops

def print_to_console(content:str):
    if content:
        if args.verbose:
            pass
        else:
            print(content)

def scrape_country(country_code: str):
    """Scrapes Uber Eats data for all cities in a given country."""
    if GlobalConfig.cancel_requested:
        return

    normalized_country_code = get_country_code(country_code)

    headers = {"User-Agent": get_random_user_agent()}
    time.sleep(random.uniform(0.5, 1.5))  # Random sleep

    # Fetch country information
    try:
        session = get_session()
        response = session.get(f"https://restcountries.com/v3.1/alpha/{normalized_country_code}?fields=name", headers=headers, timeout=10)
        response.raise_for_status()
        country_info = response.json()
        country = country_info[0]["name"]["common"] if isinstance(country_info, list) else normalized_country_code.upper()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching country info for {normalized_country_code}: {e}")
        country = normalized_country_code.upper()

    # Load existing data
    file_path = f"countries/{normalized_country_code}.json"
    data, error_info = load_existing_data(file_path)
    if data:
        GlobalConfig.data = data
    else:
        error_message = f"Error: Country file is empty or invalid: {file_path}"
        print_to_console(error_message)
        return error_message
    
    GlobalConfig.data["country"] = country
    logging.info(f"Scraping {country}...")
    
    # Scrape cities from the country
    url = f"https://www.ubereats.com/{normalized_country_code}/location"
    try:
        time.sleep(random.uniform(0.5, 1.5))  # Random sleep
        session = get_session()
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred while scraping {country}: {e}")
        log_failed_link(normalized_country_code, country, url)  # Log the failed country URL
        return

    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.find_all('a')
    cities_to_scrape = []
    
    for link in links:
        if GlobalConfig.cancel_requested:
            return
        href = link.get('href')
        name = link.get_text().strip()
        if href and href.startswith(f"/{normalized_country_code}/city"):
            city_url = f"https://www.ubereats.com{href}"
            # Check if the city has already been scraped
            existing_city = next((city for city in GlobalConfig.data["cities"] if city["city"] == name), None)
            if existing_city:
                logging.info(f"Exist : {name:<30} Skipping")
                continue
            cities_to_scrape.append((city_url, name))

    if not cities_to_scrape:
        logging.warning(f"No cities found for {country}. This might indicate an error in fetching the links.")
        log_failed_link(normalized_country_code, country, url)  # Log the country URL if no cities are found

    # Scrape each city in parallel using threads
    try:
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            future_to_city = {executor.submit(scrape_city, city_url, name, headers, normalized_country_code): (city_url, name) for city_url, name in cities_to_scrape}
            for future in as_completed(future_to_city):
                if GlobalConfig.cancel_requested:
                    executor.shutdown(wait=False)
                    return
                city_url, name = future_to_city[future]
                try:
                    shops = future.result()
                    city_data = {
                        "city": name,
                        "shops": shops
                    }
                    GlobalConfig.data["cities"].append(city_data)
                    
                    save_data(file_path, GlobalConfig.data)
                    logging.info(f" Saved : {name:<30} {'in':<5} {country}")
                except Exception as exc:
                    logging.error(f"An error occurred while processing {name}: {exc}")
                    log_failed_link(normalized_country_code, name, city_url)  # Log the failed city URL
    except Exception as exc:
        logging.error(f"An error occurred while processing {country}: {exc}")
        log_failed_link(normalized_country_code, country, url)  # Log the country URL if there's an overall error
    finally:
        if GlobalConfig.cancel_requested:
            logging.info(f"{'Scraping for ' + country:<30} was cancelled.")
        else:
            logging.info(f"All data for {country} has been saved to {file_path}")

def Console_log(args):
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
    console_handler.addFilter(VerboseFilter(args.verbose))
    logging.getLogger('').addHandler(console_handler)

def print_initial_info(args):
    start_time = datetime.datetime.now()
    initial_info = f"Script started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n" \
                   f"Arguments received: {vars(args)}"
    
    # Log to file
    logging.info(initial_info)
    if not args.verbose:
        print(initial_info)


class VerboseFilter(logging.Filter):
    def __init__(self, verbose):
        super().__init__()
        self.verbose = verbose

    def filter(self, record):
        return self.verbose

def Input_Country():
    def get_key():
        if sys.platform.startswith('win'):
            import msvcrt
            return msvcrt.getch().decode('utf-8').lower()
        else:
            import termios
            import tty
            fd = sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)
            try:
                tty.setraw(sys.stdin.fileno())
                ch = sys.stdin.read(1)
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
            return ch.lower()
    
    while True:
        user_input = input("Please enter at least one country code or a list of countries separated by space (e.g., uk de): ").strip().lower()
        if user_input:
            country_codes = user_input.split()
            if country_codes:
                break
            else:
                print("Invalid input. Please enter at least one country code.")
        else:
            print("No input provided. Please try again.")
    
    prompt = f"""
    Are you sure you want to continue with these country codes?
    {', '.join(country_codes)}

    Press Y to continue or N to exit: """

    print(prompt, end='', flush=True)

    while True:
        key = get_key()
        if key == ('y' or ''):
            print("\nContinuing...")
            return country_codes
        else:
            print("\nExiting...")
            exit_program(0)

if __name__ == "__main__":
    args = parse_arguments()
    setup_logging(args)
    Console_log(args)
    print_initial_info(args)
    
    # Handle countries from arguments
    # countries_to_scrape = args.country if args.country else COUNTRIES
    country_codes = None
    if not args.country:
        country_codes = Input_Country()
    
    countries_to_scrape = None
    if country_codes:
        countries_to_scrape = country_codes
    else:
        countries_to_scrape = args.country if args.country else COUNTRIES
    
    invalid_countries = [code for code in countries_to_scrape if get_country_code(code) not in COUNTRIES]
    
    if invalid_countries:
        logging.warning(f"Invalid country codes provided: {', '.join(invalid_countries)}")
    
    # Start time logging
    GlobalConfig.start_time = time.time()
    logging.info(f"{'Scraping started at:':<30} {time.ctime(GlobalConfig.start_time)}")

    if args.resume:
        for country_code in countries_to_scrape:
            if GlobalConfig.cancel_requested:
                break
            
            normalized_country_code = get_country_code(country_code)
            # Load existing data before resuming
            file_path = f"countries/{normalized_country_code}.json"
            
            data, error_info = load_existing_data(file_path)
            EMPTY_DATA_STRUCTURE = {"country": "", "cities": []}

            if data:
                if data != EMPTY_DATA_STRUCTURE:
                    GlobalConfig.data = data
                    resume_failed_scraping(normalized_country_code)
                else:
                    if input("File is empty. Add failed links? (y/N): ").strip().lower().startswith('y'):
                        print("Adding new links...")
            else:
                error_message = f"Error in loading file: {file_path}"
                print_to_console(error_message)
            
    else:
        for country_code in countries_to_scrape:
            if GlobalConfig.cancel_requested:
                break
            scrape_country(country_code)

    exit_program(0)

