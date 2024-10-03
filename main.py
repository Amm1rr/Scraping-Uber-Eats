import datetime
import json
import logging
import os
import random
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from config import COUNTRIES
import config
import utils

class GlobalConfig:
    current_file:       str   = None
    cancel_requested:   bool  = False
    start_time:         float = None
    IsConnectionsAvailable    = True
    retry_time_on_connection_fail: int = 3
    data: Dict[str, any] = {"country": "", "cities": []}

def exit_program(error_number = 0, show_message = True):
    # End time logging
    if error_number == 0:
        if GlobalConfig.start_time:
            end_time = time.time()
            elapsed_time = end_time - GlobalConfig.start_time
            
            msg = [
            f"{'Total elapsed time: ':<30} {elapsed_time:.2f} seconds",
            f"{'Scraping ended at:':<30} {time.ctime(end_time)}"
            ]

            for message in msg:
                logging.info(message)
                if show_message:
                    print_to_console(message)
        
        exit(error_number)
    else:
        print("Script ended unsuccessfully.{:<30}")
        exit(error_number)
        

def get_random_user_agent() -> str:
    """Returns a random user agent from the list."""
    return random.choice(config.USER_AGENTS)

def cleanup_json_file():
    """Cleans up the current JSON file by removing trailing commas."""
    try:
        if isinstance(GlobalConfig.current_file, list):
            file_path = f"countries/{GlobalConfig.current_file[0]}.json"
            GlobalConfig.current_file = (file_path, GlobalConfig.data)
        else:
            file_path = GlobalConfig.current_file
        
        file_path = str(file_path)
        
        if os.path.exists(file_path):
            with open(file_path, 'r+') as f:
                content = f.read().rstrip().rstrip(',')
                if content.endswith('{') or content.endswith('['):
                    content = content[:-1]
                elif not (content.endswith('}') or content.endswith(']')):
                    content += '}'
                f.seek(0)
                f.write(content)
                f.truncate()
            
            message = [
            f"Successfully cleaned up JSON file : {GlobalConfig.current_file}"
            ]
            
            for msg in message:
                logging.info(msg)
            
        else:
            logging.error(f"Unsuccessfull cleaned up JSON. File not found : {file_path}")
            print_to_console(f"Unsuccessfull cleaned up JSON. File not found : {file_path}")
    
    except Exception as e:
        logging.error(f"Error cleaning up JSON file {GlobalConfig.current_file}: {e}")
        print_to_console(f"Error cleaning up JSON file {GlobalConfig.current_file}: {e}")

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
        
        messages = [
            f"\n\t----- Summary : ",
            f"{'Estimating, Start Time:':<30} {formatted_start_time}",
            f"{'Estimating, End Time:':<30} {formatted_end_time}",
            f"{'Estimating, Elapsed Time:':<30} {formatted_elapsed_time}",
        ]
        
        messages += [f"{'Scraping ' :<30} was cancelled."] if GlobalConfig.cancel_requested else []
        
        for msg in messages:
            logging.info(msg)
            print_to_console(msg)

def interrupt_handler(signum, frame):
    """Handles keyboard interrupts (Ctrl+C) to gracefully stop the scraper."""
    GlobalConfig.cancel_requested = True
    logging.info("\n\nInterrupt received. Cancelling the scraping process...\n")

    cleanup_json_file()  # Cleanup any JSON files if necessary

    # Log the summary before exiting
    log_summary()  # Call the new summary logging function

    exit_program(0, False)  # Exit the program gracefully

# Register the interrupt handler
signal.signal(signal.SIGINT, interrupt_handler)

def log_failed_link(country_code: str, city_name: str, link: str = None):
    try:
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
            logging.info(f" Kept  : {city_name:<30} {'for':<5} later")
            GlobalConfig.data["cities"].append({"city": city_name.strip(), "link": link.strip(), "shops": []})
            save_data(f"countries/{normalized_country_code}.json", GlobalConfig.data, False)
        
        with open(failed_links_file, 'w') as f:
            json.dump(failed_data, f, indent=4)
        
        return True
    except Exception as e:
        print("PRINT log_failed_link : log failed\n\n", e)
        return False

def load_existing_data(file_path: str) -> Dict:
    """
    Loads existing data from the JSON file.
    If the file doesn't exist or contains invalid JSON, returns a default dictionary.
    """
    default_data = {"country": "", "cities": []}
    
    if not os.path.exists(file_path):
        logging.warning(f"File not found: {file_path}. Returning default data.")
        return default_data

    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
        logging.info(f"Successfully loaded data from {file_path}")
        return data

    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON in {file_path}: {str(e)}"
        logging.error(f"{error_msg}. Returning default data.")
        return None

    except IOError as e:
        error_msg = f"IO error while reading {file_path}: {str(e)}"
        logging.error(f"{error_msg}. Returning default data.")
        return None

    except Exception as e:
        error_msg = f"Unexpected error while loading {file_path}: {str(e)}"
        logging.error(f"{error_msg}. Returning default data.")
        return None

def save_data(file_path: str, data: Dict, show_log: bool = True) -> bool:
    """Saves data to the specified JSON file."""
    try:
        GlobalConfig.current_file = file_path  # Update the current file path in GlobalConfig
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        if show_log : logging.info(f"Data successfully saved to {file_path}.")
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
                save_data(f"countries/{country_code}.json", GlobalConfig.data, False)
                
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

def get_session():
    """Creates a session with retry logic."""
    session = requests.Session()
    retry = Retry(total=GlobalConfig.retry_time_on_connection_fail, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def scrape_city(city_url: str, city_name: str, headers: Dict, country_code: str) -> List[Dict]:
    shops = []
    IsConnection = True

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

    except requests.exceptions.ConnectionError as e:
        logging.warning(f"Connection error while scraping city {city_name}")
        IsConnection = False
        log_failed_link(country_code, city_name, city_url)
    except requests.exceptions.Timeout as e:
        logging.warning(f"Timeout error while scraping city {city_name}")
        IsConnection = False
        log_failed_link(country_code, city_name, city_url)
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error while scraping city {city_name}")
        IsConnection = False
        log_failed_link(country_code, city_name, city_url)
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred while scraping city {city_name}: {e}")
        log_failed_link(country_code, city_name, city_url)
    finally:
        if IsConnection:
            return shops
        else:
            GlobalConfig.cancel_requested = True

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
    country = None
    try:
        session = get_session()
        response = session.get(f"https://restcountries.com/v3.1/alpha/{normalized_country_code}?fields=name", headers=headers, timeout=10)
        response.raise_for_status()
        country_info = response.json()
        country = country_info[0]["name"]["common"] if isinstance(country_info, list) else normalized_country_code.upper()
    
    except requests.exceptions.ConnectionError as e:
        logging.warning(f"Scrape_Country : Connection error while scraping country")
        GlobalConfig.IsConnectionsAvailable = False
    except requests.exceptions.Timeout as e:
        logging.warning(f"Scrape_Country : Timeout error while scraping country")
        GlobalConfig.IsConnectionsAvailable = False
    except requests.exceptions.HTTPError as e:
        logging.error(f"Scrape_Country : HTTP error while scraping country")
        GlobalConfig.IsConnectionsAvailable = False
    except requests.exceptions.RequestException as e:
        logging.error(f"Scrape_Country : Error fetching country info for {normalized_country_code}: {e}")
    finally:
        if not GlobalConfig.IsConnectionsAvailable: return "Connection Error"
        country = normalized_country_code.upper()

    # Load existing data
    file_path = f"countries/{normalized_country_code}.json"
    data = load_existing_data(file_path)
    if data:
        GlobalConfig.data = data
    else:
        error_message = f"Error: Country file is empty or invalid: {file_path}"
        print_to_console(error_message)
        return error_message
    
    GlobalConfig.data["country"] = country
    logging.info(f"Scraping {country}...")
    print_to_console(f"Scraping {country}...")
    
    # Scrape cities from the country
    url_country = f"https://www.ubereats.com/{normalized_country_code}/location"
    try:
        time.sleep(random.uniform(0.5, 1.5))  # Random sleep
        GlobalConfig.IsConnectionsAvailable = True
        session = get_session()
        response = session.get(url_country, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.ConnectionError as e:
        logging.warning(f"Scrape_Country:get_session : Connection error while scraping {country}")
        GlobalConfig.IsConnectionsAvailable = False
        # log_failed_link(normalized_country_code, country, url_country)
        return
    except requests.exceptions.Timeout as e:
        logging.warning(f"Scrape_Country:get_session : Timeout error while scraping {country}")
        GlobalConfig.IsConnectionsAvailable = False
        # log_failed_link(normalized_country_code, country, url_country)
        return
    except requests.exceptions.HTTPError as e:
        logging.error(f"Scrape_Country:get_session : HTTP error while scraping {country}")
        GlobalConfig.IsConnectionsAvailable = False
        # log_failed_link(normalized_country_code, country, url_country)
        return
    except requests.exceptions.RequestException as e:
        logging.error(f"Scrape_Country:get_session : An error occurred while scraping {country}: {e}")
        # log_failed_link(normalized_country_code, country, url_country)
        return

    soup_all_Cities = BeautifulSoup(response.content, "html.parser")
    Cities_Country = soup_all_Cities.find_all('a')
    shop_count = 0
    is_city_exist = False
    is_update = False
    
    # Initialize the main data structure
    data_to_save = {
        "country": normalized_country_code,
        "cities": []
    }
    
    for city in Cities_Country:
        if GlobalConfig.cancel_requested:
            return
        href = city.get('href')
        name = city.get_text().strip()
        if href and href.startswith(f"/{normalized_country_code}/city"):
            shop_count += 1
            city_url = f"https://www.ubereats.com{href}"
            is_city_exist = False
            # Check if the city has already been scraped or link is empty for update link
            for city_detail in GlobalConfig.data["cities"]:
                if city_detail.get("city") == name:
                    is_city_exist = True
                    if not city_detail.get("link"):
                        is_update = True
                        city_detail["link"] = "TEST"
                        logging.info(f"Updated link : {city_detail['city']:<30}: {city_url}")
                    else:
                        # logging.info(f"Exist : {name:<30} Skipping")
                        pass
                    
                    continue
            
            if is_city_exist: continue
            
            # Create a new city entry with an empty shops list
            new_city_entry = {
                "city": name,
                "link": city_url,
                "shops": []
            }
            
            logging.info(f"Extracted Shop : {shop_count} {name:<30}")
            data_to_save["cities"].append(new_city_entry)
            GlobalConfig.data["cities"].append(new_city_entry)
    
    if GlobalConfig.data["cities"] and shop_count < 1:
        logging.warning(f"No cities found for {normalized_country_code}. This might indicate an error in fetching the links.")
        print_to_console(f"No cities found for {normalized_country_code}. This might indicate an error in fetching the links.")
        return
    
    if shop_count > 0 or is_update:
        if not save_data(file_path, GlobalConfig.data, True):
            logging.error("Failed to save data after adding a new city.")
            print_to_console("Failed to save data after adding a new city.")
    
    if GlobalConfig.cancel_requested: return
    
    # Country_Page = load_existing_data(file_path)
    # if Country_Page:
    #     GlobalConfig.data = Country_Page
    # else:
    #     error_message = f"Error : Country file is empty or invalid  (2): {file_path}"
    #     logging.WARN(error_message)
    #     print_to_console(error_message)
    #     return error_message

    # Scrape each city in parallel using threads
    try:
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            
            future_to_city = {
                executor.submit(scrape_city, city["link"], city["city"], headers, normalized_country_code): (city["link"], city["city"])
                for city in GlobalConfig.data['cities']
            }
            
            for future in as_completed(future_to_city):
                if GlobalConfig.cancel_requested:
                    print_to_console("\n\nCANCEL 1\n\n")
                    log_failed_link(normalized_country_code, country, url_country)
                    executor.shutdown(wait=False)
                    return
                city_url, name = future_to_city[future]
                try:
                    shops = future.result()
                    if not shops:
                        log_failed_link(normalized_country_code, name, city_url)
                        return
                    
                    city_shop_data = {
                        "city": name,
                        "link": city_url,
                        "shops": shops
                    }
                    
                    city_found = False
                    for city_detail in GlobalConfig.data["cities"]:
                        if city_detail.get("city") == name:
                            city_found = True
                            city_detail["shops"] = shops
                            continue
                    if not city_found:
                        print("ERROR:CITY_NOT_FOUND")
                        GlobalConfig.data["cities"].append(city_shop_data)
                    
                    save_data(file_path, GlobalConfig.data, False)
                    logging.info(f" Saved : {name:<30} {'in':<5} {country}")
                except Exception as exc:
                    logging.error(f"An error occurred while processing country INSIDE {name}: {exc}")
                    log_failed_link(normalized_country_code, name, city_url)  # Log the failed city URL
    
    
    except requests.exceptions.ConnectionError as e:
        logging.warning(f"Scrape_Country:Get City : Connection error while scraping country {country}")
        GlobalConfig.IsConnectionsAvailable = False
        log_failed_link(normalized_country_code, country, url_country)
    except requests.exceptions.Timeout as e:
        logging.warning(f"Scrape_Country:Get City : Timeout error while scraping country {country}")
        GlobalConfig.IsConnectionsAvailable = False
        log_failed_link(normalized_country_code, country, url_country)
    except requests.exceptions.HTTPError as e:
        logging.error(f"Scrape_Country:Get City : HTTP error while scraping country {country}")
        GlobalConfig.IsConnectionsAvailable = False
        log_failed_link(normalized_country_code, country, url_country)
    except Exception as exc:
        logging.error(f"Scrape_Country:Get City : An error occurred while processing country {country}: {exc}")
        log_failed_link(normalized_country_code, country, url_country)
    finally:
        if not GlobalConfig.cancel_requested:
            logging.info(f"All data for {country} has been saved to {file_path}")

def config_Console_log(args):
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
    console_handler.addFilter(config.VerboseFilter(args.verbose))
    logging.getLogger('').addHandler(console_handler)

def print_initial_info(args):
    start_time = datetime.datetime.now()
    initial_info = f"Script started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n" \
                   f"Arguments received: {vars(args)}"
    
    # Log to file
    logging.info(initial_info)
    if not args.verbose:
        print(initial_info)

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
            exit_program(0, True)

if __name__ == "__main__":
    args = utils.parse_arguments()
    config.setup_logging(args)
    config_Console_log(args)
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
            
            data = load_existing_data(file_path)
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

    exit_program(0, False)
