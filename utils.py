import config
import logging
import argparse

# Command line argument parsing function
def parse_arguments():
    parser = argparse.ArgumentParser(description="Scrape Uber Eats data")
    parser.add_argument("--country", "-c", type=str, nargs='+', help="Scrape data from specific countries. If not specified, all countries will be scraped.", metavar="")
    parser.add_argument("--threads", "-t", type=int, default=5, help="Number of threads to use for scraping")
    parser.add_argument("--resume",  "-r", action="store_true", help="Resume scraping from failed links")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    parser.add_argument("--debug",   "-d", action="store_true", help="Enable detailed logging and error tracing for debugging purposes.")
    return parser.parse_args()
