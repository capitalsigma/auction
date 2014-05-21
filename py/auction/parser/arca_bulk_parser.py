from auction.time_utils import *
from auction.paths import *
from multiprocessing import Process, Pool
from path import path
import subprocess
import logging
import os

__ARCA_SRC_PATH__ = DATA_PATH / 'NYSE_ARCA2'

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser("""
        Takes all or a subset of raw data files and generate corresponding hdf5 data files as well as book
        files for selected symbols present in the raw data.
        """)

    parser.add_argument('-p', '--filepattern', 
                    dest='filepattern',
                    action='store',                                          
                    nargs='?',
                    help='File pattern, if empty all files assumed')

    parser.add_argument('-s', '--symbol', 
                    dest='symbols',
                    action='store',
                    nargs='*',             
                    help='Symbols to include')

    options = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    import pprint
    __HERE__ = path(os.path.realpath(__file__))

    def generate_book_data(input):
        args = ["python", __HERE__.parent / "arca_parser.py",] + [ '-d', get_date_string(get_date_of_file(input)) ] + ['-s'] + options.symbols
        logging.info("Generating data input: %s =>\n\t%s"%(input.name, args))
        subprocess.call(args)

    p = Pool(22)
    if options.filepattern:
        input_files = __ARCA_SRC_PATH__.files(options.filepattern)
    else: 
        input_files  = __ARCA_SRC_PATH__.files()

    p.map(generate_book_data, input_files)

