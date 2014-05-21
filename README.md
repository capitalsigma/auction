This directory contains code to build continuous limit order books from CME, NASDAQ, and NYSE data. To run scripts in this library, please include a path to the `py` directory in PYTHONPATH. 

To parse the NYSE ARCA data for symbol GOOG for all files in 2010, run the following command:

    python py/auction/parser/arca_bulk_parser.py -s GOOG -p *2010*
    
Relative paths to the relevant input and output directories are set in `/py/auction/paths.py`. 
