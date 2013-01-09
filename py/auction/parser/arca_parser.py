###############################################################################
#
# File: arca_parser.py
#
# Description: Parser for arca record data files
#
##############################################################################
from path import path
from attribute import readable, writable
from auction.paths import *
from auction.book import Book, BookTable
from auction.parser.parser_summary import ParseManager
from auction.time_utils import *

from tables import *
from sets import Set
import os
import zipfile
import re
import logging
import gzip
import datetime
import time
import string
import pprint
from numpy import zeros, array

__PriceRe__ = re.compile(r"\s*(\d*)(?:\.(\d+))?\s*")
__DateRe__ = re.compile(r"(\d\d\d\d)(\d\d)(\d\d)")
__ARCA_SRC_PATH__ = COMPRESSED_DATA_PATH / 'arca'
__FLUSH_FREQ__ = 10000
__LEVELS__ = 5
__PX_MULTIPLIER__ = 1000000
__PX_DECIMAL_DIGITS__ = 6
__TICK_SIZE__ = 10000

class PriceOrderedDict(object):
    """
    Dictionary with keys sorted by price - quite similar to effect of an
    std::map.
    """
    def __init__(self, ascending = True):
        self.d = {}
        self.L = []
        self.ascending = ascending
        self.sorted = True
    
    def __len__(self):
        return len(self.L)

    def get_quantity(self, px):
        return self.d.get(px, 0)

    def update_quantity(self, px, q):
        assert(q!=0)
        assert(len(self.L) == len(self.d))
        qty = self.d.get(px)
        if qty:
            qty += q
            if qty:
                self.d[px] = qty
            else:
                del self.d[px]
                self.L.remove(px)
        else:
            self.L.append(px)
            self.sorted = False
            self.d[px] = q

    def top(self):
        if not self.sorted:
            self.L.sort()
            self.sorted = True
        if 0 == len(self.L):
            return None
        if self.ascending:
            return self.L[0]
        else:
            return self.L[-1]

def get_date_of_file(fileName):
    """
    Given a filename with a date in it (YYYYMMDD), parse out the date
    Return None if no date present
    """
    m = __DateRe__.search(fileName)
    if m:
        year, month, day = m.groups()
        return datetime.date(int(year), int(month), int(day))
    else:
        return None

def make_timestamp(start_of_date, seconds, millis):
    """
    Given a timestamp for start_of_date, calculate new timestamp given
    additional seconds and millis.
    """
    seconds = int(seconds)
    millis = int(millis)
    return start_of_date + seconds*1000000 + millis*1000

def int_price(px_str):
    """
    Given a price as a string, convert to an integer. All prices are stored as
    64bit integers. The largest decimal I've encountered is 6 places, so all
    prices are aligned to 6 decimals. Given an integer price px, then, the
    quoted price would be px/1e6
    """
    m = __PriceRe__.match(px_str)
    if not m:
        raise RuntimeError("Invalid price " + px_str)
    else:
        int_part, decimal_part = m.groups()

        if int_part == None or int_part == '':
            int_part = 0
        else:
            int_part = int(int_part)

        if not decimal_part:
            decimal_part = "0"
        
        if len(decimal_part) > 6:
            raise RuntimeError("Invalid price - more than 6 decimal places:" + px_str)

        return int_part*__PX_MULTIPLIER__ + \
            int(decimal_part.ljust(__PX_DECIMAL_DIGITS__, '0'))

class AddRecord(object):
    r"""
Stores fields for single Add record
"""

    readable(seq_num=None, order_id=None, exchange=None, is_buy=None,
             quantity=None, symbol=None, price=None, 
             system_code=None, quote_id=None, timestamp=None)

    def __init__(self, fields, start_of_date):
        """
        fields - Line split by ',' (i.e. all fields)
        """
        self.__seq_num = fields[1]
        self.__order_id = fields[2]
        self.__exchange = fields[3]
        self.__is_buy = fields[4] == 'B'
        self.__quantity = int(fields[5])
        self.__symbol = fields[6]
        self.__price = int_price(fields[7])
        self.__system_code = fields[10]
        self.__quote_id = fields[11]
        self.__timestamp = make_timestamp(start_of_date, fields[8], fields[9])

class DeleteRecord(object):
    r"""
Stores fields for single Delete record
"""

    readable(seq_num=None, order_id=None, symbol=None, 
             exchange=None, system_code=None, quote_id=None,
             is_buy=None, timestamp=None) 

    def __init__(self, fields, start_of_date):
        """
        fields - Line split by ',' (i.e. all fields)
        """
        self.__seq_num = fields[1]
        self.__order_id = fields[2]
        self.__symbol = fields[5]
        self.__exchange = fields[6]        
        self.__system_code = fields[7]        
        self.__quote_id = fields[8]
        self.__is_buy = fields[9] == 'B'
        self.__timestamp = make_timestamp(start_of_date, fields[3], fields[4])

class ModifyRecord(object):
    r"""
Stores fields for single Modify record
"""
    readable(seq_num=None, order_id=None, quantity=None, price=None,
             symbol=None, quote_id=None, is_buy=None, timestamp=None) 

    def __init__(self, fields, start_of_date):
        """
        fields - Line split by ',' (i.e. all fields)
        """
        self.__seq_num = fields[1]
        self.__order_id = fields[2]
        self.__quantity = int(fields[3])
        self.__price = int_price(fields[4])
        self.__symbol = fields[7]  
        self.__quote_id = fields[10]
        self.__is_buy = fields[11] == 'B'
        self.__timestamp = make_timestamp(start_of_date, fields[5], fields[6])

class FileRecordCounter(object):
    """
    Periodically the file gets flushed. Flushing too frequently can hurt
    performance. Multiple datasets are stored in a single file, so the count
    is based on number of adds to *any* of the data sets.
    """
    readable(h5_file = None, count = 0)

    def __init__(self, h5_file):
        """
        H5 file object to flush
        """
        self.__h5_file = h5_file
        self.__count = 0

    def increment_count(self):
        """
        Increment counter and flush if __FLUSH_FREQ__ records have been added
        """
        self.__count += 1
        if 0 == (self.__count % __FLUSH_FREQ__):
            self.__h5_file.flush()

class BookBuilder(object):
    """
    Processes Add/Modify/Delete records to build books per symbol
    """

    __book_files__ = {}

    def __init__(self, symbol, h5_file):
        self.__file_record_counter = BookBuilder.__book_files__.get(h5_file, None)
        if not self.__file_record_counter:
            BookBuilder.__book_files__[h5_file] = FileRecordCounter(h5_file)
            self.__file_record_counter = BookBuilder.__book_files__[h5_file]

        h5_file = self.__file_record_counter.h5_file
        filters = Filters(complevel=1, complib='zlib')
        group = h5_file.createGroup("/", symbol, 'Book data')
        self.__table = h5_file.createTable(group, 'books', BookTable, 
                                           "Data for "+str(symbol), filters=filters)
        self.__tick_size = __TICK_SIZE__ # TODO
        self.__record = self.__table.row
        self.__symbol = symbol
        self.__orders = {}
        self.__bids_to_qty = PriceOrderedDict(False)
        self.__asks_to_qty = PriceOrderedDict()
        self.__bids = zeros(shape=[__LEVELS__,2])
        self.__asks = zeros(shape=[__LEVELS__,2])

    def summary(self):
        """
        Prints some summary information for a parse
        """
        print "Completed data for:", self.__symbol
        print "\tOutstanding orders:", len(self.__orders)
        print "\tOutstanding bids:", len(self.__bids_to_qty)
        print "\tOutstanding asks:", len(self.__asks_to_qty)


    def make_record(self, ts, ts_s):
        """
        A new record has been processed and the bids and asks updated
        accordingly. This takes the new price data and updates the book and
        timestamps for storing.
        """
        bid_top = self.__bids_to_qty.top()
        if bid_top:
            for i, px in enumerate(range(bid_top, 
                                         bid_top-__LEVELS__*self.__tick_size, 
                                         -self.__tick_size)):
                self.__bids[i][0] = px
                self.__bids[i][1] = self.__bids_to_qty.get_quantity(px)
        else:
            self.__bids = zeros(shape=[__LEVELS__,2])

        ask_top = self.__asks_to_qty.top()
        if ask_top:
            for i, px in enumerate(range(ask_top, 
                                         ask_top+__LEVELS__*self.__tick_size, 
                                         self.__tick_size)):
                self.__asks[i][0] = px
                self.__asks[i][1] = self.__asks_to_qty.get_quantity(px)
        else:
            self.__asks = zeros(shape=[__LEVELS__,2])

        self.__record['bid'] = self.__bids
        self.__record['ask'] = self.__asks
        self.__record['timestamp'] = ts
        self.__record['timestamp_s'] = ts_s

        if bid_top and ask_top and bid_top >= ask_top:
            msg = [(top_bid==top_ask and "Locked " or "Crossed "),
                   "Market %s"%self.__symbol, 
                   "Bids"+str(self.__bids), 
                   "Asks"+str(self.__asks)]
            raise RuntimeError(string.join(msg,':'))        

    def process_record(self, amd_record):
        """
        Incorporate the contents of the new record into the bids/asks
        """

        if isinstance(amd_record, AddRecord):
            entry = (amd_record.price, amd_record.quantity)
            current = self.__orders.setdefault(amd_record.order_id, entry)
            if current != entry:
                raise RuntimeError("Duplicate add for order: " + amd_record.order_id)

            if amd_record.is_buy:
                self.__bids_to_qty.update_quantity(entry[0], entry[1])
            else:
                self.__asks_to_qty.update_quantity(entry[0], entry[1])

        elif isinstance(amd_record, DeleteRecord):
            assert(amd_record.symbol == self.__symbol)
            current = self.__orders.get(amd_record.order_id, None)
            if not current:
                raise "Record not found for delete: " + amd_record.order_id

            if amd_record.is_buy:
                self.__bids_to_qty.update_quantity(current[0], -current[1])
            else:
                self.__asks_to_qty.update_quantity(current[0], -current[1])

            del self.__orders[amd_record.order_id]

        elif isinstance(amd_record, ModifyRecord):
            assert(amd_record.symbol == self.__symbol)
            current = self.__orders.get(amd_record.order_id, None)
            if not current:
                raise "Record not found for modify: " + amd_record.order_id

            if amd_record.is_buy:
                self.__bids_to_qty.update_quantity(current[0], -current[1])
                self.__bids_to_qty.update_quantity(amd_record.price, amd_record.quantity)
            else:
                self.__asks_to_qty.update_quantity(current[0], -current[1])
                self.__asks_to_qty.update_quantity(amd_record.price, amd_record.quantity)

            self.__orders[amd_record.order_id] = (amd_record.price, amd_record.quantity)
        else:
            raise RuntimeError("Invalid record: " + amd_record)

        # bids and asks have been updated, now update the record and append to the table
        self.make_record(amd_record.timestamp, 
                         chicago_time_str(amd_record.timestamp))
        self.__record.append()
        self.__file_record_counter.increment_count()

class ArcaRecord(IsDescription):
    asc_ts      = StringCol(12)
    ts          = Int64Col()
    seq_num     = Int64Col()
    order_id    = Int64Col()
    symbol      = StringCol(8) 
    price       = Int64Col()
    quantity    = Int32Col()
    record_type = StringCol(1) # 'A', 'M', 'D'
    buy_sell    = StringCol(1) # 'B', 'S'
    
class ArcaFixParser(object):
    r"""

Parse arca files and create book

"""


    def __init__(self, input_path, input_date, file_tag, symbol_match_re = None):
        """
        input_path - path to input data
        input_date - date of data in file
        file_tag - tag for naming the file
        symbol_match_re - regex on which symbols to include
        """
        self.__input_path = input_path
        self.__date = input_date
        self.__file_tag = file_tag
        self.__output_base = __ARCA_SRC_PATH__ / 'h5' / (str(self.__date)+'_'+file_tag)
        self.__symbol_match_re = symbol_match_re
        self.__start_of_date = start_of_date(self.__date.year, self.__date.month,
                                             self.__date.day, NY_TZ)

        self.__book_builders = {}

        if not self.__input_path.exists():
            raise RuntimeError("Input path does not exist " + self.__input_path)

    def parse(self, build_book = True, stop_early_at_hit=0):
        """
        Parse the input file. There are two modes: build_book=True and
        build_book=False. If build_book=False, the h5 file is simply the same
        record data from the gz file, but stored as hdf5. If build_book=True,
        the hdf5 file created has book data for all matching inputs. Each
        symbol gets it's own dataset.

        The ParseManager is used to store summary information for the parse of
        this data.
        """
        self.__output_path = self.__output_base + (build_book and ".h5" or "_AMD_.h5")
        logging.info("Parsing file %s\n\tto create %s"% (self.__input_path, self.__output_path))
        self.__h5_file = openFile(self.__output_path, mode = "w", title = "ARCA Equity Data")
        if not build_book:
            ## If not building book, then just writing out AMD data as hdf5
            filters = Filters(complevel=1, complib='zlib')
            group = self.__h5_file.createGroup("/", '_AMD_Data_', 'Add-Modify-Delete data')
            table = self.__h5_file.createTable(group, 'records', ArcaRecord, 
                                               "Data for "+str(self.__date), filters=filters)
            h5Record = table.row

        self.__parse_manager = ParseManager(self.__input_path, self.__h5_file)
        self.__parse_manager.mark_start()

        hit_count = 0
        data_start_timestamp = None

        for self.__line_number, line in enumerate(gzip.open(self.__input_path, 'rb')):

            if stop_early_at_hit and hit_count > stop_early_at_hit:
                break 

            ###################################################
            # Show progress periodically
            ###################################################
            if 0 == (self.__line_number % 1000000):
                logging.info("At %d hit count is %d on %s" % 
                             (self.__line_number, hit_count, 
                              (self.__symbol_match_re and 
                               self.__symbol_match_re.pattern or "*")))

            fields = line.split(',')
            code = fields[0]
            record = None
            if code == 'A':
                record = AddRecord(fields, self.__start_of_date)
            elif code == 'D':
                record = DeleteRecord(fields, self.__start_of_date)
            elif code == 'M':
                record = ModifyRecord(fields, self.__start_of_date)
            elif code == 'I':
                continue
            else:
                raise RuntimeError("Unexpected record type '" + 
                                   code + "' at line " + str(self.__line_number) + 
                                   " of file " + self.__input_path)

            if self.__symbol_match_re and \
                    not self.__symbol_match_re.search(record.symbol):
                continue
            else:
                hit_count += 1

                # record the timestamp of the first record as data_start
                if not data_start_timestamp:
                    data_start_timestamp = record.timestamp

                if build_book:
                    self.build_books(record)
                else:
                    h5Record['ts'] = record.timestamp
                    h5Record['asc_ts'] = chicago_time_str(record.timestamp)
                    h5Record['symbol'] = record.symbol
                    h5Record['seq_num'] = record.seq_num
                    h5Record['order_id'] = record.order_id
                    h5Record['record_type'] = code
                    h5Record['buy_sell'] = (record.is_buy and 'B' or 'S')
                    if code != 'D':
                        h5Record['price'] = record.price
                        h5Record['quantity'] = record.quantity

                    h5Record.append()

                    if 0 == self.__line_number % __FLUSH_FREQ__:
                        table.flush()

        for symbol, builder in self.__book_builders.iteritems():
            builder.summary()

        ############################################################
        # Finish filling in the parse summary info and close up
        ############################################################
        self.__parse_manager.data_start(data_start_timestamp)
        self.__parse_manager.data_stop(record.timestamp)
        self.__parse_manager.processed(self.__line_number+1)
        self.__parse_manager.mark_stop(True)
        self.__h5_file.close()


    def build_books(self, record):
        """
        Dispatch the new record to the appropriate BookBuilder for the symbol
        """
        builder = self.__book_builders.get(record.symbol, None)
        if not builder:
            builder = BookBuilder(record.symbol, self.__h5_file)
            self.__book_builders[record.symbol] = builder

        try:
            builder.process_record(record)
        except Exception,e:
            self.__parse_manager.warning(record.symbol +': ' + e.message, self.__line_number+1)
            

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser("""
Take input raw data and generate corresponding hdf5 data files as well as book
files for symbols present in the raw data.
""")

    parser.add_argument('-d', '--date', 
                        dest='dates',
                        action='store',
                        nargs='*',
                        help='Date(s) to process, if empty all dates assumed')

    parser.add_argument('-s', '--symbol', 
                        dest='symbols',
                        action='store',
                        nargs='*',
                        help='Symbols to include')

    parser.add_argument('-f', '--force', 
                        dest='force',
                        action='store_true',
                        help='Overwrite existing files')

    parser.add_argument('-v', '--verbose', 
                        dest='verbose',
                        action='store_true',
                        help='Output extra logging information')

    options = parser.parse_args()

    if options.verbose:
        logging.basicConfig(level=logging.INFO)

    src_compressed_files = []
    if options.dates:
        all_files = __ARCA_SRC_PATH__.files()
        for date in options.dates:
            src_compressed_files += filter(lambda f: f.find(date)>=0, all_files)
    else:
        src_compressed_files = __ARCA_SRC_PATH__.files()

    symbol_text = None
    if not options.symbols:
        options.symbols = [ 
            'SPY', 'DIA', 'QQQ', 
            'XLK', 'XLF', 'XLP', 'XLE', 'XLY', 'XLV', 'XLB',
            'CSCO','MSFT', 'HD', 'LOW',
            ]
        symbol_text = 'BASKET'

    options.symbols.sort()
    re_text = r'\b(?:' + string.join(options.symbols, '|') + r')\b'
    symbol_re = re.compile(re_text)
    if not symbol_text:
        symbol_text = string.join(options.symbols, '_')

    for compressed_src in src_compressed_files:
        print "Examining", compressed_src.basename()
        date = get_date_of_file(compressed_src)
        if date:
            parser = ArcaFixParser(compressed_src, date, symbol_text, symbol_re)
            parser.parse(True, 5000)
