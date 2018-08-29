## ## this file should be in site-packages/zipline/data/bundles/iex.py
## ## add the followign code in         ~/.zipline/extension.py
## ## and to import data run              zipline ingest -b IEX
## 
## from zipline.data.bundles import register
## from zipline.data.bundles.iex import iex_equities
##
## etfs = { 'SPY','XLB','XLE','XLF','XLI','XLK','XLP','XLU','XLV','XLY' }
##
## register( 'IEX', iex_equities(etfs) )

import os
import requests
import numpy as np
import pandas as pd
from dateutil.parser import parse
from datetime import datetime, timedelta
from zipline.utils.cli import maybe_show_progress
from iexfinance import get_historical_data

def _cachpath(symbol, type_):
    return '-'.join((symbol.replace(os.path.sep, '_'), type_))

def iex_equities(symbols):
    """Create a data bundle ingest function from a set of symbols loaded from
    IEX.

    Parameters
    ----------
    symbols : iterable[str]
        The ticker symbols to retrieve (the period is fixed at five years).

    Returns
    -------
    ingest : callable
        The bundle ingest function for the given set of symbols.

    Examples
    --------
    This code should be added to ~/.zipline/extension.py

    .. code-block:: python

       from zipline.data.bundles import register
       from zipline.data.bundles iex_equities

       symbols = ( 'AAPL', 'IBM', 'MSFT', )
       register('IEX', iex_equities(symbols))

    Notes
    -----
    The sids for each symbol will be the index into the symbols sequence.
    """
    # strict this in memory so that we can reiterate over it
    symbols = tuple(symbols)

    def ingest(environ,
               asset_db_writer,
               minute_bar_writer, # ignored
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session, # ignored
               end_session, # ignored
               cache,
               show_progress,
               output_dir):

        metadata = pd.DataFrame(np.empty(len(symbols), dtype=[
            ('start_date', 'datetime64[ns]'),
            ('end_date', 'datetime64[ns]'),
            ('auto_close_date', 'datetime64[ns]'),
            ('symbol', 'object'),
        ]))

        today = datetime.today()
        start = datetime(today.year-5,today.month,today.day)
        
        def _pricing_iter():
            sid = 0
            with maybe_show_progress(
                    symbols,
                    show_progress,
                    label='Downloading IEX pricing data: ') as it, \
                    requests.Session() as session:
                for symbol in it:
                    path = _cachpath(symbol, 'ohlcv')
                    try:
                        df = cache[path]
                    except KeyError:
                        df = cache[path] = get_historical_data(symbol, start=start, end=None, output_format='pandas').sort_index()
                    df.index = pd.to_datetime(df.index)
                    # the start date is the date of the first trade and
                    # the end date is the date of the last trade
                    start_date = df.index[0]
                    end_date = df.index[-1]
                    # The auto_close date is the day after the last trade.
                    ac_date = end_date + pd.Timedelta(days=1)
                    metadata.iloc[sid] = start_date, end_date, ac_date, symbol

                    df.rename(
                        columns={
                            'Open': 'open',
                            'High': 'high',
                            'Low': 'low',
                            'Close': 'close',
                            'Volume': 'volume',
                        },
                        inplace=True,
                    )
                    yield sid, df
                    sid += 1

        daily_bar_writer.write(_pricing_iter(), show_progress=True)

        metadata['exchange'] = "NYSE"
        
        symbol_map = pd.Series(metadata.symbol.index, metadata.symbol)
        asset_db_writer.write(equities=metadata)

        adjustment_writer.write()

    return ingest
