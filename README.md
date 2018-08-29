# zipline-IEX-bundle

Custom bundle to import IEX data into zipline (http://www.zipline.io).

Useful because the standard bundles do not include ETFs.

This file should be in site-packages/zipline/data/bundles/iex.py

Add the followign code in         ~/.zipline/extension.py

    from zipline.data.bundles import register
    from zipline.data.bundles.iex import iex_equities

    etfs = { 'SPY','XLB','XLE','XLF','XLI','XLK','XLP','XLU','XLV','XLY' }
    register( 'IEX', iex_equities(etfs) )
    
To import the selected symbols from IEX run

    zipline ingest -b IEX
