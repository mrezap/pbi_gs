import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config import load_config
from data_extractor import DataExtractor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

DAX_QUERY = r"""
DEFINE
    VAR _maxDate = MAX('calendar lite 2'[Date])
    VAR _minDate = DATE(YEAR(_maxDate), MONTH(_maxDate), 1)
    VAR _period = TREATAS(VALUES('calendar lite 2'[Date]), 'calendar lite 2'[Date])
    VAR _filteredTable =
        FILTER(
            SUMMARIZECOLUMNS(
                'master_BP'[Dealer Full],
                'master_SC'[UNIQ SC],
                'master_SC'[NAME SC],
                'master_SC'[Brand],
                'master_SC'[Date],
                'calendar lite 2'[Month-Year],
                _period,
                "TTL_Target_Rofo", 'Metrics'[TTL Target Rofo],
                "Total_Nett_No_Tax", 'Metrics'[Total Nett No Tax],
                "QVO_Potential", 'Metrics'[QVO Potential]
            ),
            'master_SC'[Date] >= _minDate && 'master_SC'[Date] <= _maxDate && NOT ISBLANK( [TTL_Target_Rofo] )
        )
EVALUATE
SELECTCOLUMNS(
    _filteredTable,
    "Period", [Month-Year],
    "SC Uniqcode", [UNIQ SC],
    "Brand", [Brand],
    "Dealer", [Dealer Full],
    "Total Target", [TTL_Target_Rofo],
    "Total Sales", COALESCE([Total_Nett_No_Tax], 0 ),
    "QVO Potential", COALESCE([QVO_Potential], 0 )
)
"""

HEADER = ["Period", "UNIQ SC", "Brand", "Dealer", "Target Rofo", "Total Sales", "QVO Potential"]
WORKSHEET_NAME = "dealer_target"
LOG_WORKSHEET = "logs"

def main():
    try:
        cfg = load_config()
        with DataExtractor(cfg) as extractor:
            count = extractor.extract_and_push(
                DAX_QUERY, 
                WORKSHEET_NAME, 
                HEADER,
                log_worksheet=LOG_WORKSHEET,
                script_name="extract_dealertarget"
            )
            logging.info(f"Extraction complete: {count} rows processed")
    except Exception as e:
        logging.exception("Error during extraction")
        raise

if __name__ == "__main__":
    main()