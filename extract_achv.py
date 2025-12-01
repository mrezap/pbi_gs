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
    VAR _table =
        VAR _maxDate = MAX ( 'calendar lite 2'[Date] )
        VAR _minDate = DATE( YEAR( _maxDate ), MONTH(_maxDate ), 1)
        RETURN
            ADDCOLUMNS(
                SUMMARIZE( master_SC, master_SC[UNIQ ASH], master_SC[NAME ASH], master_SC[UNIQ SC], master_SC[NAME SC], master_SC[Date] ),
                "Target Rofo", CALCULATE([TTL Target Rofo], DATESBETWEEN( 'calendar lite 2'[Date], _minDate, _maxDate )),
                "Total Sales", CALCULATE([Total Nett No Tax], DATESBETWEEN( 'calendar lite 2'[Date], _minDate, _maxDate )),
                "Tgt BO", CALCULATE([Total Target BO], DATESBETWEEN( 'calendar lite 2'[Date], _minDate, _maxDate )),
                "Acv BO", CALCULATE([BO Achv], DATESBETWEEN( 'calendar lite 2'[Date], _minDate, _maxDate )),
                "Tgt QVO", CALCULATE([Total Target QVO], DATESBETWEEN( 'calendar lite 2'[Date], _minDate, _maxDate )),
                "Acv QVO", CALCULATE([QVO Achv], DATESBETWEEN( 'calendar lite 2'[Date], _minDate, _maxDate ))
            )
EVALUATE
    FILTER(
        SELECTCOLUMNS(
            _table,
            "Period", IF( NOT ISBLANK( [Total Sales] ), FORMAT([Date], "MMM-yyyy" ) ),
            "ASH Uniqcode", [UNIQ ASH],
            "SC Uniqcode", [UNIQ SC],
            "Target Rofo", IF( ISBLANK( [Target Rofo] ), 0, [Target Rofo] ),
            "Total Sales", [Total Sales],
            "Target BO", IF( ISBLANK( [Tgt BO] ), 0, [Tgt BO] ),
            "Actual BO", IF( ISBLANK( [Acv BO] ), 0, [Acv BO] ),
            "Target QVO", IF( ISBLANK( [Tgt QVO] ), 0, [Tgt QVO] ),
            "Actual QVO", IF( ISBLANK( [Acv QVO] ), 0, [Acv QVO] )
        ),
        NOT ISBLANK( [Period] )
    )
"""

HEADER = ["Period", "UNIQ ASH", "UNIQ SC", "Target Rofo", "Total Sales", "Target BO", "Achv BO", "Target QVO", "Achv QVO"]
WORKSHEET_NAME = "data_achv"

def main():
    cfg = load_config()
    with DataExtractor(cfg) as extractor:
        count = extractor.extract_and_push(DAX_QUERY, WORKSHEET_NAME, HEADER)
        logging.info(f"✅ Extraction complete: {count} rows processed")

if __name__ == "__main__":
    main()