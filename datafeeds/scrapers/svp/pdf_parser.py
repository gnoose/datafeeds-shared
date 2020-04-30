import logging
import re
from datetime import timedelta, date
from io import BytesIO
from typing import List

from datafeeds.common import BillingDatum
from datafeeds.common.typing import BillingDatumItemsEntry
from datafeeds.common.upload import hash_bill, upload_bill_to_s3
from datafeeds.parsers import pdfparser

log = logging.getLogger(__name__)


def process_pdf(service_id: str, statement_dt: date, pdf_filename: str) -> BillingDatum:
    log.info("Parsing text from PDF %s", pdf_filename)
    text = pdfparser.pdf_to_str(pdf_filename)
    """
    example: (no newlines in the data)
    
    [Record*SCL0415A*2622*00061400-04*KEY=00061400-04*Index]City of Santa Clara Municipal 
    UtilitiesREGULAR BILLAccount Number:00061400-04Account Name:3975 FREEDOM CIRCLE
    Service Address:3975 FREEDOM CIRBill Date:04/15/2020Amount Due:$26,475.48Customer Service
    (408) 615-2300  Current ChargesCI_DEMEXCPg 1 of 3  Usage Table and History Graphs  
    Billing CommentsTHIS BILL IS DUE UPON RECEIPT  To avoid late charges and additional fees, 
    pay 26,475.48 IN FULL before 05/06/2020.  For more information, see back of bill.   
    Billing InformationPrevious BalancePayments (04/07/2020) - Thank YouCurrent Charges
    ______________________________________________________________________________________________
    __________________________________________________________
    Amount Due$31,910.84-$31,910.84$26,475.48$26,475.48Silicon Valley Power$25,420.90
    Meter Charge$69.51Energy$18,961.97  169,803 kWh X $.11167/kWh = $18,961.97Demand$6,525.12
    Power Factor Charge-$74.01Primary Voltage Discount-$815.64Public Benefit Charge$703.01
    State Surcharge$50.94Water / Sewer / Solar$1,054.58Water$547.36Backflow Device Charge$39.06
    Sewer$468.16Current Charges Total$26,475.48ServiceTypeMeter ReadingsCurrent       Prior
    This MonthLast Year(Daily Avg.)This MonthThis Year(Daily Avg.)RateMeterNumberUsageMultDays
    Read DatesPrior   CurrentEWW03/0503/0503/0504/0604/0604/0632323214982196201497219542800011169,8041078539551368235713682361CB1VW04W045,3062341,8236,7154,3594,359 
    kWh HCF HCF kWh Gal Gal kWh Gal GalEffective immediately and until further notice Municipal 
    Services has implemented a suspension on service disconnections for nonpayment of utility 
    services kW DemandPower Factor:  88%     536.00    1017.60     776.80Actual:HI:Billed:07/19AMI
    Amount Enclosed:SCL0415A  AUTO  SCH 5-DIGIT 950567000004456 00.0011.0091 
    2191/1FDFADAFTDATAFTTDTDFFAFTDTDAATDDTFTFDTDDATTTTDFDDFADAFATTFFTDFTTTD3975 FREEDOM CIRCLEPRISA 
    LHC LLC DBA 3975 FREEDOM CIRCLE LL3979 FREEDOM CIRCLE STE 135SANTA CLARA CA 
    95054-1245ANEMGPCNDLDMFKBKEKAJEFDAGAFPLOPHFALKAMJLMCHJJNCGJBOAOKAIHOKEKAJCFNEHDHPKDLLDDDDLLLLDLLDDLL
    Please return this portion with your payment in envelope provided. Make check payable to City of 
    Santa Clara.Account No:     00061400-04Route No. 501Bill Date:     04/15/2020Past Due Date: 
    05/06/20200006140000040002647548340CITY OF SANTA CLARAAmount Due:\x0c    
    """

    # TODO: sample values; extract these from text
    cost = 25420.90  # Silicon Valley Power$25,420.90
    used = 169803  # 169,803 kWh X $.11167/kWh
    demand = 536.0  # kW DemandPower Factor:  88%     536.00
    start_date = date(2020, 3, 5)  # CurrentEWW03/0503/0503/0504/0604/0604/06
    end_date = date(2020, 4, 6)
    # adjust end date because SVP bills overlap on start/end dates
    end_date = end_date - timedelta(days=1)
    line_items: List[BillingDatumItemsEntry] = []
    """
    Meter Charge$69.51Energy$18,961.97  169,803 kWh X $.11167/kWh = $18,961.97Demand$6,525.12
    Power Factor Charge-$74.01Primary Voltage Discount-$815.64Public Benefit Charge$703.01
    State Surcharge$50.94
    """
    # from Meter Charge through Water / Sewer
    match = re.search("(Meter Charge.*?)Water", text)
    if match:
        line_items_text = match.group(1)
    """
    get values for
        Meter Charge (quantity=None, rate=None, total=69.51, kind=other, unit=None)
        Energy (quantity=169803, rate=0.11167, total=18961.97, kind=use, unit=kWh)
        Demand (quantity=demand, rate=None, total=6525.12, kind=demand, unit=kW)
        Power Factor Charge (quantity=None, rate=None, total=-74.01, kind=other, unit=None)
        Primary Voltage Discount (quantity=None, rate=None, total=-815.64, kind=other, unit=None)
        Public Benefit Charge (quantity=None, rate=None, total=703.01, kind=other, unit=None)
        State Surcharge (quantity=None, rate=None, total=50.94, kind=other, unit=None)
    create BillingDatumItemsEntry(
        description: str
        quantity: float
        rate: float
        total: float
        kind: str
        unit: str
    """

    key = hash_bill(
        service_id,
        start_date,
        end_date,
        # statement_date will go here (future PR)
        cost,
        demand,
        used,
    )
    with open(pdf_filename, "rb") as pdf_data:
        attachment_entry = upload_bill_to_s3(BytesIO(pdf_data.read()), key)

    return BillingDatum(
        start=start_date,
        end=end_date,
        # keep statement_dt param since it will be added here in another PR
        cost=cost,
        used=used,
        peak=demand,
        items=line_items,
        attachments=[attachment_entry],
    )
