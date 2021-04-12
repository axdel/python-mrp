import fdb
import os

from collections import Counter
from time import perf_counter

from django.conf import settings
from django.utils import timezone

from base.crypto import get_hash
from base.utils import get_logger, json_loads, strip_spaces, to_linux_newlines, create_chunks, parse_price

logger = get_logger(__name__)


class MRP_CASH_REGISTER_PAYMENT:
    ID = 'IDR'
    DATE = 'DATUM'
    DATETIME = 'LOG_DATE'
    AMOUNT = 'CASTKA'
    RAW_DATA = 'ZPRAVA'
    IS_REFUND = 'UID_STORNO'

class MRP_INVOICE:
    ID = 'IDFAK'
    VARIABLE_SYMBOL = 'VARSYMB'
    PAID_BY_VARIABLE_SYMBOL = 'CIS_PREDF'
    COMPANY_ID_NUMBER = 'ICO'
    TOTAL = 'CELKEM'
    ISSUE_DATE = 'DATVYSTAVE'
    ISSUE_DATETIME = 'LOG_DATE'
    DUE_DATE = 'DATSPLATNO'
    PAYMENT_METHOD = 'FORMAUHRAD'
    SHIPPING_METHOD = 'SPOSOBDOPR'
    UPDATE_COUNT = 'UPDCNT'

MRP_INVOICE_PAYMENT_METHODS = (1, 3)
MRP_INVOICE_CASH_PAYMENT = 'hotovost'
MRP_INVOICE_VARIABLE_SYMBOL_REGEXP = '20%'
MRP_INVOICE_MAX_CREDIT_NOTE_VALUE = -5000
MRP_PROFORMA_INVOICE_VARIABLE_SYMBOL_REGEXP = '920%'

class MRP_INVOICE_PAYMENT:
    ID = 'IDR'
    INVOICE_ID = 'IDFAK'
    BANK_ID = 'IDBANKY'
    AMOUNT = 'CIASTKA'
    AMOUNT_IN_CURRENCY = 'CSTMENA'
    AMOUNT_IN_CURRENCY_ON_INVOICE = 'CSTMENAFA'
    CURRENCY = 'MENA'
    DATE = 'DATUM'
    DATETIME = 'LOG_DATE'
    PAYMENT_METHOD = 'ZPUSOBUHR'
    LOG_USER = 'LOG_USER'
    UPDATE_COUNT = 'UPDCNT'

MRP_INVOICE_PAYMENT_BANK_ID = 6101
MRP_INVOICE_PAYMENT_CURRENCY = 'EUR'
MRP_INVOICE_PAYMENT_LOG_USER = 'MRPDBA'
MRP_INVOICE_PAYMENT_METHOD = 1

class MRP_PAYMENT_METHOD:
    BANK_TRANSFER = 1
    CASH = 2
    CASH_ON_DELIVERY = 3

class MRP_PRODUCT:
    ID = 'IDR'
    NUMBER = 'CISLO'
    NAME = 'NAZOV2'
    METATAGS = 'KOD1'
    CATEGORY_NUMBER = 'CISKAT'
    GROUP_NUMBER = 'SKUPINA'
    SKU = 'NAZOV'
    EAN = 'KOD'
    ESHOP_FLAG = 'KOD2'
    ESHOP_INFO = 'KOD3'
    SMALL_NOTE = 'POZNAMKA'
    ATTRIBUTES = 'POZNAMKA1'
    UNITS = 'MJ'
    UNITS_MULTIPLIER = 'ZAKLPOCMJ'
    VAT_PERCENT = 'SADZBADPH'
    WARRANTY = 'USRFLD1'
    # __UNDEF__ = 'USRFLD2'
    # __UNDEF__ = 'USRFLD3'
    # __UNDEF__ = 'USRFLD4'
    # __UNDEF__ = 'USRFLD5'
    STOCK_MINIMUM = 'MINIMUM'

MRP_PRODUCT_STOCK_NUMBERS = (1, 2)  # stock 2 deleted
MRP_PRODUCT_ESHOP_FLAG_REGEXP = 'ESHOP%'

class MRP_PRODUCT_CATEGORY:
    ID = 'IDR'
    NAME = 'POPIS'
    NUMBER = 'CISKAT'
    PARENT_NUMBER = 'UCISKAT'
    ORDER = 'PORADIKAT'

class MRP_PRODUCT_CATEGORY_EX:
    PRODUCT_ID = 'IDRKAR'
    CATEGORY_NUMBER = 'CISKAT'

class MRP_PRODUCT_DETAIL:
    PRODUCT_ID = 'IDSKKAR'
    DESCRIPTION = 'VELPOPIS'
    ATTRIBUTES = 'VELPOPIS2'
    UPDATE_COUNT = 'UPDCNT'

class MRP_PRODUCT_GROUP:
    NUMBER = 'SKUPINA'
    NAME = 'NAZOV'

class MRP_PRODUCT_ITEM:
    ID = 'IDR'
    MASTER_PRODUCT_ID = 'IDSKKARM'
    SLAVE_PRODUCT_ID = 'IDSKKARS'
    SLAVE_PRODUCT_COUNT = 'POCETMJS'

class MRP_PRODUCT_STATUS:
    PRODUCT_ID = 'IDRKAR'
    PRICE1 = 'CENA1'
    PRICE2 = 'CENA2'
    PRICE3 = 'CENA3'
    PRICE4 = 'CENA4'
    PRICE5 = 'CENA5'
    STOCK_QUANTITY = 'POCETMJ'
    STOCK_NUMBER = 'CISLOSKL'
    UPDATE_COUNT = 'UPDCNT'

class MRP_SHIPPING_METHODS:
    NONE = ''
    GW = 'G.W.'
    GLS = 'GLS'
    PERSONALY = 'osobne'

class MRP_STOCK_MOVEMENT:
    ID = 'IDPOH'
    DATE = 'DATUM'
    STOCK_NUMBER = 'CISLOSKL'
    MOVEMENT_NUMBER = 'DRUHPOHYBU'
    VARIABLE_SYMBOL = 'CISLOFAK'
    COMPANY_ID_NUMBER = 'ICO'
    IS_EXPENSE = 'JEPRIJEM'
    TOTAL = 'CELKOM'

MRP_STOCK_MOVEMENT_NUMBERS = (1, 2, 3)

class MRP_USER:
    ID = 'IDRADR'
    NAME = 'MENO'
    ADDRESS = 'ULICA'
    ZIP = 'PSC'
    CITY = 'MESTO'
    COUNTRY = 'STAT'
    COUNTRY_CODE = 'KODSTAT'
    PHONE = 'TELEFON'
    PHONE2 = 'TELEFON2'
    PHONE3 = 'TELEFON3'
    EMAIL = 'EMAIL'
    INDIVIDUAL = 'FYZOSOB'
    COMPANY_NAME = 'FIRMA'
    COMPANY_ID_NUMBER = 'ICO'
    COMPANY_TAX_ID = 'DIC'
    COMPANY_VAT_ID = 'IC_DPH'
    DUE_DATE_DAYS = 'SPLATNOST'
    PRICE_GROUP = 'CENSKUP'
    ADDED = 'DAT_ZAR'
    SMALL_NOTE = 'INE'
    NOTE = 'POZNAMKA'
    UPDATE_COUNT = 'UPDCNT'

class MRP_TABLE:
    CASH_REGISTER_PAYMENT = 'EKASA_LOG'
    CASH_REGISTER_PAYMENT_DETAIL = 'MOARCHIV'
    INVOICE = 'FAKVY'
    # INVOICE_ITEM = 'FAKVYPOL'
    INVOICE_PAYMENT = 'FAKVYUHR'
    PRODUCT = 'SKKAR'
    PRODUCT_CATEGORY = 'SKKARKAT'
    PRODUCT_CATEGORY_EX = 'SKKARKATEX'
    PRODUCT_DETAIL = 'SKKARDET'
    PRODUCT_GROUP = 'SKKARSKU'
    PRODUCT_ITEM = 'SKKARPOL'  # compounded products
    PRODUCT_STATUS = 'SKKARSTA'
    STOCK_MOVEMENT = 'SKPOH'
    USER = 'ADRES'

MRP_INTEGRITY_CHECK_TABLES = {
    MRP_TABLE.CASH_REGISTER_PAYMENT: ['CASTKA', 'CISFA', 'CUSTOMER', 'CUSTOMERID', 'DATUM', 'EMAIL', 'GUID', 'IDR', 'ID_FV', 'ID_MO', 'ID_SV', 'ISSUEDATE', 'LOG_DATE', 'LOG_USER', 'ODPOVED', 'OKP', 'PARAGON', 'REC_TYPE', 'STATE', 'TYP_EKASY', 'UID', 'UID_STORNO', 'ZPRAVA'],
    MRP_TABLE.INVOICE: ['CALCPARAM', 'CELKEM', 'CELK_ZAHR', 'CENYSDPH', 'CISLO', 'CISLODODLI', 'CISLOOBJED', 'CISLO_ZAK', 'CISPLATKAR', 'CIS_PREDF', 'DATDODANI', 'DATOBJED', 'DATSPLATNO', 'DATVYSTAVE', 'DATZDANPLN', 'DOBROPIS_PRO', 'DOTRIGGER', 'DPH1', 'DPH2', 'DRUH', 'EET_STAV', 'EKODAND', 'EKOKOM', 'FIXACECST', 'FORMAUHRAD', 'HMOTNOST', 'ICO', 'ICOPRIJ', 'IDBANKY', 'IDFAK', 'IDKONTAKT', 'KH_LEASING', 'KODPLNENI', 'KONSTSYMB', 'KURZ_EUR', 'KURZ_EUR_P', 'KURZ_SK', 'KURZ_ZAHR', 'LOG_DATE', 'LOG_USER', 'MENA', 'MIMODPH', 'ORIGCIS2', 'ORIGCISDOK', 'ORIGCISLO', 'PDANALYT', 'PDSYNTET', 'PLATKAR', 'POZNAMKA', 'REZIM_DPH', 'SKONTO', 'SKONTODNY', 'SKONTOPROC', 'SPECISYMB', 'SPOSOBDOPR', 'SPOTRDAND', 'STAT_DPH', 'STORNO_FA', 'STORNO_PRO', 'STREDISKO', 'TYPDPH', 'TYP_DOKL', 'TYP_POL', 'UDPREDKONT', 'UPDCNT', 'USRFLD1', 'USRFLD2', 'USRFLD3', 'USRFLD4', 'USRFLD5', 'VARSYMB', 'VATNUMBER', 'VRUBOPIS_PRO', 'ZAKL0', 'ZAKL1', 'ZAKL2', 'ZAPLACCISLO', 'ZLAVA'],
    # MRP_TABLE.INVOICE_ITEM: ['CENAMJ', 'CISLOKAR', 'CISLO_ZAK', 'DATUMDO', 'DATUMOD', 'DPH', 'HMOTNOST', 'IDFAK', 'IDOBJPOL', 'IDOPR', 'IDOPRMAT', 'IDR', 'IDSKPOH', 'IDSKPOHPOL', 'LOG_DATE', 'LOG_USER', 'MJ', 'POCETMJ', 'RIADOK', 'SADZBADPH', 'SLEVAMJ', 'STREDISKO', 'TEXT', 'TYPDPH', 'TYP_POL', 'TYP_RADKU', 'TYP_SUM', 'UPDCNT', 'ZLAVA'],
    MRP_TABLE.INVOICE_PAYMENT: ['CIASTKA', 'CSTMENA', 'CSTMENAFA', 'DATUM', 'DOKLAD', 'DOTRIGGER', 'IDBANKY', 'IDBANOBRAT', 'IDFAK', 'IDINTDOK', 'IDPOKDOK', 'IDPOKL', 'IDR', 'IDRX', 'IDUHR', 'IDZAPOCPOL', 'KURZ', 'KURZ_POCJEDN', 'LOG_DATE', 'LOG_USER', 'MENA', 'UHRADAFM', 'UPDCNT', 'ZPUSOBUHR'],
    MRP_TABLE.PRODUCT: ['BALENIE', 'BALENI_DEL', 'BEZDPH', 'CELKEMMJ', 'CELKOBJMJ', 'CELKOPRMJ', 'CELKREZMJ', 'CELKSPRMJ', 'CENA0', 'CENA1', 'CENA2', 'CENA3', 'CENA4', 'CENA5', 'CENAMJ', 'CENYSDPH', 'CISKAT', 'CISLO', 'DAN0', 'DAT_ZAR', 'DELKA', 'DMJ', 'DODAVATEL', 'DOMINUSU', 'DOMINUSU_Z', 'DOPLKOD', 'DOPREPCEN', 'EKO_KOD', 'HMOTNOST', 'IDOBAL_B', 'IDOBAL_K', 'IDOBAL_P', 'IDR', 'JEDNOTKMJ', 'JEDNPOCMJ', 'KOD', 'KOD1', 'KOD2', 'KOD3', 'KOEFDMJ', 'KOEFDSV', 'KOEFEDMJ', 'KOEFSDMJ', 'KOEFSSBL', 'KUSY', 'LIH_KOD', 'LOG_DATE', 'LOG_USER', 'MAKROPRCEN', 'MAXIMUM', 'MAXSLEVAL', 'MAXSLEVAP', 'MINIMUM', 'MJ', 'NAZOV', 'NAZOV2', 'NAZOV3', 'NEHMOTPRD', 'NORMA', 'POLSA', 'POUZIVANA', 'POZNAMKA', 'POZNAMKA1', 'PUBLIKOVAT', 'PUVODKR', 'PUVODST', 'RABAT1', 'RABAT2', 'RABAT3', 'RABAT4', 'RABAT5', 'RABATZC', 'RABPROC1', 'RABPROC2', 'RABPROC3', 'RABPROC4', 'RABPROC5', 'RECYKL_INC', 'RECYKL_KOD', 'SADZBADPH', 'SELLER', 'SELLERID', 'SIRKA', 'SKUPINA', 'SLOZKART', 'SPOTR_KOD', 'TLAC', 'TYPKARTY', 'TYPSAZBY', 'TYP_POL', 'UPDCNT', 'USRFLD1', 'USRFLD2', 'USRFLD3', 'USRFLD4', 'USRFLD5', 'USRLOCK', 'VARIANTGRP', 'VYSKA', 'ZAKAZCENSK', 'ZAKAZSLEVY', 'ZAKLPOCMJ'],
    MRP_TABLE.PRODUCT_CATEGORY: ['CISKAT', 'IDR', 'POPIS', 'PORADIKAT', 'UCISKAT'],
    MRP_TABLE.PRODUCT_DETAIL: ['IDSKKAR', 'MALOBR', 'MALOBRAZ', 'MALPOPIS', 'MALPOPIS2', 'UPDCNT', 'VELOBR', 'VELOBRAZ', 'VELPOPIS', 'VELPOPIS2'],
    MRP_TABLE.PRODUCT_GROUP: ['IDR', 'NAZOV', 'SKUPINA'],
    MRP_TABLE.PRODUCT_ITEM: ['IDR', 'IDSKKARM', 'IDSKKARS', 'JEUCETPOL', 'POCETMJM', 'POCETMJS', 'TYP', 'UPDCNT'],
    MRP_TABLE.PRODUCT_STATUS: ['AKTIVNI', 'CENA0', 'CENA1', 'CENA2', 'CENA3', 'CENA4', 'CENA5', 'CENAMJ', 'CHECKMAX', 'CHECKMIN', 'CHECKREZ', 'CHECKSTAV', 'CISLOKAR', 'CISLOSKL', 'DOPOCPOC', 'DOPOCREZMJ', 'DOPREPCEN', 'DPH1', 'DPH2', 'DPH3', 'DPH4', 'DPH5', 'IDRKAR', 'INVENTURA', 'MAXIMUM', 'MINIMUM', 'NORMA', 'PLU', 'POCETMJ', 'POCOBJMJ', 'POCOPRMJ', 'POCPOCETMJ', 'POCREZMJ', 'POCSPRMJ', 'POZICE', 'RABAT1', 'RABAT2', 'RABAT3', 'RABAT4', 'RABAT5', 'RABPROC1', 'RABPROC2', 'RABPROC3', 'RABPROC4', 'RABPROC5', 'UPDCNT'],
    MRP_TABLE.STOCK_MOVEMENT: ['BRUTTO', 'CALCPARAM', 'CAS', 'CELKOM', 'CELK_ZAHR', 'CENYSDPH', 'CIASTKA', 'CISLOFAK', 'CISLOOBJED', 'CISLOPOH', 'CISLOSKL', 'CISLO_ZAK', 'DATDODANI', 'DATOBJED', 'DATUM', 'DATUMIS', 'DODPODM', 'DPH', 'DPH1', 'DPH2', 'DRUH', 'DRUHDOPR', 'DRUHPOHYBU', 'EKODAND', 'FIXACECST', 'FORMAUHRAD', 'ICO', 'ICOPRIJ', 'IDFAKPR', 'IDFAKVY', 'IDFIRPOB', 'IDKONTAKT', 'IDOSPOH', 'IDOSZAV', 'IDPOH', 'IDPOKDOK', 'INTRASTAT', 'JEPLATNY', 'JEPRIJEM', 'KODSTAT', 'KURZ_EUR', 'KURZ_EUR_P', 'KURZ_SK', 'KURZ_ZAHR', 'LIHDANV', 'LOG_DATE', 'LOG_USER', 'MENA', 'MIMODPH', 'NAKLADY', 'NETTO', 'NEWNAKL', 'OCENENO', 'ORDDATCAS', 'ORIGCISLO', 'ORIGDATUM', 'POZNAMKA', 'PVCISLOPOH', 'REZIM_DPH', 'SESAZBOUDPH', 'SLEVKARTYP', 'SPOSOBDOPR', 'SPOTRDAND', 'STAT_DPH', 'STORNO', 'STREDISKO', 'TRANSAKCE', 'TYPDPH', 'UCCASTKA', 'UDCASTKA', 'UPD_USER', 'UPD_DATE', 'UPDCNT', 'USRFLD1', 'USRFLD2', 'USRFLD3', 'USRFLD4', 'USRFLD5', 'USRLOCK', 'VARSYMB', 'VATNUMBER', 'ZAKKARTA', 'ZAKL0', 'ZAKL1', 'ZAKL2', 'ZAKL_ZAHR', 'ZAUC_EET', 'ZAUC_FISK', 'ZISKCAST', 'ZISKPROC', 'ZLAVA', 'ZVLPOH'],
    MRP_TABLE.USER: ['ADRESTYP', 'CENSKUP', 'CISOB', 'CISORP', 'CISPOVOL', 'CRPDATNESP', 'CRPKONTDAT', 'CRPSTATUS', 'DAN_URAD', 'DATNAROZ', 'DAT_ZAR', 'DIC', 'DLINHEXP', 'DLINHPROF', 'DODAVATEL', 'DOTRIGGER', 'EANKOD', 'EANSYS', 'EANSYS_DL', 'EMAIL', 'FAKAUTOPRN', 'FAKEMAIL', 'FAKINHEXP', 'FAKINHPROF', 'FAKPDFPWD', 'FAKSLEVA', 'FAKSTRED', 'FAX', 'FIRMA', 'FIRMA2', 'FORMAUHRAD', 'FYZOSOB', 'ICO', 'ICOPRIJ', 'IC_DPH', 'ID', 'IDBANKY', 'IDDODTXT', 'IDKONTAKT', 'IDRADR', 'INE', 'KODADR', 'KODSTAT', 'KREDIT', 'LOG_DATE', 'LOG_USER', 'MENO', 'MESTO', 'NA_PLATNO', 'OBJEMAIL', 'ODBERATEL', 'PDANALYTFP', 'PDANALYTFV', 'PDSYNTETFP', 'PDSYNTETFV', 'POZNAMKA', 'PSC', 'SKONTODNY', 'SKONTOPROC', 'SPECSYMBFP', 'SPECSYMBFV', 'SPLATNOST', 'SPOSOBDOPR', 'STAT', 'TELEFON', 'TELEFON2', 'TELEFON3', 'TEMP_REC', 'TLAC', 'TOLERSPL', 'TYPPOVOL', 'UDPREDKFP', 'UDPREDKFV', 'ULICA', 'UPDCNT', 'USRFLD1', 'USRFLD2', 'USRFLD3', 'USRFLD4', 'USRFLD5', 'VARSYMBFP', 'VARSYMBFV', 'VELOBCH'],
}

def TO_MRP_NEWLINES(string):
    mrp_string = "' || ASCII_CHAR(13) || ASCII_CHAR(10) || '".join(
        list(map(lambda sp: sp.strip(), to_linux_newlines(string).strip().split('\n')))
    )
    return f"'{mrp_string}'"


class MrpIntegrityError(Exception):
    pass


class MrpService:

    def __init__(self, mrp_year=None):
        self.connection = None
        self.cursor = None
        self.mrp_year = mrp_year or timezone.now().year

    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.connection.commit()
        self.connection.close()
        logger.debug('Connection to MRP closed')

    def _connect(self):
        MRP_DATABASE = os.path.join(settings.MRP_DATA_PATH, settings.MRP_DATA_FILES[self.mrp_year])
        self.connection = fdb.connect(
            host=settings.MRP_HOST, port=settings.MRP_PORT,
            database=MRP_DATABASE,
            user=settings.MRP_USER, password=settings.MRP_PASSWORD,
            charset='WIN1250'
        )
        self.cursor = self.connection.cursor()
        logger.debug('Connection to MRP (year: %s) successful [firebird://.../%s]', self.mrp_year, MRP_DATABASE)

    def _execute(self, query):
        query = strip_spaces(query)
        logger.debug('Executing SQL: %s', query)
        started = perf_counter()
        self.cursor.execute(query)
        logger.debug('Executed in %fs', (perf_counter()-started))

    def _fetchall(self):
        logger.debug('Fetching results')
        started = perf_counter()
        results = []
        for row in self.cursor.fetchall():
            results.append(row[0] if len(row) == 1 else row)
        logger.debug('Fetched %d results in %fs', len(results), (perf_counter() - started))
        return results

    def _fetchone(self):
        fetchall = self._fetchall()
        return fetchall[0] if fetchall else None

    def _fetchallmap(self):
        logger.debug('Fetching results (map)')
        started = perf_counter()
        results = []
        for row in self.cursor.fetchallmap():
            results.append({key: value for key, value in row.items()})
        logger.debug('Fetched %d results in %fs', len(results), (perf_counter() - started))
        return results

    def _fetchonemap(self):
        fetchallmap = self._fetchallmap()
        return fetchallmap[0] if fetchallmap else None

    def _get_table_fields(self, table_name):
        query = f'''
            SELECT TRIM(RDB$FIELD_NAME) FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME='{table_name}' ORDER BY RDB$FIELD_NAME
        '''
        self._execute(query)
        return self._fetchall()

    def _integrity_check(self):
        logger.info('Starting MRP integrity check')
        for table_name, table_fields in MRP_INTEGRITY_CHECK_TABLES.items():
            fields_diff = list(set(self._get_table_fields(table_name)).symmetric_difference(set(table_fields)))
            if fields_diff:
                raise MrpIntegrityError(f'{table_name} table structure changed, symmetric diff: {fields_diff}')
            logger.info('%s table structure OK', table_name)

    #
    # CASH REGISTER
    #
    def get_cash_register_records_by_date(self, mrp_date):
        cash_register_records = []
        query = f'''
            SELECT
                {MRP_CASH_REGISTER_PAYMENT.ID} AS ID,
                {MRP_CASH_REGISTER_PAYMENT.AMOUNT} AS AMOUNT,
                {MRP_CASH_REGISTER_PAYMENT.DATETIME} AS DATETIME,
                {MRP_CASH_REGISTER_PAYMENT.RAW_DATA} AS RAW_DATA,
                TRIM({MRP_CASH_REGISTER_PAYMENT.IS_REFUND}) AS IS_REFUND
            FROM
                {MRP_TABLE.CASH_REGISTER_PAYMENT}
            WHERE
                {MRP_CASH_REGISTER_PAYMENT.DATE} = '{mrp_date}'
        '''
        self._execute(query)
        for cash_register_record in self._fetchallmap():
            raw_data = json_loads(cash_register_record.pop('RAW_DATA'))['ReceiptData']
            cash_register_record.update({
                'DISCOUNT': parse_price(0),
                'CASHIER': raw_data['Custom']['Cashier'],
                'CARD': parse_price(raw_data['Custom'].get('PaymentCard', 0)),
                'CASH': parse_price(raw_data['Custom'].get('PaymentCash', 0)),
                'IS_REFUND': bool(cash_register_record['IS_REFUND']),
                'VARIABLE_SYMBOL': raw_data.get('InvoiceNumber', '')
            })
            if raw_data['ReceiptType'] == 'PD':
                cash_register_record['DISCOUNT'] = parse_price(sum([item['Price'] for item in raw_data['Items'] if item['ItemType'] == 'Z']))
            cash_register_records.append(cash_register_record)
        total_amount = sum([r['AMOUNT'] for r in cash_register_records])
        card_amount = sum([r['CARD'] for r in cash_register_records])
        cash_amount = sum([r['CASH'] for r in cash_register_records])
        cashiers_stats = dict(Counter([r['CASHIER'] for r in cash_register_records if not r['IS_REFUND']]))  # drop refunds
        discount_amount = sum([r['DISCOUNT'] for r in cash_register_records])
        return {
            'CASH_REGISTER_RECORDS': cash_register_records,
            'CARD_AMOUNT': card_amount,
            'CASH_AMOUNT': cash_amount,
            'DISCOUNT_AMOUNT': discount_amount,
            'TOTAL_AMOUNT': total_amount,
            'CUSTOMERS': sum(cashiers_stats.values()),
            'CASHIERS_STATS': cashiers_stats,
        }

    #
    # HELPERS
    #
    def get_company_id_numbers_by_stock_movements_date(self, mrp_date):
        query = f'''
            SELECT
                REPLACE(TRIM({MRP_TABLE.STOCK_MOVEMENT}.{MRP_STOCK_MOVEMENT.COMPANY_ID_NUMBER}), ' ', '') AS COMPANY_ID_NUMBER
            FROM
                {MRP_TABLE.STOCK_MOVEMENT}
            WHERE
                {MRP_TABLE.STOCK_MOVEMENT}.{MRP_STOCK_MOVEMENT.DATE} = '{mrp_date}'
                AND {MRP_TABLE.STOCK_MOVEMENT}.{MRP_STOCK_MOVEMENT.MOVEMENT_NUMBER} IN {MRP_STOCK_MOVEMENT_NUMBERS}
            GROUP BY
                REPLACE(TRIM({MRP_TABLE.STOCK_MOVEMENT}.{MRP_STOCK_MOVEMENT.COMPANY_ID_NUMBER}), ' ', '')
        '''
        self._execute(query)
        return list(filter(None, self._fetchall()))

    #
    # INVOICES
    #
    def add_invoice_payment(self, mrp_invoice_id, mrp_paid_amount, mrp_payment_date):
        query = f'''
            INSERT INTO {MRP_TABLE.INVOICE_PAYMENT} (
                {MRP_INVOICE_PAYMENT.ID},
                {MRP_INVOICE_PAYMENT.INVOICE_ID},
                {MRP_INVOICE_PAYMENT.BANK_ID},
                {MRP_INVOICE_PAYMENT.AMOUNT},
                {MRP_INVOICE_PAYMENT.AMOUNT_IN_CURRENCY},
                {MRP_INVOICE_PAYMENT.AMOUNT_IN_CURRENCY_ON_INVOICE},
                {MRP_INVOICE_PAYMENT.DATE},
                {MRP_INVOICE_PAYMENT.PAYMENT_METHOD},
                {MRP_INVOICE_PAYMENT.CURRENCY},
                {MRP_INVOICE_PAYMENT.LOG_USER}
            )
            VALUES (
                (SELECT MAX({MRP_INVOICE_PAYMENT.ID}) FROM {MRP_TABLE.INVOICE_PAYMENT}) + 1,
                {mrp_invoice_id},
                {MRP_INVOICE_PAYMENT_BANK_ID},
                {mrp_paid_amount},
                {mrp_paid_amount},
                {mrp_paid_amount},
                '{mrp_payment_date}',
                {MRP_INVOICE_PAYMENT_METHOD},
                '{MRP_INVOICE_PAYMENT_CURRENCY}',
                '{MRP_INVOICE_PAYMENT_LOG_USER}'
            )
            RETURNING
                {MRP_INVOICE_PAYMENT.ID}
        '''
        self._execute(query)
        return self._fetchone()

    def get_exposure_by_date(self, mrp_date):
        # TODO: subtract current overpayments
        query = f'''
            SELECT
                1,
                COUNT(1) AS INVOICES,
                SUM(TOTAL) AS EXPOSURE,
                SUM(CASE WHEN DUE_DATE < '{mrp_date}' THEN 1 ELSE 0 END) AS OVERDUE_INVOICES,
                SUM(CASE WHEN DUE_DATE < '{mrp_date}' THEN TOTAL ELSE 0 END) AS OVERDUE_EXPOSURE
            FROM (
                SELECT
                    CASE
                        WHEN ({MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL} - SUM({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.AMOUNT})) > 0
                        THEN ({MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL} - SUM({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.AMOUNT}))  -- partial payment
                        ELSE {MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL}
                    END AS TOTAL,
                    {MRP_TABLE.INVOICE}.{MRP_INVOICE.DUE_DATE} AS DUE_DATE
                FROM
                    {MRP_TABLE.INVOICE}
                    LEFT JOIN {MRP_TABLE.INVOICE_PAYMENT} ON ({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.INVOICE_ID} = {MRP_TABLE.INVOICE}.{MRP_INVOICE.ID})
                WHERE
                    {MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL} != 0
                    AND {MRP_TABLE.INVOICE}.{MRP_INVOICE.VARIABLE_SYMBOL} LIKE '{MRP_INVOICE_VARIABLE_SYMBOL_REGEXP}'
                    AND {MRP_TABLE.INVOICE}.{MRP_INVOICE.ISSUE_DATE} <= '{mrp_date}'
                GROUP BY
                    {MRP_TABLE.INVOICE}.{MRP_INVOICE.ID},
                    {MRP_TABLE.INVOICE}.{MRP_INVOICE.DUE_DATE},
                    {MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL}
                HAVING
                    (COALESCE(MAX({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.DATE}), '2030-01-01') > '{mrp_date}'
                        AND COALESCE(SUM({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.AMOUNT}), {MRP_INVOICE_MAX_CREDIT_NOTE_VALUE}) <= {MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL})
                    OR COALESCE(SUM({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.AMOUNT}), -1000) < {MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL}
            ) GROUP BY 1
        '''
        self._execute(query)
        return self._fetchonemap()

    def _get_invoices_base(self, where_clause=None, having_clause=None):
        WHERE = f''
        HAVING = f''
        if where_clause: WHERE += f' WHERE {where_clause}'
        if having_clause: HAVING += f' HAVING {having_clause}'
        query = f'''
            SELECT
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.ID} AS ID,
                TRIM({MRP_TABLE.USER}.{MRP_USER.NAME}) AS NAME,
                TRIM({MRP_TABLE.USER}.{MRP_USER.COMPANY_NAME}) AS COMPANY_NAME,
                REPLACE(TRIM({MRP_TABLE.USER}.{MRP_USER.COMPANY_ID_NUMBER}), ' ', '') AS COMPANY_ID_NUMBER,
                REPLACE(TRIM({MRP_TABLE.USER}.{MRP_USER.COMPANY_TAX_ID}), ' ', '') AS COMPANY_TAX_ID,
                REPLACE(TRIM({MRP_TABLE.USER}.{MRP_USER.COMPANY_VAT_ID}), ' ', '') AS COMPANY_VAT_ID,
                TRIM({MRP_TABLE.INVOICE}.{MRP_INVOICE.VARIABLE_SYMBOL}) AS VARIABLE_SYMBOL,
                TRIM({MRP_TABLE.INVOICE}.{MRP_INVOICE.PAID_BY_VARIABLE_SYMBOL}) AS PAID_BY_VARIABLE_SYMBOL,
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.ISSUE_DATE} AS ISSUE_DATE,
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.ISSUE_DATETIME} AS ISSUE_DATETIME,
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.DUE_DATE} AS DUE_DATE,
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL} AS TOTAL,
                ({MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL} - COALESCE(SUM({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.AMOUNT}), 0)) AS MISSING,
                LIST({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.AMOUNT}) AS PAYMENTS,
                LIST({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.DATE}) AS PAYMENTS_DATES,
                COALESCE(SUM({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.AMOUNT}), 0) AS PAYMENTS_SUM,
                MAX({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.DATE}) AS PAID_DATE,
                CASE WHEN SUM({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.AMOUNT}) >= {MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL} THEN 1 ELSE 0 END AS IS_PAID,
                CASE WHEN SUM({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.AMOUNT}) < {MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL} THEN 1 ELSE 0 END AS IS_PARTIALLY_PAID,
                CASE WHEN SUM({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.AMOUNT}) > {MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL} THEN 1 ELSE 0 END AS IS_OVERPAID,
                CASE WHEN {MRP_TABLE.INVOICE}.{MRP_INVOICE.VARIABLE_SYMBOL} LIKE '{MRP_PROFORMA_INVOICE_VARIABLE_SYMBOL_REGEXP}' THEN 1 ELSE 0 END AS IS_PROFORMA,
                CASE WHEN {MRP_TABLE.INVOICE}.{MRP_INVOICE.DUE_DATE} < COALESCE(MAX({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.DATE}), CAST('NOW' AS DATE)) THEN 1 ELSE 0 END AS IS_OVERDUE,
                CASE WHEN DATEADD(1 DAY TO {MRP_TABLE.INVOICE}.{MRP_INVOICE.DUE_DATE}) = CAST('NOW' AS DATE) THEN 1 ELSE 0 END AS IS_FRESH_OVERDUE,
                CASE WHEN {MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL} < 0 THEN 1 ELSE 0 END AS IS_CREDIT_NOTE,
                TRIM({MRP_TABLE.INVOICE}.{MRP_INVOICE.SHIPPING_METHOD}) AS SHIPPING_METHOD,
                TRIM({MRP_TABLE.INVOICE}.{MRP_INVOICE.PAYMENT_METHOD}) AS PAYMENT_METHOD
            FROM
                {MRP_TABLE.INVOICE}
                LEFT JOIN {MRP_TABLE.USER} ON ({MRP_TABLE.USER}.{MRP_USER.COMPANY_ID_NUMBER} = {MRP_TABLE.INVOICE}.{MRP_INVOICE.COMPANY_ID_NUMBER})
                LEFT JOIN {MRP_TABLE.INVOICE_PAYMENT} ON ({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.INVOICE_ID} = {MRP_TABLE.INVOICE}.{MRP_INVOICE.ID})
            { WHERE }
            GROUP BY
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.ID},
                {MRP_TABLE.USER}.{MRP_USER.NAME},
                {MRP_TABLE.USER}.{MRP_USER.COMPANY_NAME},
                {MRP_TABLE.USER}.{MRP_USER.COMPANY_ID_NUMBER},
                {MRP_TABLE.USER}.{MRP_USER.COMPANY_TAX_ID},
                {MRP_TABLE.USER}.{MRP_USER.COMPANY_VAT_ID},
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.VARIABLE_SYMBOL},
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.PAID_BY_VARIABLE_SYMBOL},
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.ISSUE_DATE},
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.ISSUE_DATETIME},
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.DUE_DATE},
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL},
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.SHIPPING_METHOD},
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.PAYMENT_METHOD}
            { HAVING }
            ORDER BY
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.VARIABLE_SYMBOL} ASC
        '''
        self._execute(query)
        invoices = self._fetchallmap()
        for invoice in invoices:
            FLAGS = []
            FLAGS_SHORT = []
            if invoice['IS_PARTIALLY_PAID']: FLAGS.append('ČIASTOČNE UHRADENÁ'); FLAGS_SHORT.append('ČU')
            if invoice['IS_CREDIT_NOTE']: FLAGS.append('DOBROPIS'); FLAGS_SHORT.append('DP')
            if invoice['IS_PROFORMA']: FLAGS.append('PROFORMA FAKTÚRA'); FLAGS_SHORT.append('PF')
            if invoice['IS_OVERPAID']: FLAGS.append('PREPLATOK'); FLAGS_SHORT.append('PP')
            if invoice['IS_OVERDUE'] or invoice['IS_FRESH_OVERDUE']: FLAGS.append('PO SPLATNOSTI'); FLAGS_SHORT.append('PS')
            invoice['FLAGS'] = ','.join(FLAGS)
            invoice['FLAGS_SHORT'] = ','.join(FLAGS_SHORT)
            if invoice['TOTAL'] == 0: invoice['IS_PAID'] = 1  # all 0-invoices are a priori paid
            # TODO: paid by other invoice, info in INVOICE_ITEM table
            if invoice['IS_PROFORMA'] and invoice['PAID_BY_VARIABLE_SYMBOL']:  # (proforma) invoice paid by other invoice
                paid_by_variable_symbol = invoice['PAID_BY_VARIABLE_SYMBOL']
                self._execute(f'''
                    SELECT
                        LIST({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.AMOUNT}) AS PAYMENTS,
                        LIST({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.DATE}) AS PAYMENTS_DATES,
                        COALESCE(SUM({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.AMOUNT}), 0) AS PAYMENTS_SUM,
                        MAX({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.DATE}) AS PAID_DATE
                    FROM
                        {MRP_TABLE.INVOICE_PAYMENT}
                    WHERE
                        {MRP_INVOICE_PAYMENT.INVOICE_ID} = (
                            SELECT {MRP_INVOICE.ID} FROM {MRP_TABLE.INVOICE} WHERE {MRP_INVOICE.VARIABLE_SYMBOL} = '{paid_by_variable_symbol}'
                        )
                    GROUP BY
                        {MRP_INVOICE_PAYMENT.INVOICE_ID}
                ''')
                paid_by_invoice = self._fetchonemap()
                # -- proforma invoice cannot be overpaid (will cause double overpayment!) ---
                if paid_by_invoice['PAYMENTS_SUM'] > invoice['TOTAL']: paid_by_invoice['PAYMENTS_SUM'] = invoice['TOTAL']
                # -- proforma invoice cannot be overpaid (will cause double overpayment) ---
                invoice['MISSING'] = invoice['TOTAL'] - paid_by_invoice['PAYMENTS_SUM']
                invoice['PAYMENTS'] = paid_by_invoice['PAYMENTS']
                invoice['PAYMENTS_DATES'] = paid_by_invoice['PAYMENTS_DATES']
                invoice['PAYMENTS_SUM'] = paid_by_invoice['PAYMENTS_SUM']
                invoice['PAID_DATE'] = paid_by_invoice['PAID_DATE']
                invoice['IS_PAID'] = int(bool(paid_by_invoice['PAYMENTS_SUM'] >= invoice['TOTAL']))  # set as paid if paid by other invoice
                invoice['IS_OVERPAID'] = int(bool(paid_by_invoice['PAYMENTS_SUM'] > invoice['TOTAL']))
        return invoices

    def get_invoice_by_id(self, mrp_invoice_id):
        invoice = self.get_invoices_by_ids([mrp_invoice_id])
        return invoice[0] if invoice else None

    def get_invoices_by_company_id_number(self, mrp_company_id_number):
        where_clause = f'''
            REPLACE(TRIM({MRP_TABLE.INVOICE}.{MRP_INVOICE.COMPANY_ID_NUMBER}), ' ', '') = '{mrp_company_id_number}'
        '''
        return self._get_invoices_base(where_clause=where_clause)

    def get_invoices_by_date(self, mrp_date):
        return self.get_invoices_by_date_range(mrp_date, mrp_date)

    def get_invoices_by_date_range(self, mpr_date_from, mrp_date_to):
        where_clause = f'''
            {MRP_TABLE.INVOICE}.{MRP_INVOICE.ISSUE_DATE} >= '{mpr_date_from}'
            AND {MRP_TABLE.INVOICE}.{MRP_INVOICE.ISSUE_DATE} <= '{mrp_date_to}'
        '''
        invoices = self._get_invoices_base(where_clause=where_clause)
        total_amount = sum([i['TOTAL'] for i in invoices if not i['IS_PROFORMA']])
        missing_amount = sum([i['MISSING'] for i in invoices])
        return {
            'INVOICES': invoices,
            'TOTAL_AMOUNT': total_amount,
            'MISSING_AMOUNT': missing_amount,
        }

    def get_invoices_by_due_date(self, mrp_date):
        where_clause = f'''
            {MRP_TABLE.INVOICE}.{MRP_INVOICE.DUE_DATE} = '{mrp_date}'
        '''
        invoices = self._get_invoices_base(where_clause=where_clause)
        total_amount = sum([i['TOTAL'] for i in invoices if not i['IS_PROFORMA']])
        missing_amount = sum([i['MISSING'] for i in invoices])
        missing_count = len([i for i in invoices if not i['IS_PAID']])
        return {
            'INVOICES': invoices,
            'TOTAL_AMOUNT': total_amount,
            'MISSING_AMOUNT': missing_amount,
            'MISSING_COUNT': missing_count,
        }

    def get_invoices_by_ids(self, mrp_invoices_ids):
        invoices = []
        mrp_invoices_ids_chunks = create_chunks(mrp_invoices_ids, 250)  # firebird limit for IN is 1500
        for mrp_invoices_ids_chunk in mrp_invoices_ids_chunks:
            where_clause = f'''
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.ID} IN ({', '.join(map(str, mrp_invoices_ids_chunk))})
            '''
            invoices += self._get_invoices_base(where_clause=where_clause)
        return invoices

    def get_invoices_by_price(self, mrp_price):
        where_clause = f'''
            {MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL} = {mrp_price}
        '''
        return self._get_invoices_base(where_clause=where_clause)

    def get_invoice_by_variable_symbol(self, mrp_variable_symbol):
        invoice = self.get_invoices_by_variable_symbols([mrp_variable_symbol])
        return invoice[0] if invoice else None

    def get_invoices_by_variable_symbols(self, mrp_variable_symbols):
        where_clause = f'''
            {MRP_TABLE.INVOICE}.{MRP_INVOICE.VARIABLE_SYMBOL} IN ({', '.join(map(lambda vs: f"'{vs}'", mrp_variable_symbols))})
        '''
        return self._get_invoices_base(where_clause=where_clause)

    def get_invoices_states(self):
        query = f'''
            SELECT
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.ID},
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.UPDATE_COUNT},
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL},
                COALESCE(SUM({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.AMOUNT}), 0)
            FROM
                {MRP_TABLE.INVOICE}
                LEFT JOIN {MRP_TABLE.INVOICE_PAYMENT} ON ({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.INVOICE_ID} = {MRP_TABLE.INVOICE}.{MRP_INVOICE.ID})
            GROUP BY
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.ID},
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.UPDATE_COUNT},
                {MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL}
        '''

        self._execute(query)
        invoices_states = self._fetchall()
        logger.debug('Hashing invoices states')
        started = perf_counter()
        results = [(s[0], get_hash(s[1:])) for s in invoices_states]  # tuple(id, hash)
        logger.debug('Hashed in %fs', (perf_counter() - started))
        return results

    def get_paid_invoices_by_date(self, mrp_date, mrp_report_mode=False):
        return self.get_paid_invoices_by_date_range(mrp_date, mrp_date, mrp_report_mode)

    def get_paid_invoices_by_date_range(self, mpr_date_from, mrp_date_to, mrp_report_mode=False):
        REPORT_MODE_CONDITNION = ''
        if mrp_report_mode:
            REPORT_MODE_CONDITNION = f'''
                OR (CAST({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.DATETIME} AS DATE) >= '{mpr_date_from}'
                    AND CAST({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.DATETIME} AS DATE) <= '{mrp_date_to}')
            '''
        where_clause = f'''
            {MRP_TABLE.INVOICE}.{MRP_INVOICE.ID} IN (
                SELECT
                    {MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.INVOICE_ID}
                FROM
                    {MRP_TABLE.INVOICE_PAYMENT}
                WHERE
                    ({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.DATE} >= '{mpr_date_from}'
                    AND {MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.DATE} <= '{mrp_date_to}')
                    {REPORT_MODE_CONDITNION}
                GROUP BY
                    {MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.INVOICE_ID}
            )
        '''
        invoices = self._get_invoices_base(where_clause=where_clause)
        total_amount = sum([i['TOTAL'] for i in invoices if not i['IS_PROFORMA']])
        missing_amount = sum([i['MISSING'] for i in invoices])
        return {
            'INVOICES': invoices,
            'TOTAL_AMOUNT': total_amount,
            'MISSING_AMOUNT': missing_amount,
        }

    def get_unpaid_invoices(self):
        having_clause = f'''
            COALESCE(SUM({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.AMOUNT}), {MRP_INVOICE_MAX_CREDIT_NOTE_VALUE}) < {MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL}
        '''
        invoices = [i for i in self._get_invoices_base(having_clause=having_clause) if not i['IS_PAID']]  # drop paid proforma invoices (won't be catched by SQL)
        total_amount = sum([i['TOTAL'] for i in invoices])
        missing_amount = sum([i['MISSING'] for i in invoices])
        overdue_invoices = list(filter(lambda i: i['IS_OVERDUE'], invoices))
        overdue_amount = sum([i['MISSING'] for i in overdue_invoices])
        return {
            'INVOICES': invoices,
            'OVERDUE_INVOICES': overdue_invoices,
            'TOTAL_AMOUNT': total_amount,
            'MISSING_AMOUNT': missing_amount,
            'OVERDUE_AMOUNT': overdue_amount,
        }

    def get_overpaid_invoices(self):
        having_clause = f'''
            COALESCE(SUM({MRP_TABLE.INVOICE_PAYMENT}.{MRP_INVOICE_PAYMENT.AMOUNT}), {MRP_INVOICE_MAX_CREDIT_NOTE_VALUE}) > {MRP_TABLE.INVOICE}.{MRP_INVOICE.TOTAL}
        '''
        invoices = self._get_invoices_base(having_clause=having_clause)
        overpaid_amount = sum([i['MISSING'] for i in invoices])
        return {
            'INVOICES': invoices,
            'OVERPAID_AMOUNT': overpaid_amount,
        }

    #
    # PRODUCTS
    #
    def get_category_by_id(self, mrp_category_id):
        category = self.get_categories_by_ids([mrp_category_id])
        return category[0] if category else None

    def get_categories_by_ids(self, mrp_categories_ids):
        query = f'''
            SELECT
                {MRP_TABLE.PRODUCT_CATEGORY}.{MRP_PRODUCT_CATEGORY.ID} AS ID,
                TRIM({MRP_TABLE.PRODUCT_CATEGORY}.{MRP_PRODUCT_CATEGORY.NAME}) AS NAME,
                CAST({MRP_TABLE.PRODUCT_CATEGORY}.{MRP_PRODUCT_CATEGORY.NUMBER} AS INTEGER) AS NUMBER,
                CAST(COALESCE({MRP_TABLE.PRODUCT_CATEGORY}.{MRP_PRODUCT_CATEGORY.PARENT_NUMBER}, 0) AS INTEGER) AS PARENT_NUMBER,
                CAST({MRP_TABLE.PRODUCT_CATEGORY}.{MRP_PRODUCT_CATEGORY.ORDER} AS INTEGER) AS "ORDER"
            FROM
                {MRP_TABLE.PRODUCT_CATEGORY}
            WHERE
                {MRP_TABLE.PRODUCT_CATEGORY}.{MRP_PRODUCT_CATEGORY.ID} IN ({', '.join(map(str, mrp_categories_ids))})
            ORDER BY
                {MRP_TABLE.PRODUCT_CATEGORY}.{MRP_PRODUCT_CATEGORY.ID} ASC
        '''
        self._execute(query)
        return self._fetchallmap()

    def get_categories_states(self):
        query = f'''
            SELECT
                {MRP_TABLE.PRODUCT_CATEGORY}.{MRP_PRODUCT_CATEGORY.ID},
                {MRP_TABLE.PRODUCT_CATEGORY}.{MRP_PRODUCT_CATEGORY.NAME},
                {MRP_TABLE.PRODUCT_CATEGORY}.{MRP_PRODUCT_CATEGORY.NUMBER},
                {MRP_TABLE.PRODUCT_CATEGORY}.{MRP_PRODUCT_CATEGORY.PARENT_NUMBER},
                {MRP_TABLE.PRODUCT_CATEGORY}.{MRP_PRODUCT_CATEGORY.ORDER}
            FROM
                {MRP_TABLE.PRODUCT_CATEGORY}
            ORDER BY
                {MRP_TABLE.PRODUCT_CATEGORY}.{MRP_PRODUCT_CATEGORY.ID} ASC
        '''
        self._execute(query)
        categories_states = self._fetchall()
        logger.debug('Hashing categories states')
        started = perf_counter()
        results = [(cs[0], get_hash(cs[1:])) for cs in categories_states]  # tuple(id, hash)
        logger.debug('Hashed in %fs', (perf_counter() - started))
        return results

    def get_product_by_number(self, mrp_product_number):
        query = f'''
            SELECT
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ID}
            FROM
                {MRP_TABLE.PRODUCT}
            WHERE
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.NUMBER} = {mrp_product_number}
        '''
        self._execute(query)
        mrp_product_id = self._fetchone()
        if not mrp_product_id: return None  # CISLO does not exist
        return self.get_product_by_id(mrp_product_id)

    def get_product_by_id(self, mrp_product_id):
        product = self.get_products_by_ids([mrp_product_id])
        return product[0] if product else None

    def get_products_by_ids(self, mrp_products_ids):
        products = []
        mrp_products_ids_chunks = create_chunks(mrp_products_ids, 250)  # firebird limit for IN is 1500
        for mrp_products_ids_chunk in mrp_products_ids_chunks:
            query = f'''
                SELECT
                    {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ID} AS ID,
                    CAST({MRP_TABLE.PRODUCT}.{MRP_PRODUCT.NUMBER} AS INTEGER) AS NUMBER,
                    TRIM({MRP_TABLE.PRODUCT}.{MRP_PRODUCT.NAME}) AS NAME,
                    COALESCE(CAST({MRP_TABLE.PRODUCT}.{MRP_PRODUCT.CATEGORY_NUMBER} AS INTEGER), 0) AS CATEGORY_NUMBER,
                    '' AS CATEGORY_NUMBER_EX,
                    COALESCE(TRIM({MRP_TABLE.PRODUCT_GROUP}.{MRP_PRODUCT_GROUP.NAME}), '') AS GROUP_NAME,
                    TRIM({MRP_TABLE.PRODUCT}.{MRP_PRODUCT.METATAGS}) AS METATAGS,
                    TRIM({MRP_TABLE.PRODUCT}.{MRP_PRODUCT.EAN}) AS EAN,
                    TRIM({MRP_TABLE.PRODUCT}.{MRP_PRODUCT.SKU}) AS SKU,
                    TRIM({MRP_TABLE.PRODUCT}.{MRP_PRODUCT.UNITS}) AS UNITS,
                    CAST({MRP_TABLE.PRODUCT}.{MRP_PRODUCT.UNITS_MULTIPLIER} AS INTEGER) AS UNITS_MULTIPLIER,
                    CAST({MRP_TABLE.PRODUCT}.{MRP_PRODUCT.VAT_PERCENT} AS INTEGER) AS VAT_PERCENT,
                    TRIM({MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ESHOP_FLAG}) AS ESHOP_FLAG,
                    TRIM({MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ESHOP_INFO}) AS ESHOP_INFO,
                    CAST(TRIM('0' || {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.WARRANTY}) AS INTEGER) AS WARRANTY,
                    COALESCE(TRIM({MRP_TABLE.PRODUCT_DETAIL}.{MRP_PRODUCT_DETAIL.DESCRIPTION}), '') AS DESCRIPTION,
                    COALESCE(TRIM({MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ATTRIBUTES}), '') AS ATTRIBUTES,
                    MAX({MRP_TABLE.PRODUCT_STATUS}.{MRP_PRODUCT_STATUS.PRICE1}) AS PRICE1,
                    MAX({MRP_TABLE.PRODUCT_STATUS}.{MRP_PRODUCT_STATUS.PRICE2}) AS PRICE2,
                    MAX({MRP_TABLE.PRODUCT_STATUS}.{MRP_PRODUCT_STATUS.PRICE3}) AS PRICE3,
                    MAX({MRP_TABLE.PRODUCT_STATUS}.{MRP_PRODUCT_STATUS.PRICE4}) AS PRICE4,
                    MAX({MRP_TABLE.PRODUCT_STATUS}.{MRP_PRODUCT_STATUS.PRICE5}) AS PRICE5,
                    CAST(SUM({MRP_TABLE.PRODUCT_STATUS}.{MRP_PRODUCT_STATUS.STOCK_QUANTITY}) AS INTEGER) AS STOCK_QUANTITY,
                    CAST({MRP_TABLE.PRODUCT}.{MRP_PRODUCT.STOCK_MINIMUM} AS INTEGER) AS STOCK_MINIMUM,
                    COALESCE({MRP_TABLE.PRODUCT_ITEM}.{MRP_PRODUCT_ITEM.MASTER_PRODUCT_ID}, 0) AS MASTER_PRODUCT_ID,
                    '' AS SLAVE_PRODUCTS_NAMES,
                    '' AS SLAVE_PRODUCTS_SKUS
                FROM
                    {MRP_TABLE.PRODUCT}
                    LEFT JOIN {MRP_TABLE.PRODUCT_DETAIL} ON ({MRP_TABLE.PRODUCT_DETAIL}.{MRP_PRODUCT_DETAIL.PRODUCT_ID} = {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ID})
                    LEFT JOIN {MRP_TABLE.PRODUCT_GROUP} ON ({MRP_TABLE.PRODUCT_GROUP}.{MRP_PRODUCT_GROUP.NUMBER} = {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.GROUP_NUMBER})
                    LEFT JOIN {MRP_TABLE.PRODUCT_ITEM} ON ({MRP_TABLE.PRODUCT_ITEM}.{MRP_PRODUCT_ITEM.SLAVE_PRODUCT_ID} = {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ID})
                    LEFT JOIN {MRP_TABLE.PRODUCT_STATUS} ON ({MRP_TABLE.PRODUCT_STATUS}.{MRP_PRODUCT_STATUS.PRODUCT_ID} = {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ID})
                WHERE
                    {MRP_PRODUCT_STATUS.STOCK_NUMBER} IN {MRP_PRODUCT_STOCK_NUMBERS}
                    AND {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ID} IN ({', '.join(map(str, mrp_products_ids_chunk))})
                GROUP BY
                    {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ID},
                    {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.NUMBER},
                    {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.NAME},
                    {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.CATEGORY_NUMBER},
                    {MRP_TABLE.PRODUCT_GROUP}.{MRP_PRODUCT_GROUP.NAME},
                    {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.METATAGS},
                    {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.EAN},
                    {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.SKU},
                    {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.UNITS},
                    {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.UNITS_MULTIPLIER},
                    {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.VAT_PERCENT},
                    {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ESHOP_FLAG},
                    {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ESHOP_INFO},
                    {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.WARRANTY},
                    {MRP_TABLE.PRODUCT_DETAIL}.{MRP_PRODUCT_DETAIL.DESCRIPTION},
                    {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ATTRIBUTES},
                    {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.STOCK_MINIMUM},
                    {MRP_TABLE.PRODUCT_ITEM}.{MRP_PRODUCT_ITEM.MASTER_PRODUCT_ID}
                ORDER BY
                    {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ID} ASC
            '''
            self._execute(query)
            products_chunk = self._fetchallmap()
            for product in products_chunk:
                # EXTENDED CATEGORIES
                self._execute(f'''
                    SELECT
                        COALESCE(LIST({MRP_TABLE.PRODUCT_CATEGORY_EX}.{MRP_PRODUCT_CATEGORY_EX.CATEGORY_NUMBER}), '') AS CATEGORY_NUMBER_EX
                    FROM
                        {MRP_TABLE.PRODUCT_CATEGORY_EX}
                    WHERE
                        {MRP_TABLE.PRODUCT_CATEGORY_EX}.{MRP_PRODUCT_CATEGORY_EX.PRODUCT_ID} = {product['ID']}
                    GROUP BY
                        {MRP_TABLE.PRODUCT_CATEGORY_EX}.{MRP_PRODUCT_CATEGORY_EX.PRODUCT_ID}
                ''')
                extended_categories = self._fetchonemap()
                if extended_categories: product.update(extended_categories)
                # COMPOUNDED PRODUCT CHECK
                master_product_id = product['ID']
                self._execute(f'''
                    SELECT
                        TRIM({MRP_TABLE.PRODUCT}.{MRP_PRODUCT.NAME}) AS SLAVE_PRODUCT_NAME,
                        TRIM({MRP_TABLE.PRODUCT}.{MRP_PRODUCT.SKU}) AS SLAVE_PRODUCT_SKU,
                        {MRP_TABLE.PRODUCT_ITEM}.{MRP_PRODUCT_ITEM.SLAVE_PRODUCT_ID} AS SLAVE_PRODUCT_ID,
                        {MRP_TABLE.PRODUCT_ITEM}.{MRP_PRODUCT_ITEM.SLAVE_PRODUCT_COUNT} AS SLAVE_PRODUCT_COUNT
                    FROM
                        {MRP_TABLE.PRODUCT_ITEM}
                        LEFT JOIN {MRP_TABLE.PRODUCT} ON ({MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ID} = {MRP_TABLE.PRODUCT_ITEM}.{MRP_PRODUCT_ITEM.SLAVE_PRODUCT_ID})
                    WHERE
                        {MRP_TABLE.PRODUCT_ITEM}.{MRP_PRODUCT_ITEM.MASTER_PRODUCT_ID} = {master_product_id}
                    ORDER BY
                        {MRP_TABLE.PRODUCT_ITEM}.{MRP_PRODUCT_ITEM.ID} ASC
                ''')
                slave_products = self._fetchallmap()
                if slave_products:
                    product.update({
                        'SLAVE_PRODUCTS_NAMES': '|'.join([f"{sp['SLAVE_PRODUCT_COUNT']}x - {sp['SLAVE_PRODUCT_NAME']}" for sp in slave_products]),
                        'SLAVE_PRODUCTS_SKUS': '|'.join([f"{sp['SLAVE_PRODUCT_COUNT']}x - {sp['SLAVE_PRODUCT_SKU']}" for sp in slave_products])
                    })
                    # UPDATE STOCK_QUANTITY, STOCK_MINIMUM
                    slave_product_id = slave_products[0]['SLAVE_PRODUCT_ID']
                    self._execute(f'''
                        SELECT 
                            CAST(SUM({MRP_TABLE.PRODUCT_STATUS}.{MRP_PRODUCT_STATUS.STOCK_QUANTITY}) AS INTEGER) AS STOCK_QUANTITY
                        FROM
                            {MRP_TABLE.PRODUCT_STATUS}
                        WHERE
                            {MRP_PRODUCT_STATUS.STOCK_NUMBER} IN {MRP_PRODUCT_STOCK_NUMBERS}
                            AND {MRP_TABLE.PRODUCT_STATUS}.{MRP_PRODUCT_STATUS.PRODUCT_ID} = {slave_product_id}
                        GROUP BY
                            {MRP_TABLE.PRODUCT_STATUS}.{MRP_PRODUCT_STATUS.PRODUCT_ID}
                    ''')
                    product.update(self._fetchonemap())
            products += products_chunk
        return products

    def get_products_states(self, mrp_products_ids=None):
        WHERE = f'''
            WHERE {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ID} IN ({', '.join(map(str, mrp_products_ids))})
        ''' if mrp_products_ids else ''
        query = f'''
            SELECT
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ID},
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.NAME},
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.CATEGORY_NUMBER},
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.METATAGS},
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.EAN},
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.SKU},
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ESHOP_FLAG},
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.WARRANTY},
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.STOCK_MINIMUM},
                HASH({MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ATTRIBUTES}),
                SUM({MRP_TABLE.PRODUCT_DETAIL}.{MRP_PRODUCT_DETAIL.UPDATE_COUNT}),
                SUM({MRP_TABLE.PRODUCT_STATUS}.{MRP_PRODUCT_STATUS.UPDATE_COUNT}),
                LIST({MRP_TABLE.PRODUCT_CATEGORY_EX}.{MRP_PRODUCT_CATEGORY_EX.CATEGORY_NUMBER})
            FROM
                {MRP_TABLE.PRODUCT}
                LEFT JOIN {MRP_TABLE.PRODUCT_DETAIL} ON ({MRP_TABLE.PRODUCT_DETAIL}.{MRP_PRODUCT_DETAIL.PRODUCT_ID} = {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ID})
                LEFT JOIN {MRP_TABLE.PRODUCT_STATUS} ON ({MRP_TABLE.PRODUCT_STATUS}.{MRP_PRODUCT_STATUS.PRODUCT_ID} = {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ID})
                LEFT JOIN {MRP_TABLE.PRODUCT_CATEGORY_EX} ON ({MRP_TABLE.PRODUCT_CATEGORY_EX}.{MRP_PRODUCT_CATEGORY_EX.PRODUCT_ID} = {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ID})
            { WHERE }
            GROUP BY
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ID},
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.NAME},
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.CATEGORY_NUMBER},
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.METATAGS},
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.EAN},
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.SKU},
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ESHOP_FLAG},
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.WARRANTY},
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.STOCK_MINIMUM},
                HASH({MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ATTRIBUTES})
            ORDER BY
                {MRP_TABLE.PRODUCT}.{MRP_PRODUCT.ID} ASC
        '''
        self._execute(query)
        products_states = self._fetchall()
        logger.debug('Hashing products states')
        started = perf_counter()
        results = [(ps[0], get_hash(ps[1:])) for ps in products_states]  # tuple(id, hash)
        logger.debug('Hashed in %fs', (perf_counter() - started))
        return results

    def set_product_attributes(self, mrp_product_id, mrp_attributes):
        mrp_attributes = TO_MRP_NEWLINES(str(mrp_attributes))  # BLOB
        query = f'''
            UPDATE {MRP_TABLE.PRODUCT} SET {MRP_PRODUCT.ATTRIBUTES} = {mrp_attributes} WHERE {MRP_PRODUCT.ID} = {mrp_product_id}
        '''
        self._execute(query)

    def set_product_description(self, mrp_product_id, mrp_description):
        mrp_description = TO_MRP_NEWLINES(str(mrp_description))  # BLOB
        query = f'''
            UPDATE OR INSERT INTO
                {MRP_TABLE.PRODUCT_DETAIL} ({MRP_PRODUCT_DETAIL.PRODUCT_ID}, {MRP_PRODUCT_DETAIL.DESCRIPTION})
            VALUES ({mrp_product_id}, {mrp_description})
        '''
        self._execute(query)

    def set_product_ean(self, mrp_product_id, mrp_ean):
        mrp_ean = str(mrp_ean)[:25]  # CHAR(25)
        query = f'''
            UPDATE {MRP_TABLE.PRODUCT} SET {MRP_PRODUCT.EAN} = '{mrp_ean}' WHERE {MRP_PRODUCT.ID} = {mrp_product_id}
        '''
        self._execute(query)

    def set_product_eshop_flag(self, mrp_product_id, mrp_eshop_flag):
        mrp_eshop_flag = str(mrp_eshop_flag)[:50]  # CHAR(50)
        query = f'''
            UPDATE {MRP_TABLE.PRODUCT} SET {MRP_PRODUCT.ESHOP_FLAG} = '{mrp_eshop_flag}' WHERE {MRP_PRODUCT.ID} = {mrp_product_id}
        '''
        self._execute(query)

    def set_product_eshop_info(self, mrp_product_id, mrp_eshop_info):
        mrp_eshop_info = str(mrp_eshop_info)[:50]  # CHAR(50)
        query = f'''
            UPDATE {MRP_TABLE.PRODUCT} SET {MRP_PRODUCT.ESHOP_INFO} = '{mrp_eshop_info}' WHERE {MRP_PRODUCT.ID} = {mrp_product_id}
        '''
        self._execute(query)

    def set_product_metatags(self, mrp_product_id, mrp_metatags):
        mrp_metatags = str(mrp_metatags)[:50]  # CHAR(50)
        query = f'''
            UPDATE {MRP_TABLE.PRODUCT} SET {MRP_PRODUCT.METATAGS} = '{mrp_metatags}' WHERE {MRP_PRODUCT.ID} = {mrp_product_id}
        '''
        self._execute(query)

    def set_product_name(self, mrp_product_id, mrp_name):
        mrp_name = str(mrp_name)[:64]  # CHAR(64)
        query = f'''
            UPDATE {MRP_TABLE.PRODUCT} SET {MRP_PRODUCT.NAME} = '{mrp_name}' WHERE {MRP_PRODUCT.ID} = {mrp_product_id}
        '''
        self._execute(query)

    def set_product_small_note(self, mrp_product_id, mrp_small_note):
        mrp_small_note = str(mrp_small_note)[:50]  # CHAR(50)
        query = f'''
            UPDATE {MRP_TABLE.PRODUCT} SET {MRP_PRODUCT.SMALL_NOTE} = '{mrp_small_note}' WHERE {MRP_PRODUCT.ID} = {mrp_product_id}
        '''
        self._execute(query)

    def set_product_sku(self, mrp_product_id, mrp_sku):
        mrp_sku = str(mrp_sku)[:64]  # CHAR(64)
        query = f'''
            UPDATE {MRP_TABLE.PRODUCT} SET {MRP_PRODUCT.SKU} = '{mrp_sku}' WHERE {MRP_PRODUCT.ID} = {mrp_product_id}
        '''
        self._execute(query)

    #
    # USERS
    #
    def add_user(self, mrp_name, mrp_address, mrp_city, mrp_zip, mrp_country, mrp_country_code, mrp_phone, mrp_email, mrp_individual,
                 mrp_company_name, mrp_company_id_number, mrp_company_tax_id, mrp_company_vat_id):
        if not mrp_company_id_number:  # auto-generate company_id_number
            self._execute("SELECT FIRST 1 CAST(SUBSTRING(ICO FROM 2) AS INTEGER) FROM ADRES WHERE ICO LIKE 'A0%' ORDER BY ICO DESC")
            mrp_company_id_number = f'A0{self._fetchone() + 1}'
        query = f'''
            UPDATE OR INSERT INTO {MRP_TABLE.USER} (
                {MRP_USER.ID},
                {MRP_USER.NAME},
                {MRP_USER.ADDRESS},
                {MRP_USER.CITY},
                {MRP_USER.ZIP},
                {MRP_USER.COUNTRY},
                {MRP_USER.COUNTRY_CODE},
                {MRP_USER.PHONE},
                {MRP_USER.EMAIL},
                {MRP_USER.INDIVIDUAL},
                {MRP_USER.COMPANY_NAME},
                {MRP_USER.COMPANY_ID_NUMBER},
                {MRP_USER.COMPANY_TAX_ID},
                {MRP_USER.COMPANY_VAT_ID},
                {MRP_USER.SMALL_NOTE}
            )
            VALUES (
                (SELECT MAX({MRP_USER.ID}) FROM {MRP_TABLE.USER}) + 1,
                '{mrp_name}',
                '{mrp_address}',
                '{mrp_city}',
                '{mrp_zip}',
                '{mrp_country}',
                '{mrp_country_code}',
                '{mrp_phone}',
                '{mrp_email}',
                '{mrp_individual}',
                '{mrp_company_name}',
                '{mrp_company_id_number}',
                '{mrp_company_tax_id}',
                '{mrp_company_vat_id}',
                ' - BENALEXPLUS INTRANET - '
            )
            MATCHING ({MRP_USER.COMPANY_ID_NUMBER})
            RETURNING
                {MRP_USER.ID}
        '''
        self._execute(query)
        return self._fetchone(), mrp_company_id_number  # mrp_user_id, mrp_company_id_number

    def get_users_states(self):
        query = f'''
            SELECT
                MAX({MRP_TABLE.USER}.{MRP_USER.ID}),
                REPLACE(TRIM({MRP_TABLE.USER}.{MRP_USER.COMPANY_ID_NUMBER}), ' ', ''),
                SUM({MRP_TABLE.USER}.{MRP_USER.UPDATE_COUNT})
            FROM
                {MRP_TABLE.USER}
            GROUP BY
                REPLACE(TRIM({MRP_TABLE.USER}.{MRP_USER.COMPANY_ID_NUMBER}), ' ', '')
        '''
        self._execute(query)
        users_states = self._fetchall()
        logger.debug('Hashing users states')
        started = perf_counter()
        results = [(us[0], get_hash(us[1:])) for us in users_states]  # tuple(id, hash)
        logger.debug('Hashed in %fs', (perf_counter() - started))
        return results

    def get_user_by_company_id_number(self, mrp_company_id_number):
        query = f'''
            SELECT
                MAX({MRP_TABLE.USER}.{MRP_USER.ID})
            FROM
                {MRP_TABLE.USER}
            WHERE
                REPLACE(TRIM({MRP_TABLE.USER}.{MRP_USER.COMPANY_ID_NUMBER}), ' ', '') = '{mrp_company_id_number}'
            GROUP BY
                REPLACE(TRIM({MRP_TABLE.USER}.{MRP_USER.COMPANY_ID_NUMBER}), ' ', '')
        '''
        self._execute(query)
        mrp_user_id = self._fetchone()
        if not mrp_user_id: return None  # ICO does not exist
        return self.get_user_by_id(mrp_user_id)

    def get_user_by_id(self, mrp_user_id):
        user = self.get_users_by_ids([mrp_user_id])
        return user[0] if user else None

    def get_users_by_ids(self, mrp_users_ids):
        users = []
        mrp_users_ids_chunks = create_chunks(mrp_users_ids, 250)  # firebird limit for IN is 1500
        for mrp_users_ids_chunk in mrp_users_ids_chunks:
            query = f'''
                SELECT
                    {MRP_TABLE.USER}.{MRP_USER.ID} AS ID,
                    TRIM({MRP_TABLE.USER}.{MRP_USER.NAME}) AS NAME,
                    TRIM({MRP_TABLE.USER}.{MRP_USER.ADDRESS}) AS ADDRESS,
                    TRIM({MRP_TABLE.USER}.{MRP_USER.ZIP}) AS ZIP,
                    TRIM({MRP_TABLE.USER}.{MRP_USER.CITY}) AS CITY,
                    TRIM({MRP_TABLE.USER}.{MRP_USER.COUNTRY}) AS COUNTRY,
                    COALESCE(TRIM({MRP_TABLE.USER}.{MRP_USER.COUNTRY_CODE}), '') AS COUNTRY_CODE,
                    TRIM({MRP_TABLE.USER}.{MRP_USER.EMAIL}) AS EMAIL,
                    TRIM({MRP_TABLE.USER}.{MRP_USER.PHONE}) AS PHONE,
                    TRIM({MRP_TABLE.USER}.{MRP_USER.PHONE2}) AS PHONE2,
                    TRIM({MRP_TABLE.USER}.{MRP_USER.PHONE3}) AS PHONE3,
                    CASE WHEN {MRP_TABLE.USER}.{MRP_USER.INDIVIDUAL} = 'F' THEN 1 ELSE 0 END AS IS_COMPANY,
                    TRIM({MRP_TABLE.USER}.{MRP_USER.COMPANY_NAME}) AS COMPANY_NAME,
                    REPLACE(TRIM({MRP_TABLE.USER}.{MRP_USER.COMPANY_ID_NUMBER}), ' ', '') AS COMPANY_ID_NUMBER,
                    REPLACE(TRIM({MRP_TABLE.USER}.{MRP_USER.COMPANY_TAX_ID}), ' ', '') AS COMPANY_TAX_ID,
                    REPLACE(TRIM({MRP_TABLE.USER}.{MRP_USER.COMPANY_VAT_ID}), ' ', '') AS COMPANY_VAT_ID,
                    CAST(COALESCE({MRP_TABLE.USER}.{MRP_USER.DUE_DATE_DAYS}, 14) AS INTEGER) AS DUE_DATE_DAYS,
                    CAST(COALESCE({MRP_TABLE.USER}.{MRP_USER.PRICE_GROUP}, 1) AS INTEGER) AS PRICE_GROUP,
                    {MRP_TABLE.USER}.{MRP_USER.ADDED} AS ADDED,
                    COALESCE(TRIM({MRP_TABLE.USER}.{MRP_USER.NOTE}), '') AS NOTE
                FROM
                    {MRP_TABLE.USER}
                WHERE
                    {MRP_TABLE.USER}.{MRP_USER.ID} IN ({', '.join(map(str, mrp_users_ids_chunk))})
                ORDER BY
                    {MRP_TABLE.USER}.{MRP_USER.ID} ASC
                    '''
            self._execute(query)
            users += self._fetchallmap()
        return users

    def get_user_finance_stats(self, mrp_company_id_number):
        query = f'''
            SELECT
                CAST({MRP_TABLE.STOCK_MOVEMENT}.{MRP_STOCK_MOVEMENT.MOVEMENT_NUMBER} AS INTEGER) AS MOVEMENT_NUMBER,
                TRIM(',' FROM REPLACE(LIST(TRIM({MRP_TABLE.STOCK_MOVEMENT}.{MRP_STOCK_MOVEMENT.VARIABLE_SYMBOL})), ',,', ',')) AS VARIABLE_SYMBOLS,
                COALESCE(SUM({MRP_TABLE.STOCK_MOVEMENT}.{MRP_STOCK_MOVEMENT.TOTAL}), 0) AS TOTAL,
                CASE WHEN {MRP_TABLE.STOCK_MOVEMENT}.{MRP_STOCK_MOVEMENT.IS_EXPENSE} = 'T' THEN 1 ELSE 0 END AS IS_EXPENSE,
                CASE WHEN {MRP_TABLE.STOCK_MOVEMENT}.{MRP_STOCK_MOVEMENT.IS_EXPENSE} = 'F' THEN 1 ELSE 0 END AS IS_INCOME
            FROM
                {MRP_TABLE.STOCK_MOVEMENT}
            WHERE
                REPLACE(TRIM({MRP_TABLE.STOCK_MOVEMENT}.{MRP_STOCK_MOVEMENT.COMPANY_ID_NUMBER}), ' ', '') = '{mrp_company_id_number}'
                AND {MRP_TABLE.STOCK_MOVEMENT}.{MRP_STOCK_MOVEMENT.MOVEMENT_NUMBER} IN {MRP_STOCK_MOVEMENT_NUMBERS}
            GROUP BY
                REPLACE(TRIM({MRP_TABLE.STOCK_MOVEMENT}.{MRP_STOCK_MOVEMENT.COMPANY_ID_NUMBER}), ' ', ''),
                {MRP_TABLE.STOCK_MOVEMENT}.{MRP_STOCK_MOVEMENT.MOVEMENT_NUMBER},
                {MRP_TABLE.STOCK_MOVEMENT}.{MRP_STOCK_MOVEMENT.IS_EXPENSE}
        '''
        self._execute(query)
        stock_movements = self._fetchallmap()
        income_stock_movements = [sm for sm in stock_movements if sm['IS_INCOME']]
        income_total_amount = sum([sm['TOTAL'] for sm in income_stock_movements])
        income_missing_amount = 0
        for stock_movement in income_stock_movements:
            if stock_movement['MOVEMENT_NUMBER'] == 2 and stock_movement['VARIABLE_SYMBOLS']:  # invoices
                income_missing_amount = sum([i['MISSING'] for i in self.get_invoices_by_variable_symbols(stock_movement['VARIABLE_SYMBOLS'].split(','))])
        expense_stock_movements = [sm for sm in stock_movements if sm['IS_EXPENSE']]
        expense_total_amount = sum([sm['TOTAL'] for sm in expense_stock_movements])
        expense_missing_amount = 0  # TODO?
        return {
            'YEAR': self.mrp_year,
            'INCOME_TOTAL_AMOUNT': income_total_amount,
            'INCOME_MISSING_AMOUNT': income_missing_amount,
            'EXPENSE_TOTAL_AMOUNT': expense_total_amount,
            'EXPENSE_MISSING_AMOUNT': expense_missing_amount
        }