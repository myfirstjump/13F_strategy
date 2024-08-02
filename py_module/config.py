import os
import logging

class Configuration(object):

    _instance = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Configuration, cls).__new__(cls)
            cls._instance.initialize_logger()

            cls.LOCAL_FLAG = False
            '''
            Database
            '''
            # cls.hedge_fund_portfolio_table = '[US_DB].[dbo].[HEDGE_FUND_PORTFOLIO]'
            # cls.holdings_data_table = '[US_DB].[dbo].[HOLDINGS_DATA]'
            cls.hedge_fund_portfolio_table = '[US_DB].[dbo].[HEDGE_FUND_PORTFOLIO_FILTERED]'
            cls.holdings_data_table = '[US_DB].[dbo].[HOLDINGS_DATA_FILTERED]'
            cls.us_stock_info_table = '[US_DB].[dbo].[USStockInfo]'
            cls.us_stock_price_table = '[US_DB].[dbo].[USStockPrice]'
            cls.us_stock_gics_table = '[US_DB].[dbo].[Company_GICS]'
            cls.tw_stock_price_table = '[STOCK_SKILL_DB].[dbo].[TW_STOCK_PRICE_Daily]'
            cls.customized_fund_portfolio_table = '[US_DB].[dbo].[CUSTOMIZED_HEDGE_FUND_PORTFOLIO]'
            cls.customized_holdings_data_table = '[US_DB].[dbo].[CUSTOMIZED_HOLDINGS_DATA]'

            '''
            Crawler
            '''
            cls.working_dir = os.getcwd() #返還main.py檔案資料夾
            cls.assets_hedge_fund_data = os.path.join(cls.working_dir, "assets\\hedge_fund_data") if os.name == 'nt' else os.path.join(cls.working_dir, "assets/hedge_fund_data")
            cls.assets_holdings_data = os.path.join(cls.working_dir, "assets\\holdings_data") if os.name == 'nt' else os.path.join(cls.working_dir, "assets/holdings_data")
            cls.backtest_summary = os.path.join(cls.working_dir, "assets\\backtest_summary") if os.name == 'nt' else os.path.join(cls.working_dir, "assets/backtest_summary")
            cls.reference_folder = os.path.join(cls.working_dir, "assets\\reference") if os.name == 'nt' else os.path.join(cls.working_dir, "assets/reference")
            cls.hedge_fund_urls = {
                # 'SIR Capital Management':'https://13f.info/manager/0001434997-sir-capital-management-l-p', 
                # 'Robotti Robert':'https://13f.info/manager/0001105838-robotti-robert', 
                # 'Encompass Capital Advisors':'https://13f.info/manager/0001541901-encompass-capital-advisors-llc', 
                # 'Elm Ridge Management':'https://13f.info/manager/0001483276-elm-ridge-management-llc', 
                # 'RR Advisors':'https://13f.info/manager/0001322613-rr-advisors-llc', 
                # 'Peconic Partners':'https://13f.info/manager/0001050464-peconic-partners-llc',
                # 'Fairholme Capital Management':'https://13f.info/manager/0001056831-fairholme-capital-management-llc', 
                # 'Horizon Kinetics Asset Management':'https://13f.info/manager/0001056823-horizon-kinetics-asset-management-llc', 
                'Scion Asset Management':'https://13f.info/manager/0001649339-scion-asset-management-llc', 
                # 'Goldentree Asset Management':'https://13f.info/manager/0001278951-goldentree-asset-management-lp', 
                # 'Lodge Hill Capital':'https://13f.info/manager/0001598245-lodge-hill-capital-llc',
                # 'Mangrove Partners':'https://13f.info/manager/0001535392-mangrove-partners', 
                'Stilwell Value':'https://13f.info/manager/0001397076-stilwell-value-llc', 
                # 'Greenlight Capital':'https://13f.info/manager/0001079114-greenlight-capital-inc', 
                # 'Chou Associates Management':'https://13f.info/manager/0001389403-chou-associates-management-inc', 
                # 'Steinberg Asset Management':'https://13f.info/manager/0001169883-steinberg-asset-management-llc', 
                # 'Donald Smith':'https://13f.info/manager/0000814375-donald-smith-co-inc',
                # 'Fine Capital Partners':'https://13f.info/manager/0001339161-fine-capital-partners-l-p', 
                # 'Contrarian Capital Management':'https://13f.info/manager/0001050417-contrarian-capital-management-l-l-c', 
                # 'Masters Capital Management':'https://13f.info/manager/0001104186-masters-capital-management-llc', 
                'Yacktman Asset Management':'https://13f.info/manager/0000905567-yacktman-asset-management-lp', 
                # # 'Millennium Management':'https://13f.info/manager/0001273087-millennium-management-llc', 
                # 'Point72 Asset Management':'https://13f.info/manager/0001603466-point72-asset-management-l-p', 
                # 'Appaloosa':'https://13f.info/manager/0001656456-appaloosa-lp', 
                'Pershing Square Capital Management':'https://13f.info/manager/0001336528-pershing-square-capital-management-l-p', 
                # 'Berkshire Hathaway':'https://13f.info/manager/0001067983-berkshire-hathaway-inc', 
                # # 'Renaissance Technologies':'https://13f.info/manager/0001037389-renaissance-technologies-llc', 
                # # 'Citadel Advisors':'https://13f.info/manager/0001423053-citadel-advisors-llc',
                # 'Duquesne Family Office':'https://13f.info/manager/0001536411-duquesne-family-office-llc', 
                # 'Dalal Street Holdings':'https://13f.info/manager/0001549575-dalal-street-llc', 
                'Altarock Partners':'https://13f.info/manager/0001631014-altarock-partners-llc', 
                'Brave Warrior Advisors':'https://13f.info/manager/0001553733-brave-warrior-advisors-llc',
            }

            cls.popular_13F_manager_urls = {
                'Abdiel Capital Advisors':'https://13f.info/manager/0001578684-abdiel-capital-advisors-lp',
                'AKRE CAPITAL MANAGEMENT':'https://13f.info/manager/0001112520-akre-capital-management-llc',
                'Altimeter Capital Management':'https://13f.info/manager/0001541617-altimeter-capital-management-lp',
                'APPALOOSA':'https://13f.info/manager/0001656456-appaloosa-lp',
                'AQR CAPITAL MANAGEMENT':'https://13f.info/manager/0001167557-aqr-capital-management-llc',
                'ARK Investment Management':'https://13f.info/manager/0001697748-ark-investment-management-llc',
                'Atreides Management':'https://13f.info/manager/0001777813-atreides-management-lp',
                'BAILLIE GIFFORD & CO':'https://13f.info/manager/0001088875-baillie-gifford-co',
                'Balyasny Asset Management':'https://13f.info/manager/0001218710-balyasny-asset-management-l-p',
                'BAUPOST GROUP':'https://13f.info/manager/0001061768-baupost-group-llc-ma',
                'Berkshire Hathaway':'https://13f.info/manager/0001067983-berkshire-hathaway-inc',
                'BILL & MELINDA GATES FOUNDATION TRUST':'https://13f.info/manager/0001166559-bill-melinda-gates-foundation-trust',
                'BlackRock':'https://13f.info/manager/0001364742-blackrock-inc',
                'Blackstone':'https://13f.info/manager/0001393818-blackstone-inc',
                'BNP PARIBAS ASSET MANAGEMENT':'https://13f.info/manager/0001520354-bnp-paribas-asset-management-holding-s-a',
                'Bridgewater Associates':'https://13f.info/manager/0001350694-bridgewater-associates-lp',
                'Carlyle Group':'https://13f.info/manager/0001527166-carlyle-group-inc',
                'Chanos & Co':'https://13f.info/manager/0001446440-chanos-co-lp',
                'CITADEL ADVISORS':'https://13f.info/manager/0001423053-citadel-advisors-llc',
                'COATUE MANAGEMENT':'https://13f.info/manager/0001135730-coatue-management-llc',
                'COOPERMAN LEON G':'https://13f.info/manager/0000898382-cooperman-leon-g',
                'D1 Capital Partners':'https://13f.info/manager/0001747057-d1-capital-partners-l-p',
                'DAILY JOURNAL':'https://13f.info/manager/0000783412-daily-journal-corp',
                'D. E. Shaw & Co':'https://13f.info/manager/0001009207-d-e-shaw-co-inc',
                'Dragoneer Investment Group':'https://13f.info/manager/0001602189-dragoneer-investment-group-llc',
                'Duquesne Family Office':'https://13f.info/manager/0001536411-duquesne-family-office-llc',
                'Elliott Investment Management':'https://13f.info/manager/0001791786-elliott-investment-management-l-p',
                'Ensign Peak Advisors':'https://13f.info/manager/0001454984-ensign-peak-advisors-inc',
                'FARALLON CAPITAL MANAGEMENT':'https://13f.info/manager/0000909661-farallon-capital-management-llc',
                'Fundsmith':'https://13f.info/manager/0001569205-fundsmith-llp',
                'GEODE CAPITAL MANAGEMENT':'https://13f.info/manager/0001214717-geode-capital-management-llc',
                'GLENVIEW CAPITAL MANAGEMENT':'https://13f.info/manager/0001138995-glenview-capital-management-llc',
                'GOLDMAN SACHS GROUP':'https://13f.info/manager/0000886982-goldman-sachs-group-inc',
                'GREENLIGHT CAPITAL':'https://13f.info/manager/0001079114-greenlight-capital-inc',
                'HAYMAN CAPITAL MANAGEMENT':'https://13f.info/manager/0001420192-hayman-capital-management-l-p',
                'HHLR ADVISORS':'https://13f.info/manager/0001762304-hhlr-advisors-ltd',
                'Himalaya Capital Management':'https://13f.info/manager/0001709323-himalaya-capital-management-llc',
                'ICAHN CARL C':'https://13f.info/manager/0000921669-icahn-carl-c',
                'JANA PARTNERS':'https://13f.info/manager/0001159159-jana-partners-llc',
                'KING STREET CAPITAL MANAGEMENT':'https://13f.info/manager/0001218199-king-street-capital-management-l-p',
                'LONE PINE CAPITAL':'https://13f.info/manager/0001061165-lone-pine-capital-llc',
                'Marathon Partners Equity Management':'https://13f.info/manager/0001353311-marathon-partners-equity-management-llc',
                'Matrix Capital Management Company':'https://13f.info/manager/0001410830-matrix-capital-management-company-lp',
                'Melvin Capital Management':'https://13f.info/manager/0001628110-melvin-capital-management-lp',
                'MILLENNIUM MANAGEMENT':'https://13f.info/manager/0001273087-millennium-management-llc',
                'MILLER VALUE PARTNERS':'https://13f.info/manager/0001135778-miller-value-partners-llc',
                'MOORE CAPITAL MANAGEMENT':'https://13f.info/manager/0001448574-moore-capital-management-lp',
                'OAKTREE CAPITAL MANAGEMENT':'https://13f.info/manager/0000949509-oaktree-capital-management-lp',
                'PAULSON & CO':'https://13f.info/manager/0001035674-paulson-co-inc',
                'Pershing Square Capital Management':'https://13f.info/manager/0001336528-pershing-square-capital-management-l-p',
                'Point72 Asset Management':'https://13f.info/manager/0001603466-point72-asset-management-l-p',
                'RA CAPITAL MANAGEMENT':'https://13f.info/manager/0001346824-ra-capital-management-l-p',
                'RENAISSANCE TECHNOLOGIES':'https://13f.info/manager/0001037389-renaissance-technologies-llc',
                'Rokos Capital Management':'https://13f.info/manager/0001666335-rokos-capital-management-llp',
                'Saba Capital Management':'https://13f.info/manager/0001510281-saba-capital-management-l-p',
                'Saber Capital Managment':'https://13f.info/manager/0001911378-saber-capital-managment-llc',
                'SANDS CAPITAL MANAGEMENT':'https://13f.info/manager/0001020066-sands-capital-management-llc',
                'Scion Asset Management':'https://13f.info/manager/0001649339-scion-asset-management-llc',
                'Senvest Management':'https://13f.info/manager/0001328785-senvest-management-llc',
                'ShawSpring Partners':'https://13f.info/manager/0001766908-shawspring-partners-llc',
                'SOROS FUND MANAGEMENT':'https://13f.info/manager/0001029160-soros-fund-management-llc',
                'SPRUCE HOUSE INVESTMENT MANAGEMENT':'https://13f.info/manager/0001543170-spruce-house-investment-management-llc',
                'STATE STREET':'https://13f.info/manager/0000093751-state-street-corp',
                'Stockbridge Partners':'https://13f.info/manager/0001505183-stockbridge-partners-llc',
                'SUSQUEHANNA INTERNATIONAL GROUP':'https://13f.info/manager/0001446194-susquehanna-international-group-llp',
                'TCI FUND MANAGEMENT':'https://13f.info/manager/0001647251-tci-fund-management-ltd',
                'Thiel Macro':'https://13f.info/manager/0001562087-thiel-macro-llc',
                'Third Point':'https://13f.info/manager/0001040273-third-point-llc',
                'TIGER GLOBAL MANAGEMENT':'https://13f.info/manager/0001167483-tiger-global-management-llc',
                'TPG Group Holdings (SBS) Advisors':'https://13f.info/manager/0001495741-tpg-group-holdings-sbs-advisors-inc',
                'Tudor Investment':'https://13f.info/manager/0000923093-tudor-investment-corp-et-al',
                'TWO SIGMA INVESTMENTS':'https://13f.info/manager/0001179392-two-sigma-investments-lp',
                'VANGUARD GROUP':'https://13f.info/manager/0000102909-vanguard-group-inc',
                'VIKING GLOBAL INVESTORS':'https://13f.info/manager/0001103804-viking-global-investors-lp',
                'Virtu Financial':'https://13f.info/manager/0001533964-virtu-financial-llc',
                'Whale Rock Capital Management':'https://13f.info/manager/0001387322-whale-rock-capital-management-llc',
                'XTX Topco':'https://13f.info/manager/0001828301-xtx-topco-ltd',
                'XXEC':'https://13f.info/manager/0001828822-xxec-inc',
                'YALE UNIVERSITY':'https://13f.info/manager/0000938582-yale-university',
                'York Capital Management Global Advisors':'https://13f.info/manager/0001480532-york-capital-management-global-advisors-llc',
            }

            ### 
            cls.dash_port = '8050'

            '''
            Customized Hedge Components
            '''
            cls.customize_enter_date = '2013-05-30'
            # cls.target_hedge_funds = [
            #     'Scion Asset Management',
            #     'Peconic Partners',
            #     'Altarock Partners',
            #     'Elm Ridge Management',
            #     'Goldentree Asset Management',
            #     'Point72 Asset Management',
            #     'SIR Capital Management',
            #     'Duquesne Family Office',
            #     'Lodge Hill Capital',
            #     'Brave Warrior Advisors',
            #     'Appaloosa',
            #     'Yacktman Asset Management',
            #     'Robotti Robert',
            #     'Horizon Kinetics Asset Management',
            #     'Stilwell Value',
            #     'Fine Capital Partners',
            #     'Dalal Street Holdings',
            #     'Berkshire Hathaway',
            # ]

            cls.target_hedge_funds_dict = {
                'XIRR_output_filter': [
                                    'Scion Asset Management',
                                    'Peconic Partners',
                                    'Altarock Partners',
                                    'Elm Ridge Management',
                                    'Goldentree Asset Management',
                                    'Point72 Asset Management',
                                    'SIR Capital Management',
                                    'Duquesne Family Office',
                                    'Lodge Hill Capital',
                                    'Brave Warrior Advisors',
                                    'Appaloosa',
                                    'Yacktman Asset Management',
                                    'Robotti Robert',
                                    'Horizon Kinetics Asset Management',
                                    'Stilwell Value',
                                    'Fine Capital Partners',
                                    'Dalal Street Holdings',
                                    'Berkshire Hathaway',
                ],
                'sharpe_output_filter':[
                                    'Yacktman Asset Management',
                                    'Scion Asset Management',
                                    'Altarock Partners',
                                    'Brave Warrior Advisors',
                                    'Pershing Square Capital Management',
                                    'Stilwell Value',
                ],
                'sharpe_v2':[ # 0725依據一萬家13F基金篩選過後的清單
                                    'Barton Investment Management',
                                    'AMERICAN FINANCIAL GROUP INC 401(K) RETIREMENT & SAVINGS PLAN',
                                    # 'Strategic Point Investment Advisors, LLC',
                                    'BRISTOL JOHN W & CO INC /NY/',
                                    'ACR Alpine Capital Research, LLC',
                                    'PEAVINE CAPITAL, LLC',
                                    'JLB & ASSOCIATES INC',
                                    'Yacktman Asset Management',
                                    'BEDRIJFSTAKPENSIOENFONDS VOOR DE MEDIA PNO',
                                    'Saratoga Research & Investment Management',
                                    'Cohen Klingenstein LLC',
                                    'RWWM, Inc.',
                                    'JENSEN INVESTMENT MANAGEMENT INC',
                                    'YCG, LLC',
                                    'H PARTNERS MANAGEMENT, LLC',
                                    'NEW YORK STATE TEACHERS RETIREMENT SYSTEM',
                                    'SATURNA CAPITAL CORP',
                                    'Sanders Capital, LLC',
                                    'Longview Partners (Guernsey) LTD',
                                    'WEATHERLY ASSET MANAGEMENT L. P.',
                                    'PENSIOENFONDS RAIL & OV',
                                    'Dixon Mitchell Investment Counsel Inc.',
                                    'Van Berkom & Associates Inc.',
                                    # 'Main Management LLC',
                                    'Pacifica Capital Investments, LLC',
                                    'HARTFORD INVESTMENT MANAGEMENT CO',
                                    'Burgundy Asset Management Ltd.',
                                    'FIDUCIARY MANAGEMENT INC /WI/',
                                    # 'Evensky & Katz LLC'
                ],
                'sharpe_v3':[ # 0728 更新分析標的基金，有些在hgdmj那邊跑得績效較好是因為ETF表現，故應獨立跑組合，確認非ETF個股之績效
                    'Barton Investment Management',
                    'AMERICAN FINANCIAL GROUP INC 401(K) RETIREMENT & SAVINGS PLAN',
                    # 'Strategic Point Investment Advisors, LLC',
                    'BRISTOL JOHN W & CO INC /NY/',
                    'ACR Alpine Capital Research, LLC',
                    'PEAVINE CAPITAL, LLC',
                    'JLB & ASSOCIATES INC',
                    'Yacktman Asset Management',
                    'BEDRIJFSTAKPENSIOENFONDS VOOR DE MEDIA PNO',
                    'Saratoga Research & Investment Management',
                    'Cohen Klingenstein LLC',
                    'RWWM, Inc.',
                    'JENSEN INVESTMENT MANAGEMENT INC',
                    'YCG, LLC',
                    'H PARTNERS MANAGEMENT, LLC',
                    'NEW YORK STATE TEACHERS RETIREMENT SYSTEM',
                    'SATURNA CAPITAL CORP',
                    'Sanders Capital, LLC',
                    'Longview Partners (Guernsey) LTD',
                    'WEATHERLY ASSET MANAGEMENT L. P.',
                    'PENSIOENFONDS RAIL & OV',
                    'Dixon Mitchell Investment Counsel Inc.',
                    'Van Berkom & Associates Inc.',
                    # 'Main Management LLC',
                    'Pacifica Capital Investments, LLC',
                    'HARTFORD INVESTMENT MANAGEMENT CO',
                    'Burgundy Asset Management Ltd.',
                    'FIDUCIARY MANAGEMENT INC /WI/',
                    # 'Evensky & Katz LLC',
                    'Scion Asset Management',
                    'Altarock Partners',
                    'Brave Warrior Advisors',
                    'Pershing Square Capital Management',
                    'Stilwell Value'
                ]
            }

            cls.industry_top_selection = 3
            cls.company_top_selection = 3
            cls.enter_cost = 1000000
            cls.upper_price_limit = 1000

            cls.gics_dict = {
                '10': 'Energy',
                '15': 'Materials',
                '20': 'Industrials',
                '25': 'Consumer Discretionary',
                '30': 'Consumer Staples',
                '35': 'Health Care',
                '40': 'Financials',
                '45': 'Information Technology',
                '50': 'Communication Services',
                '55': 'Utilities',
                '60': 'Real Estate',
            }


        return cls._instance
    
    def initialize_logger(self):
        self.working_dir = os.getcwd() #返還main.py檔案資料夾
        log_dir = os.path.join(self.working_dir, "assets\\log") if os.name == 'nt' else os.path.join(self.working_dir, "assets/log")
        self.logger = logging.getLogger(__name__)
        handler_1 = logging.StreamHandler()
        handler_2 = logging.FileHandler(filename=os.path.join(log_dir, 'log.txt'))

        self.logger.setLevel(logging.DEBUG)
        handler_1.setLevel(logging.WARNING) # DEBUG > INFO > WARNING > ERROR > CRITICAL
        handler_2.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler_1.setFormatter(formatter)
        handler_2.setFormatter(formatter)
        self.logger.addHandler(handler_1)
        self.logger.addHandler(handler_2)	


    # def __init__(self):

    #     self.LOCAL_FLAG = True
    #     '''
    #     Crawler
    #     '''
    #     self.working_dir = os.getcwd() #返還main.py檔案資料夾
    #     self.assets_hedge_fund_data = os.path.join(self.working_dir, "assets\\hedge_fund_data") if os.name == 'nt' else os.path.join(self.working_dir, "assets/hedge_fund_data")
    #     self.assets_holdings_data = os.path.join(self.working_dir, "assets\\holdings_data") if os.name == 'nt' else os.path.join(self.working_dir, "assets/holdings_data")
    #     self.backtest_summary = os.path.join(self.working_dir, "assets\\backtest_summary") if os.name == 'nt' else os.path.join(self.working_dir, "assets/backtest_summary")
    #     self.reference_folder = os.path.join(self.working_dir, "assets\\reference") if os.name == 'nt' else os.path.join(self.working_dir, "assets/reference")
    #     print("__init__")
    #     self.hedge_fund_urls = {
    #         'SIR Capital Management':'https://13f.info/manager/0001434997-sir-capital-management-l-p', 
    #         'Robotti Robert':'https://13f.info/manager/0001105838-robotti-robert', 
    #         # 'Encompass Capital Advisors':'https://13f.info/manager/0001541901-encompass-capital-advisors-llc', 
    #         # 'Elm Ridge Management':'https://13f.info/manager/0001483276-elm-ridge-management-llc', 
    #         # 'RR Advisors':'https://13f.info/manager/0001322613-rr-advisors-llc', 
    #         # 'Peconic Partners':'https://13f.info/manager/0001050464-peconic-partners-llc',
    #         # 'Fairholme Capital Management':'https://13f.info/manager/0001056831-fairholme-capital-management-llc', 
    #         # 'Horizon Kinetics Asset Management':'https://13f.info/manager/0001056823-horizon-kinetics-asset-management-llc', 
    #         # 'Scion Asset Management':'https://13f.info/manager/0001649339-scion-asset-management-llc', 
    #         # 'Goldentree Asset Management':'https://13f.info/manager/0001278951-goldentree-asset-management-lp', 
    #         # 'Lodge Hill Capital':'https://13f.info/manager/0001598245-lodge-hill-capital-llc',
    #         # 'Mangrove Partners':'https://13f.info/manager/0001535392-mangrove-partners', 
    #         # 'Stilwell Value':'https://13f.info/manager/0001397076-stilwell-value-llc', 
    #         # 'Greenlight Capital':'https://13f.info/manager/0001079114-greenlight-capital-inc', 
    #         # 'Chou Associates Management':'https://13f.info/manager/0001389403-chou-associates-management-inc', 
    #         # 'Steinberg Asset Management':'https://13f.info/manager/0001169883-steinberg-asset-management-llc', 
    #         # 'Donald Smith':'https://13f.info/manager/0000814375-donald-smith-co-inc',
    #         # 'Fine Capital Partners':'https://13f.info/manager/0001339161-fine-capital-partners-l-p', 
    #         # 'Contrarian Capital Management':'https://13f.info/manager/0001050417-contrarian-capital-management-l-l-c', 
    #         # 'Masters Capital Management':'https://13f.info/manager/0001104186-masters-capital-management-llc', 
    #         # 'Yacktman Asset Management':'https://13f.info/manager/0000905567-yacktman-asset-management-lp', 
    #         # # 'Millennium Management':'https://13f.info/manager/0001273087-millennium-management-llc', 
    #         # 'Point72 Asset Management':'https://13f.info/manager/0001603466-point72-asset-management-l-p', 
    #         # 'Appaloosa':'https://13f.info/manager/0001656456-appaloosa-lp', 
    #         # 'Pershing Square Capital Management':'https://13f.info/manager/0001336528-pershing-square-capital-management-l-p', 
    #         # 'Berkshire Hathaway':'https://13f.info/manager/0001067983-berkshire-hathaway-inc', 
    #         # # 'Renaissance Technologies':'https://13f.info/manager/0001037389-renaissance-technologies-llc', 
    #         # # 'Citadel Advisors':'https://13f.info/manager/0001423053-citadel-advisors-llc',
    #         # 'Duquesne Family Office':'https://13f.info/manager/0001536411-duquesne-family-office-llc', 
    #         # 'Dalal Street Holdings':'https://13f.info/manager/0001549575-dalal-street-llc', 
    #         # 'Altarock Partners':'https://13f.info/manager/0001631014-altarock-partners-llc', 
    #         # 'Brave Warrior Advisors':'https://13f.info/manager/0001553733-brave-warrior-advisors-llc',
    #     }

    #     ### 
    #     self.dash_port = '8050'

    #     '''
    #     Customized Hedge Components
    #     '''
    #     self.customize_enter_date = '2019-02-15'
    #     self.target_hedge_funds = [
    #         'Scion Asset Management',
    #         'Peconic Partners',
    #         'Altarock Partners',
    #         'Elm Ridge Management',
    #         'Goldentree Asset Management',
    #         'Point72 Asset Management',
    #         'SIR Capital Management',
    #         'Duquesne Family Office',
    #         'Lodge Hill Capital',
    #         'Brave Warrior Advisors',
    #         'Appaloosa',
    #         'Yacktman Asset Management',
    #         'Robotti Robert',
    #         'Horizon Kinetics Asset Management',
    #         'Stilwell Value',
    #         'Fine Capital Partners',
    #         'Dalal Street Holdings',
    #         'Berkshire Hathaway',
    #     ]

    #     self.industry_top_selection = 3
    #     self.company_top_selection = 3
    #     self.enter_cost = 1000000

    #     self.gics_dict = {
    #         '10': 'Energy',
    #         '15': 'Materials',
    #         '20': 'Industrials',
    #         '25': 'Consumer Discretionary',
    #         '30': 'Consumer Staples',
    #         '35': 'Health Care',
    #         '40': 'Financials',
    #         '45': 'Information Technology',
    #         '50': 'Communication Services',
    #         '55': 'Utilities',
    #         '60': 'Real Estate',
    #     }
	



	