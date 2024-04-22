import os
import logging

class Configuration(object):

    _instance = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Configuration, cls).__new__(cls)
            cls._instance.initialize_logger()

            cls.LOCAL_FLAG = True
            '''
            Crawler
            '''
            cls.working_dir = os.getcwd() #返還main.py檔案資料夾
            cls.assets_hedge_fund_data = os.path.join(cls.working_dir, "assets\\hedge_fund_data") if os.name == 'nt' else os.path.join(cls.working_dir, "assets/hedge_fund_data")
            cls.assets_holdings_data = os.path.join(cls.working_dir, "assets\\holdings_data") if os.name == 'nt' else os.path.join(cls.working_dir, "assets/holdings_data")
            cls.backtest_summary = os.path.join(cls.working_dir, "assets\\backtest_summary") if os.name == 'nt' else os.path.join(cls.working_dir, "assets/backtest_summary")
            cls.reference_folder = os.path.join(cls.working_dir, "assets\\reference") if os.name == 'nt' else os.path.join(cls.working_dir, "assets/reference")
            cls.hedge_fund_urls = {
                'SIR Capital Management':'https://13f.info/manager/0001434997-sir-capital-management-l-p', 
                'Robotti Robert':'https://13f.info/manager/0001105838-robotti-robert', 
                'Encompass Capital Advisors':'https://13f.info/manager/0001541901-encompass-capital-advisors-llc', 
                'Elm Ridge Management':'https://13f.info/manager/0001483276-elm-ridge-management-llc', 
                'RR Advisors':'https://13f.info/manager/0001322613-rr-advisors-llc', 
                'Peconic Partners':'https://13f.info/manager/0001050464-peconic-partners-llc',
                'Fairholme Capital Management':'https://13f.info/manager/0001056831-fairholme-capital-management-llc', 
                'Horizon Kinetics Asset Management':'https://13f.info/manager/0001056823-horizon-kinetics-asset-management-llc', 
                'Scion Asset Management':'https://13f.info/manager/0001649339-scion-asset-management-llc', 
                'Goldentree Asset Management':'https://13f.info/manager/0001278951-goldentree-asset-management-lp', 
                'Lodge Hill Capital':'https://13f.info/manager/0001598245-lodge-hill-capital-llc',
                'Mangrove Partners':'https://13f.info/manager/0001535392-mangrove-partners', 
                'Stilwell Value':'https://13f.info/manager/0001397076-stilwell-value-llc', 
                'Greenlight Capital':'https://13f.info/manager/0001079114-greenlight-capital-inc', 
                'Chou Associates Management':'https://13f.info/manager/0001389403-chou-associates-management-inc', 
                'Steinberg Asset Management':'https://13f.info/manager/0001169883-steinberg-asset-management-llc', 
                'Donald Smith':'https://13f.info/manager/0000814375-donald-smith-co-inc',
                'Fine Capital Partners':'https://13f.info/manager/0001339161-fine-capital-partners-l-p', 
                'Contrarian Capital Management':'https://13f.info/manager/0001050417-contrarian-capital-management-l-l-c', 
                'Masters Capital Management':'https://13f.info/manager/0001104186-masters-capital-management-llc', 
                'Yacktman Asset Management':'https://13f.info/manager/0000905567-yacktman-asset-management-lp', 
                # 'Millennium Management':'https://13f.info/manager/0001273087-millennium-management-llc', 
                'Point72 Asset Management':'https://13f.info/manager/0001603466-point72-asset-management-l-p', 
                'Appaloosa':'https://13f.info/manager/0001656456-appaloosa-lp', 
                'Pershing Square Capital Management':'https://13f.info/manager/0001336528-pershing-square-capital-management-l-p', 
                'Berkshire Hathaway':'https://13f.info/manager/0001067983-berkshire-hathaway-inc', 
                # 'Renaissance Technologies':'https://13f.info/manager/0001037389-renaissance-technologies-llc', 
                # 'Citadel Advisors':'https://13f.info/manager/0001423053-citadel-advisors-llc',
                'Duquesne Family Office':'https://13f.info/manager/0001536411-duquesne-family-office-llc', 
                'Dalal Street Holdings':'https://13f.info/manager/0001549575-dalal-street-llc', 
                'Altarock Partners':'https://13f.info/manager/0001631014-altarock-partners-llc', 
                'Brave Warrior Advisors':'https://13f.info/manager/0001553733-brave-warrior-advisors-llc',
            }

            ### 
            cls.dash_port = '8050'

            '''
            Customized Hedge Components
            '''
            cls.customize_enter_date = '2019-02-15'
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
                ]
            }

            cls.industry_top_selection = 3
            cls.company_top_selection = 3
            cls.enter_cost = 1000000

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
	



	