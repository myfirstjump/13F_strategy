import os

class Configuration(object):

    def __init__(self):

        working_dir = os.getcwd() #返還main.py檔案資料夾
        self.assets_hedge_fund_data = os.path.join(working_dir, "assets\\hedge_fund_data") if os.name == 'nt' else os.path.join(working_dir, "assets/hedge_fund_data")
        self.assets_holdings_data = os.path.join(working_dir, "assets\\holdings_data") if os.name == 'nt' else os.path.join(working_dir, "assets/holdings_data")
        self.backtest_summary = os.path.join(working_dir, "assets\\backtest_summary") if os.name == 'nt' else os.path.join(working_dir, "assets/backtest_summary")
        self.hedge_fund_urls = {
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
	
        self.dash_port = '8050'

	

	

	

	

	

	