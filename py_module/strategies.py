from py_module.config import Configuration

import pymssql
import pandas as pd
import numpy as np
import sys
# np.set_printoptions(threshold=sys.maxsize)
import copy
import os
import datetime

# from pyxirr import xirr

class Strategy13F(object):

    # _instance = None
    # def __new__(cls):
    #     if cls._instance is None:
    #         cls._instance = super(Configuration, cls).__new__(cls)
    #         cls.config_obj = Configuration()

    #         cls.LOCAL_FLAG = True


    #         # 找到price data中的date欄位，對日期進行排序，找到最大的日期
    #         query = self.get_all_price_date(self.config_obj.us_stock_price_table) # 為了取得時間欄位
    #         all_date_list = self.sql_execute(query)
    #         all_date_list = pd.DataFrame(all_date_list)['date'].values
    #         us_sorted_dates = sorted(all_date_list)
    #         self.us_sorted_dates = pd.to_datetime(us_sorted_dates)
    #         self.min_date = min(self.us_sorted_dates)
    #         self.max_date = max(self.us_sorted_dates)
    #         print('美股歷史價格從{}到{}'.format(self.min_date, self.max_date))

    #         query = self.get_all_price_date(self.config_obj.tw_stock_price_table) # 為了取得時間欄位
    #         all_date_list = self.sql_execute(query)
    #         all_date_list = pd.DataFrame(all_date_list)['date'].values
    #         tws_sorted_dates = sorted(all_date_list)
    #         self.tws_sorted_dates = pd.to_datetime(tws_sorted_dates)
    #         self.tws_min_date = min(self.tws_sorted_dates)
    #         self.tws_max_date = max(self.tws_sorted_dates)
    #         print('TWS歷史價格從{}到{}'.format(self.tws_min_date, self.tws_max_date))

    def __init__(self):
        self.config_obj = Configuration()

        # 找到price data中的date欄位，對日期進行排序，找到最大的日期
        query = self.get_all_price_date(self.config_obj.us_stock_price_table) # 為了取得時間欄位
        all_date_list = self.sql_execute(query)
        all_date_list = pd.DataFrame(all_date_list)['date'].values
        us_sorted_dates = sorted(all_date_list)
        self.us_sorted_dates = pd.to_datetime(us_sorted_dates)
        self.min_date = min(self.us_sorted_dates)
        self.max_date = max(self.us_sorted_dates)
        print('美股歷史價格從{}到{}'.format(self.min_date, self.max_date))

        query = self.get_all_price_date(self.config_obj.tw_stock_price_table) # 為了取得時間欄位
        all_date_list = self.sql_execute(query)
        all_date_list = pd.DataFrame(all_date_list)['date'].values
        tws_sorted_dates = sorted(all_date_list)
        self.tws_sorted_dates = pd.to_datetime(tws_sorted_dates)
        self.tws_min_date = min(self.tws_sorted_dates)
        self.tws_max_date = max(self.tws_sorted_dates)
        print('TWS歷史價格從{}到{}'.format(self.tws_min_date, self.tws_max_date))

    def costomized_hedge_build_and_store(self):
        
        # customized_fund_list = {
        #     'SHARPE_I3C3_mcap': (self.customize_fund_components, {'industry_top_selection': 3, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
        #     'SHARPE_I3C2_mcap': (self.customize_fund_components, {'industry_top_selection': 3, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
        #     'SHARPE_I3C1_mcap': (self.customize_fund_components, {'industry_top_selection': 3, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
        #     'SHARPE_I2C3_mcap': (self.customize_fund_components, {'industry_top_selection': 2, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
        #     'SHARPE_I2C2_mcap': (self.customize_fund_components, {'industry_top_selection': 2, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
        #     'SHARPE_I2C1_mcap': (self.customize_fund_components, {'industry_top_selection': 2, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
        #     'SHARPE_I1C3_mcap': (self.customize_fund_components, {'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
        #     'SHARPE_I1C2_mcap': (self.customize_fund_components, {'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
        #     'SHARPE_I1C1_mcap': (self.customize_fund_components, {'industry_top_selection': 1, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
        # }
        customized_fund_list = {
            # 'I3C3_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['sharpe_v3'], 'industry_top_selection': 3, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I3C2_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['sharpe_v3'], 'industry_top_selection': 3, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I3C1_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['sharpe_v3'], 'industry_top_selection': 3, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I2C3_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['sharpe_v3'], 'industry_top_selection': 2, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I2C2_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['sharpe_v3'], 'industry_top_selection': 2, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I2C1_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['sharpe_v3'], 'industry_top_selection': 2, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            'I1C3_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['sharpe_v3'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['sharpe_v3'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I1C1_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['sharpe_v3'], 'industry_top_selection': 1, 'company_top_selection': 1, 'mcap_weighted_flag': True}),

            # 'I3C3_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['sharpe_v3'], 'industry_top_selection': 3, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I3C2_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['sharpe_v3'], 'industry_top_selection': 3, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I3C1_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['sharpe_v3'], 'industry_top_selection': 3, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I2C3_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['sharpe_v3'], 'industry_top_selection': 2, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I2C2_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['sharpe_v3'], 'industry_top_selection': 2, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I2C1_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['sharpe_v3'], 'industry_top_selection': 2, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            'I1C3_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['sharpe_v3'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['sharpe_v3'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I1C1_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': self.config_obj.target_hedge_funds_dict['sharpe_v3'], 'industry_top_selection': 1, 'company_top_selection': 1, 'mcap_weighted_flag': True}),

            'I1C3_reinvest_Barton_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Barton Investment Management'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_Barton_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Barton Investment Management'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            
            'I1C3_reinvest_AMERICAN_FINANCIAL_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['AMERICAN FINANCIAL GROUP INC 401(K) RETIREMENT & SAVINGS PLAN'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_AMERICAN_FINANCIAL_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['AMERICAN FINANCIAL GROUP INC 401(K) RETIREMENT & SAVINGS PLAN'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),

            'I1C3_reinvest_BRISTOL_JOHN_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['BRISTOL JOHN W & CO INC /NY/'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_BRISTOL_JOHN_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['BRISTOL JOHN W & CO INC /NY/'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),

            'I1C3_reinvest_ACR_Alpine_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['ACR Alpine Capital Research, LLC'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_ACR_Alpine_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['ACR Alpine Capital Research, LLC'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),

            'I1C3_reinvest_PEAVINE_CAPITAL_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['PEAVINE CAPITAL, LLC'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_PEAVINE_CAPITAL_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['PEAVINE CAPITAL, LLC'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),

            'I1C3_reinvest_JLB_&_ASSOCIATES_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['JLB & ASSOCIATES INC',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_JLB_&_ASSOCIATES_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['JLB & ASSOCIATES INC',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),

            'I1C3_reinvest_Yacktman_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Yacktman Asset Management',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_Yacktman_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Yacktman Asset Management',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                    
            'I1C3_reinvest_BEDRIJFSTAKPENSIOENFONDS_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['BEDRIJFSTAKPENSIOENFONDS VOOR DE MEDIA PNO',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_BEDRIJFSTAKPENSIOENFONDS_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['BEDRIJFSTAKPENSIOENFONDS VOOR DE MEDIA PNO',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                     
            'I1C3_reinvest_Saratoga_Research_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Saratoga Research & Investment Management',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_Saratoga_Research_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Saratoga Research & Investment Management',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                        
            'I1C3_reinvest_Cohen_Klingenstein_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Cohen Klingenstein LLC',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_Cohen_Klingenstein_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Cohen Klingenstein LLC',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                      
            'I1C3_reinvest_RWWM_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['RWWM, Inc.',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_RWWM_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['RWWM, Inc.',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                      
            'I1C3_reinvest_JENSEN_INVESTMENT_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['JENSEN INVESTMENT MANAGEMENT INC',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_JENSEN_INVESTMENT_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['JENSEN INVESTMENT MANAGEMENT INC',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                     
            'I1C3_reinvest_YCG_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['YCG, LLC',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_YCG_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['YCG, LLC',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                      
            'I1C3_reinvest_H_PARTNERS_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['H PARTNERS MANAGEMENT, LLC',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_H_PARTNERS_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['H PARTNERS MANAGEMENT, LLC',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                          
            'I1C3_reinvest_NEW_YORK_STATE_TEACHERS_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['NEW YORK STATE TEACHERS RETIREMENT SYSTEM',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_NEW_YORK_STATE_TEACHERS_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['NEW YORK STATE TEACHERS RETIREMENT SYSTEM',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                     
            'I1C3_reinvest_SATURNA_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['SATURNA CAPITAL CORP',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_SATURNA_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['SATURNA CAPITAL CORP',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                    
            'I1C3_reinvest_Sanders_Capital_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Sanders Capital, LLC',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_Sanders_Capital_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Sanders Capital, LLC',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                    
            'I1C3_reinvest_Longview_Partners_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Longview Partners (Guernsey) LTD',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_Longview_Partners_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Longview Partners (Guernsey) LTD',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                       
            'I1C3_reinvest_WEATHERLY_ASSET_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['WEATHERLY ASSET MANAGEMENT L. P.',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_WEATHERLY_ASSET_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['WEATHERLY ASSET MANAGEMENT L. P.',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                    
            'I1C3_reinvest_PENSIOENFONDS_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['PENSIOENFONDS RAIL & OV',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_PENSIOENFONDS_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['PENSIOENFONDS RAIL & OV',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                   
            'I1C3_reinvest_Dixon_Mitchell_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Dixon Mitchell Investment Counsel Inc.',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_Dixon_Mitchell_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Dixon Mitchell Investment Counsel Inc.',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                       
            'I1C3_reinvest_Van_Berkom_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Van Berkom & Associates Inc.',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_Van_Berkom_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Van Berkom & Associates Inc.',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                    
            'I1C3_reinvest_Pacifica_Capital_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Pacifica Capital Investments, LLC',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_Pacifica_Capital_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Pacifica Capital Investments, LLC',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                    
            'I1C3_reinvest_HARTFORD_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['HARTFORD INVESTMENT MANAGEMENT CO',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_HARTFORD_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['HARTFORD INVESTMENT MANAGEMENT CO',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                           
            'I1C3_reinvest_Burgundy_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Burgundy Asset Management Ltd.',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_Burgundy_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Burgundy Asset Management Ltd.',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                         
            'I1C3_reinvest_FIDUCIARY_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['FIDUCIARY MANAGEMENT INC /WI/',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_FIDUCIARY_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['FIDUCIARY MANAGEMENT INC /WI/',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                          
            'I1C3_reinvest_FIDUCIARY_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['FIDUCIARY MANAGEMENT INC /WI/',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_FIDUCIARY_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['FIDUCIARY MANAGEMENT INC /WI/',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                      
            'I1C3_reinvest_Scion_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Scion Asset Management',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_Scion_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Scion Asset Management',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
 
            'I1C3_reinvest_Altarock_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Altarock Partners',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_Altarock_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Altarock Partners',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                     
            'I1C3_reinvest_Brave_Warrior_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Brave Warrior Advisors',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_Brave_Warrior_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Brave Warrior Advisors',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                       
            'I1C3_reinvest_Pershing_Square_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Pershing Square Capital Management',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_Pershing_Square_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Pershing Square Capital Management',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                           
            'I1C3_reinvest_Stilwell_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_reinvest_Stilwell_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),

            # share profit
            'I1C3_share_Barton_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Barton Investment Management'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_Barton_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Barton Investment Management'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            
            'I1C3_share_AMERICAN_FINANCIAL_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['AMERICAN FINANCIAL GROUP INC 401(K) RETIREMENT & SAVINGS PLAN'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_AMERICAN_FINANCIAL_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['AMERICAN FINANCIAL GROUP INC 401(K) RETIREMENT & SAVINGS PLAN'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),

            'I1C3_share_BRISTOL_JOHN_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['BRISTOL JOHN W & CO INC /NY/'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_BRISTOL_JOHN_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['BRISTOL JOHN W & CO INC /NY/'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),

            'I1C3_share_ACR_Alpine_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['ACR Alpine Capital Research, LLC'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_ACR_Alpine_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['ACR Alpine Capital Research, LLC'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),

            'I1C3_share_PEAVINE_CAPITAL_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['PEAVINE CAPITAL, LLC'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_PEAVINE_CAPITAL_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['PEAVINE CAPITAL, LLC'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),

            'I1C3_share_JLB_&_ASSOCIATES_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['JLB & ASSOCIATES INC',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_JLB_&_ASSOCIATES_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['JLB & ASSOCIATES INC',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),

            'I1C3_share_Yacktman_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Yacktman Asset Management',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_Yacktman_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Yacktman Asset Management',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                    
            'I1C3_share_BEDRIJFSTAKPENSIOENFONDS_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['BEDRIJFSTAKPENSIOENFONDS VOOR DE MEDIA PNO',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_BEDRIJFSTAKPENSIOENFONDS_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['BEDRIJFSTAKPENSIOENFONDS VOOR DE MEDIA PNO',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                     
            'I1C3_share_Saratoga_Research_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Saratoga Research & Investment Management',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_Saratoga_Research_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Saratoga Research & Investment Management',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                        
            'I1C3_share_Cohen_Klingenstein_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Cohen Klingenstein LLC',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_Cohen_Klingenstein_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Cohen Klingenstein LLC',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                      
            'I1C3_share_RWWM_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['RWWM, Inc.',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_RWWM_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['RWWM, Inc.',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                      
            'I1C3_share_JENSEN_INVESTMENT_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['JENSEN INVESTMENT MANAGEMENT INC',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_JENSEN_INVESTMENT_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['JENSEN INVESTMENT MANAGEMENT INC',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                     
            'I1C3_share_YCG_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['YCG, LLC',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_YCG_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['YCG, LLC',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                      
            'I1C3_share_H_PARTNERS_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['H PARTNERS MANAGEMENT, LLC',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_H_PARTNERS_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['H PARTNERS MANAGEMENT, LLC',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                          
            'I1C3_share_NEW_YORK_STATE_TEACHERS_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['NEW YORK STATE TEACHERS RETIREMENT SYSTEM',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_NEW_YORK_STATE_TEACHERS_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['NEW YORK STATE TEACHERS RETIREMENT SYSTEM',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                     
            'I1C3_share_SATURNA_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['SATURNA CAPITAL CORP',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_SATURNA_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['SATURNA CAPITAL CORP',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                    
            'I1C3_share_Sanders_Capital_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Sanders Capital, LLC',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_Sanders_Capital_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Sanders Capital, LLC',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                    
            'I1C3_share_Longview_Partners_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Longview Partners (Guernsey) LTD',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_Longview_Partners_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Longview Partners (Guernsey) LTD',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                       
            'I1C3_share_WEATHERLY_ASSET_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['WEATHERLY ASSET MANAGEMENT L. P.',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_WEATHERLY_ASSET_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['WEATHERLY ASSET MANAGEMENT L. P.',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                    
            'I1C3_share_PENSIOENFONDS_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['PENSIOENFONDS RAIL & OV',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_PENSIOENFONDS_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['PENSIOENFONDS RAIL & OV',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                   
            'I1C3_share_Dixon_Mitchell_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Dixon Mitchell Investment Counsel Inc.',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_Dixon_Mitchell_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Dixon Mitchell Investment Counsel Inc.',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                       
            'I1C3_share_Van_Berkom_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Van Berkom & Associates Inc.',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_Van_Berkom_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Van Berkom & Associates Inc.',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                    
            'I1C3_share_Pacifica_Capital_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Pacifica Capital Investments, LLC',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_Pacifica_Capital_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Pacifica Capital Investments, LLC',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                    
            'I1C3_share_HARTFORD_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['HARTFORD INVESTMENT MANAGEMENT CO',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_HARTFORD_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['HARTFORD INVESTMENT MANAGEMENT CO',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                           
            'I1C3_share_Burgundy_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Burgundy Asset Management Ltd.',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_Burgundy_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Burgundy Asset Management Ltd.',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                         
            'I1C3_share_FIDUCIARY_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['FIDUCIARY MANAGEMENT INC /WI/',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_FIDUCIARY_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['FIDUCIARY MANAGEMENT INC /WI/',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                          
            'I1C3_share_FIDUCIARY_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['FIDUCIARY MANAGEMENT INC /WI/',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_FIDUCIARY_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['FIDUCIARY MANAGEMENT INC /WI/',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                      
            'I1C3_share_Scion_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Scion Asset Management',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_Scion_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Scion Asset Management',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
 
            'I1C3_share_Altarock_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Altarock Partners',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_Altarock_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Altarock Partners',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                     
            'I1C3_share_Brave_Warrior_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Brave Warrior Advisors',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_Brave_Warrior_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Brave Warrior Advisors',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                       
            'I1C3_share_Pershing_Square_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Pershing Square Capital Management',], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_Pershing_Square_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Pershing Square Capital Management',], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
                           
            'I1C3_share_Stilwell_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            'I1C2_share_Stilwell_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),        
                    
            # 'I3C3_Yacktman_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Yacktman Asset Management'], 'industry_top_selection': 3, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I3C2_Yacktman_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Yacktman Asset Management'], 'industry_top_selection': 3, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I3C1_Yacktman_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Yacktman Asset Management'], 'industry_top_selection': 3, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I2C3_Yacktman_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Yacktman Asset Management'], 'industry_top_selection': 2, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I2C2_Yacktman_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Yacktman Asset Management'], 'industry_top_selection': 2, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I2C1_Yacktman_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Yacktman Asset Management'], 'industry_top_selection': 2, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I1C3_Yacktman_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Yacktman Asset Management'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I1C2_Yacktman_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Yacktman Asset Management'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I1C1_Yacktman_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Yacktman Asset Management'], 'industry_top_selection': 1, 'company_top_selection': 1, 'mcap_weighted_flag': True}),

            # 'I3C3_Scion_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Scion Asset Management'], 'industry_top_selection': 3, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I3C2_Scion_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Scion Asset Management'], 'industry_top_selection': 3, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I3C1_Scion_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Scion Asset Management'], 'industry_top_selection': 3, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I2C3_Scion_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Scion Asset Management'], 'industry_top_selection': 2, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I2C2_Scion_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Scion Asset Management'], 'industry_top_selection': 2, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I2C1_Scion_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Scion Asset Management'], 'industry_top_selection': 2, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I1C3_Scion_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Scion Asset Management'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I1C2_Scion_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Scion Asset Management'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I1C1_Scion_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Scion Asset Management'], 'industry_top_selection': 1, 'company_top_selection': 1, 'mcap_weighted_flag': True}),

            # 'I3C3_Altarock_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Altarock Partners'], 'industry_top_selection': 3, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I3C2_Altarock_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Altarock Partners'], 'industry_top_selection': 3, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I3C1_Altarock_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Altarock Partners'], 'industry_top_selection': 3, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I2C3_Altarock_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Altarock Partners'], 'industry_top_selection': 2, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I2C2_Altarock_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Altarock Partners'], 'industry_top_selection': 2, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I2C1_Altarock_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Altarock Partners'], 'industry_top_selection': 2, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I1C3_Altarock_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Altarock Partners'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I1C2_Altarock_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Altarock Partners'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I1C1_Altarock_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Altarock Partners'], 'industry_top_selection': 1, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
        
            # 'I3C3_Brave_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Brave Warrior Advisors'], 'industry_top_selection': 3, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I3C2_Brave_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Brave Warrior Advisors'], 'industry_top_selection': 3, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I3C1_Brave_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Brave Warrior Advisors'], 'industry_top_selection': 3, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I2C3_Brave_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Brave Warrior Advisors'], 'industry_top_selection': 2, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I2C2_Brave_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Brave Warrior Advisors'], 'industry_top_selection': 2, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I2C1_Brave_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Brave Warrior Advisors'], 'industry_top_selection': 2, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I1C3_Brave_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Brave Warrior Advisors'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I1C2_Brave_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Brave Warrior Advisors'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I1C1_Brave_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Brave Warrior Advisors'], 'industry_top_selection': 1, 'company_top_selection': 1, 'mcap_weighted_flag': True}),

            # 'I3C3_Pershing_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Pershing Square Capital Management'], 'industry_top_selection': 3, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I3C2_Pershing_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Pershing Square Capital Management'], 'industry_top_selection': 3, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I3C1_Pershing_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Pershing Square Capital Management'], 'industry_top_selection': 3, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I2C3_Pershing_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Pershing Square Capital Management'], 'industry_top_selection': 2, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I2C2_Pershing_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Pershing Square Capital Management'], 'industry_top_selection': 2, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I2C1_Pershing_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Pershing Square Capital Management'], 'industry_top_selection': 2, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I1C3_Pershing_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Pershing Square Capital Management'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I1C2_Pershing_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Pershing Square Capital Management'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I1C1_Pershing_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Pershing Square Capital Management'], 'industry_top_selection': 1, 'company_top_selection': 1, 'mcap_weighted_flag': True}),

            # 'I3C3_Stilwell_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 3, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I3C2_Stilwell_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 3, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I3C1_Stilwell_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 3, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I2C3_Stilwell_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 2, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I2C2_Stilwell_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 2, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I2C1_Stilwell_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 2, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I1C3_Stilwell_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I1C2_Stilwell_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I1C1_Stilwell_reinvest_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':False, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 1, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
        
        

            # 'I3C3_Yacktman_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Yacktman Asset Management'], 'industry_top_selection': 3, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I3C2_Yacktman_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Yacktman Asset Management'], 'industry_top_selection': 3, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I3C1_Yacktman_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Yacktman Asset Management'], 'industry_top_selection': 3, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I2C3_Yacktman_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Yacktman Asset Management'], 'industry_top_selection': 2, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I2C2_Yacktman_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Yacktman Asset Management'], 'industry_top_selection': 2, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I2C1_Yacktman_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Yacktman Asset Management'], 'industry_top_selection': 2, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I1C3_Yacktman_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Yacktman Asset Management'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I1C2_Yacktman_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Yacktman Asset Management'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I1C1_Yacktman_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Yacktman Asset Management'], 'industry_top_selection': 1, 'company_top_selection': 1, 'mcap_weighted_flag': True}),

            # 'I3C3_Scion_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Scion Asset Management'], 'industry_top_selection': 3, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I3C2_Scion_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Scion Asset Management'], 'industry_top_selection': 3, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I3C1_Scion_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Scion Asset Management'], 'industry_top_selection': 3, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I2C3_Scion_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Scion Asset Management'], 'industry_top_selection': 2, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I2C2_Scion_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Scion Asset Management'], 'industry_top_selection': 2, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I2C1_Scion_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Scion Asset Management'], 'industry_top_selection': 2, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I1C3_Scion_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Scion Asset Management'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I1C2_Scion_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Scion Asset Management'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I1C1_Scion_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Scion Asset Management'], 'industry_top_selection': 1, 'company_top_selection': 1, 'mcap_weighted_flag': True}),

            # 'I3C3_Altarock_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Altarock Partners'], 'industry_top_selection': 3, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I3C2_Altarock_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Altarock Partners'], 'industry_top_selection': 3, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I3C1_Altarock_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Altarock Partners'], 'industry_top_selection': 3, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I2C3_Altarock_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Altarock Partners'], 'industry_top_selection': 2, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I2C2_Altarock_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Altarock Partners'], 'industry_top_selection': 2, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I2C1_Altarock_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Altarock Partners'], 'industry_top_selection': 2, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I1C3_Altarock_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Altarock Partners'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I1C2_Altarock_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Altarock Partners'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I1C1_Altarock_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Altarock Partners'], 'industry_top_selection': 1, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
        
            # 'I3C3_Brave_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Brave Warrior Advisors'], 'industry_top_selection': 3, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I3C2_Brave_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Brave Warrior Advisors'], 'industry_top_selection': 3, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I3C1_Brave_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Brave Warrior Advisors'], 'industry_top_selection': 3, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I2C3_Brave_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Brave Warrior Advisors'], 'industry_top_selection': 2, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I2C2_Brave_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Brave Warrior Advisors'], 'industry_top_selection': 2, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I2C1_Brave_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Brave Warrior Advisors'], 'industry_top_selection': 2, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I1C3_Brave_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Brave Warrior Advisors'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I1C2_Brave_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Brave Warrior Advisors'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I1C1_Brave_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Brave Warrior Advisors'], 'industry_top_selection': 1, 'company_top_selection': 1, 'mcap_weighted_flag': True}),

            # 'I3C3_Pershing_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Pershing Square Capital Management'], 'industry_top_selection': 3, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I3C2_Pershing_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Pershing Square Capital Management'], 'industry_top_selection': 3, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I3C1_Pershing_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Pershing Square Capital Management'], 'industry_top_selection': 3, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I2C3_Pershing_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Pershing Square Capital Management'], 'industry_top_selection': 2, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I2C2_Pershing_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Pershing Square Capital Management'], 'industry_top_selection': 2, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I2C1_Pershing_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Pershing Square Capital Management'], 'industry_top_selection': 2, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I1C3_Pershing_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Pershing Square Capital Management'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I1C2_Pershing_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Pershing Square Capital Management'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I1C1_Pershing_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Pershing Square Capital Management'], 'industry_top_selection': 1, 'company_top_selection': 1, 'mcap_weighted_flag': True}),

            # 'I3C3_Stilwell_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 3, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I3C2_Stilwell_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 3, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I3C1_Stilwell_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 3, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I2C3_Stilwell_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 2, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I2C2_Stilwell_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 2, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I2C1_Stilwell_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 2, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
            # 'I1C3_Stilwell_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 1, 'company_top_selection': 3, 'mcap_weighted_flag': True}),
            # 'I1C2_Stilwell_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 1, 'company_top_selection': 2, 'mcap_weighted_flag': True}),
            # 'I1C1_Stilwell_share_0803': (self.customize_fund_components_revised, {'reinvest_flag':True, 'share_profit_flag':True, 'hedge_funds_range': ['Stilwell Value'], 'industry_top_selection': 1, 'company_top_selection': 1, 'mcap_weighted_flag': True}),
        
        }


        holdings_dict = {}
        portfolio_dict = {}
        self.config_obj.logger.warning('準備建置 {} 組自定義基金'.format(len(customized_fund_list)))
        for k_, v_ in customized_fund_list.items():
            self.config_obj.logger.warning('建置自定義基金數據 {}'.format(k_))
            func_to_call, params = v_
            customized_fund_data, customized_table = func_to_call(**params)

            holdings_data = self.modify_customized_fund_data_to_holdings_data_structures(k_, customized_fund_data)
            portfolio_data = self.arrage_customized_fund_portfolio_data(k_, holdings_data)
            holdings_dict[k_] = holdings_data
            portfolio_dict[k_] = portfolio_data
            self.config_obj.logger.debug(holdings_data)
            self.config_obj.logger.debug(portfolio_data)
        # path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_customized_data.xlsx')
        # with pd.ExcelWriter(path) as writer:
        #     for k_, v_ in holdings_dict.items():
        #         holdings_dict[k_].to_excel(writer, index=False, sheet_name=k_)
        #         portfolio_dict[k_].to_excel(writer, index=False, sheet_name=k_ + '_portfolio')



        for k_, v_ in holdings_dict.items():
            self.config_obj.logger.warning('Costomized_hedge:{}'.format(k_))
            table_name, inserted_rows = self.insert_records_to_DB(table_name=self.config_obj.customized_holdings_data_table, data=v_)
            self.config_obj.logger.warning('資料庫數據Insert:TABLE{} 筆數{}'.format(table_name, inserted_rows))
            table_name, inserted_rows = self.insert_records_to_DB(table_name=self.config_obj.customized_fund_portfolio_table, data=portfolio_dict[k_])
            self.config_obj.logger.warning('資料庫數據Insert:TABLE{} 筆數{}'.format(table_name, inserted_rows))

    def back_test_flow(self):
        '''
        美股13F投資策略回測程序
            0. 定義Output obj、統計用obj:
                -. summary_table: pd.DataFrame
                    同summary_data的資料，但是更方便處理欄位關係。
                -. corr_analysis_data: list of dict
                    用來儲存需要進行相關性分析資料 {'stock_id': None, 'spread_ratio': None, 'scaling': None}
                -. null_sym_counter: int
                    計算holdings報告中的SYM，無法對應到price table的數量，作為後續調整參考使用。
                -. base_13F_date_list: list
                    列舉13F報告所有based date(截止日5/15, 8/14, 11/14, 2/14)，供後續抓取想比較的個股資料比對用。
            1. Read DB data
                -. hedge fund data: fund_data
                -. date list which have the prices: sorted_dates, max_date(最後一天，用來計算最後統計量)
            2. 各hedge fund計算迴圈
                2.1. 定義迴圈內參數:
                    -. summary_data: list of dict
                        {'date': holdings_time, '市值': market_value, '加碼': scaling_in_sum, '減碼': scaling_out_sum, 'XIRR':xirr}
                        主要輸出的物件，list每個元素是某hedge fund 在13F報告時間點的市值、加碼量、減碼量、XIRR計算值。
                    -. previous_holdings: pd.DataFrame
                        用來儲存上一個13F報告的holdings，主要用來計算shares差異。
                    -. xirr_calculate_dict: {'date':[], 'amounts':[]}
                        用來記錄holdings time和amounts，最後用來輸入至pyxirr計算XIRR值。
                2.2. 調整fund_data:
                    依據條件篩選fund_data中的records。(詳細如each_fund_data_adjust())
                2.3. 各Quarter計算迴圈:
                    2.3.1 定義迴圈內參數:
                        -. hedge_fund_data = {'date': None, '市值': None, '加碼': None, '減碼': None, 'XIRR': None}
                            即要放在主要輸出summary_data中的元素，每一季的統計數據。
                    2.3.2 調整holdings_time:
                        由於13F報告日期不一定有開市，故調整至下一個開市日期。(詳細如adjust_holdings_time())
                    2.3.3 讀取holdings data，並進行特殊處理(詳細如holdings_data_adjust())
                    2.3.4 IF-ELSE語句分別處理第一季/其他季
                        
                    2.3.5 以xirr_calculate_dict計算XIRR值
                    2.3.6 將本季度holdings data暫存，作為下一季度計算使用。(previous_holdings)
                    2.3.7 將統計數值回存至summary_data
                2.4 計算當前持股市值。
            3. 加入其他想比較的個股。(S&P500、0050.TW)
                由於TWS與US的開市時間不同，包裝一個函數能夠修改base_13F_date_list內元素，調整為有開市的時間，才找得到price。(詳細如individual_stock_summary())
            4. 加入自定義基金比較
            5. 輸出資料表格
        '''

        print('Execute Hedge Fund Backtest Flow...')
        '''0. 定義Output obj、統計用obj'''
        ### Final Output Form
        
        summary_table = None
        null_sym_counter = 0
        base_13F_date_list = []

        '''1. Read DB data'''
        query = self.create_query_data_table(self.config_obj.hedge_fund_portfolio_table)
        fund_data = self.sql_execute(query)
        fund_data = pd.DataFrame(fund_data)
        hedge_fund_list = fund_data['HEDGE_FUND'].unique()
        # hedge_fund_list = ['Appaloosa', ]
        hedge_fund_list = list(hedge_fund_list)
        # hedge_fund_list = ['Scion Asset Management']
        hedge_fund_list.remove('Citadel Advisors')
        hedge_fund_list.remove('Renaissance Technologies')
        hedge_fund_list.remove('Millennium Management')
        self.config_obj.logger.warning("Back_Test_flow，總共包含{}個對沖基金資料".format(len(hedge_fund_list)))
        # print('Hedge Funds:', )
        # print(hedge_fund_list)

        '''2. 各hedge fund計算迴圈'''
        for idx, hedge_fund in enumerate(hedge_fund_list):
            '''2.1. 定義迴圈內參數'''
            summary_data = [] # 包含項目: {'date': holdings_time, '市值': market_value, '加碼': scaling_in_sum, '減碼': scaling_out_sum, 'XIRR':xirr}
            corr_analysis_table = None #TBD
            previous_holdings = None
            xirr_calculate_dict = {'date':[], 'amounts':[]}
            previous_holdings_time = None
            previous_sym_str = tuple() #TBD
            '''2.2. 調整fund_data'''
            each_fund_data = self.each_fund_data_adjust(fund_data, hedge_fund)
            quarters_list = each_fund_data['QUARTER'].values
            date_list = each_fund_data['DATE_FILED'].values # 進場時間點使用該基金13F公布時間
            filing_list = each_fund_data['FILING_ID'].values
            if idx == 0:
                base_13F_dates = each_fund_data['BASE_DATE'].values# 進場時間點使用13F公布截止時間
                base_13F_date_list = pd.to_datetime(base_13F_dates, unit='ns')
                base_13F_date_list = [str(date) for date in base_13F_date_list.tolist()]

            print(" === === === 第{}個對沖基金：{}，包含{}個季度資料。 ".format(idx+1, hedge_fund, len(quarters_list)))
            self.config_obj.logger.warning("Back_Test_flow，對沖基金{}，最後一個季度：{}".format(hedge_fund, quarters_list[-1]))
            # print(each_fund_data)
            '''2.3. 各Quarter計算迴圈'''
            for idx_q, (quarter, holdings_time, filing_number) in enumerate(zip(quarters_list, date_list, filing_list)):
                '''2.3.1 定義回圈內參數'''
                hedge_fund_data = {'date': None, '市值': None, '加碼': None, '減碼': None, 'XIRR': None}
                
                '''2.3.2 調整holdings_time'''
                holdings_time = self.adjust_holdings_time(holdings_time, self.us_sorted_dates) # 以13F報告公布期限為基準(5/15, 8/14, 11/14, 2/14)
                # print("     第{}個季度：{}，時間為{}".format(idx_q+1, quarter, holdings_time))

                '''2.3.3 讀取holdings data'''
                query = self.create_query_holdings(hedge_fund, quarter, filing_number)
                holdings_data = self.sql_execute(query)
                holdings_data = pd.DataFrame(holdings_data)
                holdings_data = self.holdings_data_adjust(holdings_data)
                '''
                        SYM                   ISSUER_NAME         CL      CUSIP  ...      HEDGE_FUND  QUARTER  FORM_TYPE           FILING_ID
                0     GME             GAMESTOP CORP NEW       CL A  36467W109  ...  Robotti Robert  Q4 2015     13F-HR  000114036116052554
                1    PSMT                PRICESMART INC        COM  741511109  ...  Robotti Robert  Q4 2015     13F-HR  000114036116052554
                2    AEIS          ADVANCED ENERGY INDS        COM  007973100  ...  Robotti Robert  Q4 2015     13F-HR  000114036116052554
                '''
                '''2.3.4 IF-ELSE語句分別處理第一季/其他季
                        2.3.4.1 若為第一季
                            2.3.4.1.1 確認有哪些股票代碼，存為sym_str，空值則計數後存入null_sym_counter。
                            2.3.4.1.2 以holdings time和sym_str查詢當時股價。(create_query_get_open_price_by_date_n_sym())
                            2.3.4.1.3 計算market value: 以holdings_data:SHARE、price_data:Open相乘得出股票市值。(market_value_by_join_holdings_and_price())
                            2.3.4.1.4 第一季加減碼為0。
                        2.3.4.2 若為其他季度
                            2.3.4.2.1 計算本季度與上一季度的SHARE差值，主要用以計算加減碼金額。(shares_difference_between_quarters())
                            2.3.4.2.2 確認有哪些股票代碼，存為sym_str，空值則計數後存入null_sym_counter。
                            2.3.4.2.3 以holdings time和sym_str查詢當時股價。(create_query_get_open_price_by_date_n_sym())
                            2.3.4.2.4 計算市值/加碼/減碼(calculate_scaling_in_and_out())
                '''
                if idx_q > 0: #扣除第一季，每季要計算的內容
                    shares_data = self.shares_difference_between_quarters(previous_holdings, holdings_data)
                    '''
                          SYM  SHARES_current  SHARES_previous  shares_change
                    0     AGN         2937121          4261406       -1324285
                    1    ETP1        11961842         11961842              0
                    2     WPZ         9966502          9487301         479201
                    '''
                    null_sym_counter = null_sym_counter + shares_data['SYM'].isna().sum()
                    sym_str = shares_data['SYM'].dropna().values
                    
                    # print("SYMs:", sym_str)
                    if len(sym_str) == 1:
                        sym_str = str(sym_str)
                        sym_str = sym_str.replace('[', '(')
                        sym_str = sym_str.replace(']', ')')
                    else:
                        sym_str = tuple(sym_str)
                    
                    query = self.create_query_get_open_price_by_date_n_sym(sym_str, holdings_time)
                    price_data = self.sql_execute(query)
                    price_data = pd.DataFrame(price_data)
                    # print('price_data:')
                    # print(price_data)
                    '''
                            date   SYM    Open
                        0   2016-11-14  AAPL   26.93
                        1   2016-11-14   ALL   69.76
                        2   2016-11-14    AY   16.98
                    '''
                    market_value, scaling_in, scaling_out, scaling_even = self.calculate_scaling_in_and_out(shares_data, price_data)
                    # print("Shares Increased:", scaling_in)
                    # print("Shares Decreased:", scaling_out)
                    # print("Shares Unchanged:", scaling_even)
                    scaling_in_sum = sum([i for i in  scaling_in.values()])
                    scaling_out_sum = sum([i for i in  scaling_out.values()])
                    
                    xirr_calculate_dict['date'].append(holdings_time)
                    xirr_calculate_dict['amounts'].append(-(scaling_in_sum - scaling_out_sum))

                    ### correlation analysis TBD
                    current_sym_str = holdings_data['SYM'].dropna().values 
                    current_sym_str = tuple(current_sym_str)
                    # print("SYMs:", current_sym_str)            
                    # print("previous_sym_str:", previous_sym_str)
                    sym_str_combined = set(previous_sym_str) | set(current_sym_str)
                    sym_str_combined = tuple(sym_str_combined)
                    price_ratio_data = self.get_price_change_ratio(previous_holdings_time, holdings_time, sym_str_combined)
                    corr_analysis_data = self.arrange_corr_analysis_data(price_ratio_data, shares_data)
                    if corr_analysis_table is None:
                        corr_analysis_table = corr_analysis_data
                    else:
                        corr_analysis_table = pd.concat([corr_analysis_table, corr_analysis_data], ignore_index=True)
                    
                else: #第一季要計算的內容
                    null_sym_counter = null_sym_counter + holdings_data['SYM'].isna().sum()
                    current_sym_str = holdings_data['SYM'].dropna().values # TBD: 確認Drop數量
                    if len(current_sym_str) == 1:
                        current_sym_str = str(current_sym_str)
                        current_sym_str = current_sym_str.replace('[', '(')
                        current_sym_str = current_sym_str.replace(']', ')')
                    else:
                        current_sym_str = tuple(current_sym_str)
                    query = self.create_query_get_open_price_by_date_n_sym(current_sym_str, holdings_time)
                    price_data = self.sql_execute(query)
                    price_data = pd.DataFrame(price_data)
                    market_value = self.market_value_by_join_holdings_and_price(holdings_data, price_data)

                    scaling_in_sum = 0
                    scaling_out_sum = 0
                    xirr_calculate_dict['date'].append(holdings_time)
                    xirr_calculate_dict['amounts'].append(-market_value)
                '''2.3.5 以xirr_calculate_dict計算XIRR值'''
                temp_xirr_calculate_dict = copy.deepcopy(xirr_calculate_dict)
                if idx_q == 0: # 第一季直接帶pyxirr公式計算結果為10%，沒有研究計算公式，故直接assign 0。
                    xirr = 0
                else:
                    xirr = self.calculate_XIRR(temp_xirr_calculate_dict, holdings_time, market_value)
                '''2.3.6 將本季度holdings data/price_data暫存，作為下一季度計算使用。'''
                previous_holdings = holdings_data.copy()
                previous_holdings_time = holdings_time
                previous_sym_str = current_sym_str # correlation analysis使用
                '''2.3.7 將統計數值回存至summary_data'''
                hedge_fund_data = {'date': holdings_time, '市值': market_value, '加碼': scaling_in_sum, '減碼': scaling_out_sum, 'XIRR':xirr}
                summary_data.append({'hedge_fund': hedge_fund, **hedge_fund_data})
            '''2.4 計算當前持股市值。'''
            holdings_time = self.max_date#2024-03-06'#self.max_date # 可以自訂，此處以DB中最大有交易日期為主(2024-01-09)
            base_13F_date_list.append(str(holdings_time))
            query = self.create_query_get_open_price_by_join_holdings_n_price(sym_str, holdings_time, hedge_fund, quarter, filing_number)
            price_data = self.sql_execute(query)
            price_data = pd.DataFrame(price_data)
            market_value = sum(price_data['Open'] * price_data['SHARES'])
            xirr = self.calculate_XIRR(xirr_calculate_dict, holdings_time, market_value)
            hedge_fund_data = {'date': holdings_time, '市值': market_value, '加碼': 0, '減碼': 0, 'XIRR':xirr}
            summary_data.append({'hedge_fund': hedge_fund, **hedge_fund_data})
            hedge_summary = pd.DataFrame(summary_data)
            hedge_summary = self.summary_statistical_calculates(hedge_summary)
            if summary_table is None:
                summary_table = hedge_summary
            else:
                summary_table = pd.concat([summary_table, hedge_summary], ignore_index=True)
            
            '''4. 相關性分析 TBD'''
            # print(" === === === 第{}個對沖基金：{}，包含{}個季度資料。".format(idx+1, hedge_fund, len(quarters_list)))
            correlation = corr_analysis_table['price_change_ratio'].corr(corr_analysis_table['scaling'])
            # print("Correlation between price_change_ratio and scaling:", correlation)
            # print("{} ---> {}".format(round(correlation, 2), hedge_fund))
        '''3. 加入其他想比較的個股'''
        wish_list = {'^GSPC':'us', '0050':'tw', '0056':'tw'}
        for k_, v_ in wish_list.items():
            print(" === === === 加入個股比較 {}.{}  ".format(k_, v_))
            # individual_summary = self.individual_stock_summary(base_13F_date_list, 'us', '^GSPC')
            # summary_table = pd.concat([summary_table, individual_summary], ignore_index=True)
            individual_summary = self.individual_stock_summary(base_13F_date_list, v_, k_)
            summary_table = pd.concat([summary_table, individual_summary], ignore_index=True)

        '''4. 加入自定義基金比較'''

        customized_fund_list = {
            '自組基金_產業前3公司前3': (self.customize_fund_components, {'industry_top_selection': 3, 'company_top_selection': 3}),
            '自組基金_產業前3公司前2': (self.customize_fund_components, {'industry_top_selection': 3, 'company_top_selection': 2}),
            '自組基金_產業前3公司前1': (self.customize_fund_components, {'industry_top_selection': 3, 'company_top_selection': 1}),
            '自組基金_產業前2公司前3': (self.customize_fund_components, {'industry_top_selection': 2, 'company_top_selection': 3}),
            '自組基金_產業前2公司前2': (self.customize_fund_components, {'industry_top_selection': 2, 'company_top_selection': 2}),
            '自組基金_產業前2公司前1': (self.customize_fund_components, {'industry_top_selection': 2, 'company_top_selection': 1}),
            '自組基金_產業前1公司前3': (self.customize_fund_components, {'industry_top_selection': 1, 'company_top_selection': 3}),
            '自組基金_產業前1公司前2': (self.customize_fund_components, {'industry_top_selection': 1, 'company_top_selection': 2}),
            '自組基金_產業前1公司前1': (self.customize_fund_components, {'industry_top_selection': 1, 'company_top_selection': 1}),
        }

        fund_components_dict = {}
        fund_components_dict_by_hedge = {}
        for k_, v_ in customized_fund_list.items():
            print(' === === === 加入自定義基金進行比較 {}'.format(k_))
            func_to_call, params = v_
            customized_fund_data, customized_table = func_to_call(**params)
            fund_components_dict[k_] = customized_fund_data
            fund_components_dict_by_hedge[k_] = customized_table
            customized_fund_summary = self.customized_fund_stock_summary(k_, customized_fund_data)
            summary_table = pd.concat([summary_table, customized_fund_summary], ignore_index=True)

        '''5. 輸出資料表格'''
        XIRR_table = self.arragne_output_XIRR_excel_format(summary_table)
        if not os.path.exists(self.config_obj.backtest_summary):
            os.makedirs(self.config_obj.backtest_summary)
        path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_summary_table.xlsx')
        with pd.ExcelWriter(path) as writer:  
            summary_table.to_excel(writer, index=False, sheet_name='raw_data')
            XIRR_table.to_excel(writer, index=False, sheet_name='XIRR排序')
            for fund_name, components_table in fund_components_dict.items():
                components_table.to_excel(writer, index=False, sheet_name=fund_name)
        print("NULL SYM COUNTER:", null_sym_counter)
        # self.config_obj.logger.warning("Web_crawler_13F，預計爬取共{}支基金資料".format(len(urls)))

        #Fund components by each hedge analysis
        path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_customized_table_by_hedge.xlsx')
        with pd.ExcelWriter(path) as writer:  
            for fund_name, components_table in fund_components_dict_by_hedge.items():
                components_table.to_excel(writer, index=False, sheet_name=fund_name)

    def customize_fund_components_revised(self, reinvest_flag, share_profit_flag, hedge_funds_range, industry_top_selection, company_top_selection, mcap_weighted_flag): 
        
        '''
        相較於customize_fund_components()，迴圈先Quarter再Hedge，使得每季可以計算總資金變化。

        function:
            製作自定義基金。
        Input:
            -.industry_top_selection: int
                各基金取產業市值前幾
            -.company_top_selection: int
                各基金產業，取個股市值前幾
            -.mcap_weighted_flag
                產業、個股是否以市值加權 (mv: market)
        '''
        save_excel_details_flag = False #控制是否產製Excel文件查看計算細節
        enter_date = self.config_obj.customize_enter_date #2019 2/15 開始進場
        # hedge_funds = self.config_obj.target_hedge_funds_dict['XIRR_output_filter'] #XIRR表現在波克夏以上的基金
        # hedge_funds = self.config_obj.target_hedge_funds_dict['sharpe_output_filter'] #計算已平倉獲利後，依照sharpe ratio、勝算比排序
        hedge_funds = hedge_funds_range
        
        enter_cost = self.config_obj.enter_cost
        '''定義Output obj、統計用obj'''
        ### Final Output Form
        adjusted_fund_data = None
        customized_table = None

        '''Read DB data'''
        query = self.create_query_data_table(self.config_obj.hedge_fund_portfolio_table)
        fund_data = self.sql_execute(query)
        fund_data = pd.DataFrame(fund_data)
        fund_data = fund_data[fund_data['HEDGE_FUND'].isin(hedge_funds)]
        hedge_fund_list = hedge_funds


        '''各hedge fund計算迴圈'''
        for idx, hedge_fund in enumerate(hedge_fund_list):
            '''調整fund_data'''
            each_fund_data = self.each_fund_data_adjust(fund_data, hedge_fund, enter_date)

            if adjusted_fund_data is None:
                    adjusted_fund_data = each_fund_data
            else:
                adjusted_fund_data = pd.concat([adjusted_fund_data, each_fund_data], ignore_index=True)

        adjusted_fund_data = adjusted_fund_data.sort_values(by=['BASE_DATE'], ascending=True).reset_index(drop=True)
        # print(adjusted_fund_data)
        
        date_list = adjusted_fund_data['BASE_DATE'].unique()
        quarters_list = adjusted_fund_data['QUARTER'].unique()
        date_13F_list = adjusted_fund_data['13F_DATE'].unique()
        

        holdings_dict = {}
        GICs_dict = {}
        processed_dict = {}
        industries_select_dict = {}
        company_select_dict = {}

        for idx_q, (quarter, holdings_time, date_13F) in enumerate(zip(quarters_list, date_list, date_13F_list)):

            q_customized_table = None
            holdings_time = self.adjust_holdings_time(holdings_time, self.us_sorted_dates, next_day=False) # 季末當日，即3/31、6/30、9/30、12/31
            date_13F = self.adjust_holdings_time(date_13F, self.us_sorted_dates, next_day=False)
            sub_fund_list = adjusted_fund_data[adjusted_fund_data['QUARTER']==quarter]['HEDGE_FUND'].values

            
            for idx, hedge_fund in enumerate(sub_fund_list):

                filing_number = adjusted_fund_data[(adjusted_fund_data['QUARTER']==quarter) & (adjusted_fund_data['HEDGE_FUND']==hedge_fund)]['FILING_ID'].values[0]
                query = self.create_query_holdings_with_gics_n_price(hedge_fund, quarter, filing_number, holdings_time)
                holdings_data_Q_date = self.sql_execute(query)
                holdings_data_Q_date = pd.DataFrame(holdings_data_Q_date)

                query = self.create_query_holdings_with_gics_n_price(hedge_fund, quarter, filing_number, date_13F)
                holdings_data_13F_date = self.sql_execute(query)
                holdings_data_13F_date = pd.DataFrame(holdings_data_13F_date)

                if holdings_data_Q_date.empty or holdings_data_13F_date.empty:
                    sub_fund_list = sub_fund_list[sub_fund_list != hedge_fund]
                    print(f"Season {quarter} remove hedge {hedge_fund} because NO DATA.")
                    continue

                holdings_data = pd.merge(holdings_data_Q_date, holdings_data_13F_date[['SYM', 'QUARTER', 'date', 'price']], on=['SYM', 'QUARTER'], how='inner')
                holdings_data = holdings_data.rename(columns={'date_y': 'date_13F', 'price_y': 'price_13F'})
                holdings_data = holdings_data.rename(columns={'date_x': 'date', 'price_x': 'price'})
                                
                holdings_data, holdings_GICs_data = self.group_by_GICs_from_holdings_data(holdings_data) # holdings_data新增欄位；建置holdings_GICs_data
                '''儲存holdings_dict'''
                if save_excel_details_flag:
                    if hedge_fund not in holdings_dict.keys():
                        holdings_dict[hedge_fund] = holdings_data
                    else:
                        holdings_dict[hedge_fund] = pd.concat([holdings_dict[hedge_fund], holdings_data], ignore_index=True)
                '''儲存GICs_dict'''
                if save_excel_details_flag:
                    if hedge_fund not in GICs_dict.keys():
                        GICs_dict[hedge_fund] = holdings_GICs_data
                    else:
                        GICs_dict[hedge_fund] = pd.concat([GICs_dict[hedge_fund], holdings_GICs_data], ignore_index=True)
                processed_data, industry_result_data, company_result_data = self.select_company_from_holdings_adjusted(holdings_data, holdings_GICs_data, industry_top_selection, company_top_selection)

                '''儲存industries_select_dict'''
                if save_excel_details_flag:
                    if hedge_fund not in industries_select_dict.keys():
                        industries_select_dict[hedge_fund] = industry_result_data
                    else:
                        industries_select_dict[hedge_fund] = pd.concat([industries_select_dict[hedge_fund], industry_result_data], ignore_index=True)
                ''''''
                if save_excel_details_flag:
                    if hedge_fund not in processed_dict.keys(): #re-weight for company
                        processed_dict[hedge_fund] = processed_data
                    else:
                        processed_dict[hedge_fund] = pd.concat([processed_dict[hedge_fund], processed_data], ignore_index=True)
                '''儲存company_select_dict'''
                if save_excel_details_flag:
                    if hedge_fund not in company_select_dict.keys():
                        company_select_dict[hedge_fund] = company_result_data
                    else:
                        company_select_dict[hedge_fund] = pd.concat([company_select_dict[hedge_fund], company_result_data], ignore_index=True)
                hedge_num = len(sub_fund_list) #本季有幾間hedge
                company_result_data_copy = company_result_data.copy()

                # 依據投資時間(即13F報告時間)，進行篩選額外操作(若需要，例如高價股刪除)，並將資金分配至個股，計算建議購買股數(shares_to_buy)。
                customized_holdings = self.calculate_customized_shares(company_result_data_copy, enter_cost, hedge_num, mcap_weighted_flag)
                if q_customized_table is None:
                    q_customized_table = customized_holdings
                else:
                    q_customized_table = pd.concat([q_customized_table, customized_holdings], ignore_index=True)
            q_customized_table = self.individual_stock_filtering(q_customized_table, upper_price_limit=self.config_obj.upper_price_limit)
            q_customized_table_by_stock = self.arrange_customized_table(q_customized_table) #合併share_to_buy from different hedge fund
            if customized_table is None:
                customized_table = q_customized_table_by_stock
            else:
                customized_table = pd.concat([customized_table, q_customized_table_by_stock], ignore_index=True)

            if reinvest_flag: #利潤再投入

                if share_profit_flag:
                    if 'Q2' in quarter: #13F Q3報告出來，即11/15開始分潤計算、重置資金。
                        new_cmap = self.calculate_market_price_growth(q_customized_table_by_stock) #下一季的投資金額
                        enter_cost = self.config_obj.enter_cost if new_cmap > self.config_obj.enter_cost else new_cmap #若賠錢則無分潤(沿用new_cmap)，若賺錢則分潤後，重置資金
                    else:
                        enter_cost = self.calculate_market_price_growth(q_customized_table_by_stock) #下一季的投資金額
                # print('{}-MCAP:{}'.format(quarter, enter_cost))
                else:
                    enter_cost = self.calculate_market_price_growth(q_customized_table_by_stock) #下一季的投資金額

        '''
        輸出Excel
        '''
        if save_excel_details_flag:
            path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_原持股_I{}C{}.xlsx'.format(industry_top_selection, company_top_selection))
            with pd.ExcelWriter(path) as writer:
                for k_, v_ in holdings_dict.items():
                    holdings_dict[k_].to_excel(writer, index=False, sheet_name=k_)
            
            path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_原產業_I{}C{}.xlsx'.format(industry_top_selection, company_top_selection))
            with pd.ExcelWriter(path) as writer:
                for k_, v_ in GICs_dict.items():
                    GICs_dict[k_].to_excel(writer, index=False, sheet_name=k_)

            path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_篩選產業_I{}C{}.xlsx'.format(industry_top_selection, company_top_selection))
            with pd.ExcelWriter(path) as writer:
                for k_, v_ in industries_select_dict.items():
                    industries_select_dict[k_].to_excel(writer, index=False, sheet_name=k_)

            path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_持股重新配重_I{}C{}.xlsx'.format(industry_top_selection, company_top_selection))
            with pd.ExcelWriter(path) as writer:
                for k_, v_ in processed_dict.items():
                    processed_dict[k_].to_excel(writer, index=False, sheet_name=k_)


            path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_篩選持股_I{}C{}.xlsx'.format(industry_top_selection, company_top_selection))
            with pd.ExcelWriter(path) as writer:
                for k_, v_ in company_select_dict.items():
                    company_select_dict[k_].to_excel(writer, index=False, sheet_name=k_)
        # self.config_obj.logger.debug('Costomized holdings table shape:{}'.format(customized_table.shape))
        self.config_obj.logger.debug(customized_table)
        return customized_table, None

    def customize_fund_components(self, industry_top_selection, company_top_selection, mcap_weighted_flag):
        '''
        function:
            製作自定義基金。
        Input:
            -.industry_top_selection: int
                各基金取產業市值前幾
            -.company_top_selection: int
                各基金產業，取個股市值前幾
            -.mcap_weighted_flag
                產業、個股是否以市值加權 (mv: market)
        Output:
            仿製基金13F holdings table，包含欄位SYM、SHARES、date、price
        1.2019 2/15 開始進場
        2.XIRR表現在波克夏以上的基金 or 其他選擇方式(-. 單筆獲利計算夏普值排序)
        3.各基金的前(三)市值產業
        4.前(三)市值產業 的前(三)市值股票
        5.以100萬當入場金
        6.用price回推要購買的SHARES，去進場
            SYM、SHARES、date、price
        7.回測計算MDD、XIRR
        '''
        enter_date = self.config_obj.customize_enter_date #2019 2/15 開始進場
        # hedge_funds = self.config_obj.target_hedge_funds_dict['XIRR_output_filter'] #XIRR表現在波克夏以上的基金
        hedge_funds = self.config_obj.target_hedge_funds_dict['sharpe_output_filter'] #計算已平倉獲利後，依照sharpe ratio、勝算比排序
        
        enter_cost = self.config_obj.enter_cost
        '''定義Output obj、統計用obj'''
        ### Final Output Form
        customized_table = None

        '''Read DB data'''
        query = self.create_query_data_table(self.config_obj.hedge_fund_portfolio_table)
        fund_data = self.sql_execute(query)
        fund_data = pd.DataFrame(fund_data)
        fund_data = fund_data[fund_data['HEDGE_FUND'].isin(hedge_funds)]
        count_hedge_funds = fund_data.groupby('QUARTER')['HEDGE_FUND'].nunique().reset_index()
        '''
            QUARTER  HEDGE_FUND
        0   Q1 2014          12
        1   Q1 2015          15
        2   Q1 2016          17
        '''
        
        # hedge_funds = ['Dalal Street Holdings']
        hedge_fund_list = hedge_funds

        # print("總共包含{}個對沖基金資料".format(len(hedge_fund_list)))
        # print('Hedge Funds:', )
        # print(hedge_fund_list)

        '''各hedge fund計算迴圈'''
        for idx, hedge_fund in enumerate(hedge_fund_list):
            '''定義迴圈內參數'''
            # summary_data = [] # 包含項目: {'SYM': , 'SHARES': 依照資金/比例分配後的SHARES, }
            # corr_analysis_table = None 
            # previous_holdings = None
            # xirr_calculate_dict = {'date':[], 'amounts':[]}
            # previous_holdings_time = None
            # previous_sym_str = tuple() #TBD
            
            '''調整fund_data'''
            each_fund_data = self.each_fund_data_adjust(fund_data, hedge_fund)
            quarters_list = each_fund_data['QUARTER'].values
            date_list = each_fund_data['13F_DATE'].values #date_list = each_fund_data['DATE_FILED'].values # 進場時間點使用該基金13F公布時間
            filing_list = each_fund_data['FILING_ID'].values
            # if idx == 0:
            #     base_13F_dates = each_fund_data['BASE_DATE'].values# 進場時間點使用13F公布截止時間
            #     base_13F_date_list = pd.to_datetime(base_13F_dates, unit='ns')
            #     base_13F_date_list = [str(date) for date in base_13F_date_list.tolist()]

            # print(" === === === 第{}個對沖基金：{}，包含{}個季度資料。 ".format(idx+1, hedge_fund, len(quarters_list)))
            # print(each_fund_data)
            '''各Quarter計算迴圈'''
            for idx_q, (quarter, holdings_time, filing_number) in enumerate(zip(quarters_list, date_list, filing_list)):
                '''定義回圈內參數'''
                # hedge_fund_data = {'date': None, '市值': None, '加碼': None, '減碼': None, 'XIRR': None}
                customized_holdings = None
                '''調整holdings_time'''

                holdings_time = self.adjust_holdings_time(holdings_time, self.us_sorted_dates) # 修正為有開市的日期(美股or台股)
                # print("     第{}個季度：{}，時間為{}".format(idx_q+1, quarter, holdings_time))

                query = self.create_query_holdings_with_gics_n_price(hedge_fund, quarter, filing_number, holdings_time)
                holdings_data = self.sql_execute(query)
                holdings_data = pd.DataFrame(holdings_data)
                # holdings_data = self.holdings_data_adjust(holdings_data)

                if len(holdings_data) == 0:
                    pass
                else:
                    top_gics = self.get_top_gics_from_holdings(holdings_data, industry_top_selection)
                    customized_holdings = self.select_company_from_holdings(holdings_data, top_gics, company_top_selection)

                    hedge_num = count_hedge_funds[count_hedge_funds['QUARTER'] == quarter]['HEDGE_FUND'].values[0]
                    # print('該季Hedge Fund數:', hedge_num)
                    customized_holdings = self.calculate_customized_shares(customized_holdings, enter_cost, hedge_num, mcap_weighted_flag)

                if customized_table is None:
                    customized_table = customized_holdings
                else:
                    customized_table = pd.concat([customized_table, customized_holdings], ignore_index=True)
        
        # path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_{}ind_{}com_customized_table_by_hedge.csv'.format(industry_top_selection, company_top_selection))
        # customized_table.to_csv(path, index=False)

        # 整理該hedge的customized table
        
        customized_table_by_stock = self.arrange_customized_table(customized_table)
        # path = os.path.join(self.config_obj.backtest_summary, str(datetime.datetime.now()).split()[0] + '_customized_table_by_stock.csv')
        # customized_table_by_stock.to_csv(path, index=False)

        return customized_table_by_stock, customized_table


    def sql_execute(self, query):

        if self.config_obj.LOCAL_FLAG:
            conn = pymssql.connect(host='localhost', user = 'myfirstjump', password='myfirstjump', database='US_DB')
        else:
            conn = pymssql.connect(host='localhost', user = 'stock_search', password='1qazZAQ!', database='STOCK_SKILL_DB')
        cursor = conn.cursor(as_dict=True)
        cursor.execute(query)
        # data = [row for row in cursor]
        data = []
        for row in cursor:
            data.append(row)
        cursor.close()
        conn.close()
        return data

    def create_query_data_table(self, data_table):
        '''
        其中依照[DATE_FILED]做升續排列。
        '''
        query = '''SELECT * FROM {} WITH(NOLOCK) ORDER BY [DATE_FILED] ASC'''.format(data_table)
        return query

    def get_all_price_date(self, price_table):

        query = '''
        SELECT DISTINCT [date]
        FROM {}
        '''.format(price_table)
        return query

    def each_fund_data_adjust(self, fund_data, hedge_fund, enter_date):
        '''
        function:
            依據條件篩選fund_data中的records，因為每個季度可能包含多個FILE。
                QUARTER    FORM_TYPE  DATE_FILED           FILING_ID                 HEDGE_FUND
            0   Q4 2013       13F-HR  2014-01-31  000090556714000001  Yacktman Asset Management
            1   Q4 2013  RESTATEMENT  2014-02-13  000090556714000002  Yacktman Asset Management
            2   Q1 2014       13F-HR  2014-05-06  000090556714000004  Yacktman Asset Management
        Input:
            -. 原始fund_data(13F報告)
            -. 目前處理的hedge_fund string

        Output:
            調整後之fund_data
                1.新增BASE_DATE欄位，多用於後面查price。
                2.刪除多餘的報告，每季基本上只留下一組資料。
        '''
        # 依據hedge_fund string篩選基金
        # 刪除以下欄位：'HOLDINGS'(持股個數), 'VALUE'(總市值), 'TOP_HOLDINGS'(最高持股量的個股)
        # 重製dataframe index
        df = fund_data[fund_data['HEDGE_FUND']==hedge_fund].drop(['HOLDINGS', 'VALUE', 'TOP_HOLDINGS', ], axis=1)

        #某些狀況要使用13F報告公布日期
        def calculate_13F_date(row):
            year = int(row['QUARTER'].split()[1])
            if 'Q1' in row['QUARTER']:
                return datetime.datetime(year, 5, 15)
            elif 'Q2' in row['QUARTER']:
                return datetime.datetime(year, 8, 14)
            elif 'Q3' in row['QUARTER']:
                return datetime.datetime(year, 11, 14)
            elif 'Q4' in row['QUARTER']:
                return datetime.datetime(year + 1, 2, 14)
        #某些狀況要使用每季最後一天
        def calculate_base_date(row):
            year = int(row['QUARTER'].split()[1])
            if 'Q1' in row['QUARTER']:
                return datetime.datetime(year, 3, 31)
            elif 'Q2' in row['QUARTER']:
                return datetime.datetime(year, 6, 30)
            elif 'Q3' in row['QUARTER']:
                return datetime.datetime(year, 9, 30)
            elif 'Q4' in row['QUARTER']:
                return datetime.datetime(year, 12, 31)
        # 新增欄位BASE_DATE，依據QUARTER欄位的資料，回傳Q1->5/15, Q2->8/14, Q3->11/14, Q4->2/14等日期

        df['13F_DATE'] = df.apply(calculate_13F_date, axis=1)# 新增 '13F DATE' 基準日期欄位 (即5/15, 8/14, 11/14, 2/14)
        df['BASE_DATE'] = df.apply(calculate_base_date, axis=1)# 新增 'BASE DATE' 基準日期欄位 (即3/31, 6/30, 9/30, 12/31)
        df['DATE_FILED'] = pd.to_datetime(df['DATE_FILED'])# 將 'DATE_FILED' 轉換為日期時間格式 (此為確切公告日期)
        
        # 時間篩選
        enter_date = pd.to_datetime(enter_date)
        df['DATE_FILED'] = pd.to_datetime(df['DATE_FILED'])
        df = df[df['DATE_FILED'] >= enter_date].reset_index(drop=True) # 刪除DATE_FILED欄位資料在enter_date以前的records
        df = df[df['BASE_DATE'] >= enter_date].reset_index(drop=True) # 刪除DATE_FILED欄位資料在enter_date以前的records
        

        df = df[~(df['FORM_TYPE'] == 'NEW HOLDINGS')] # 刪除NEW HOLDINGS的Records

        # 刪除 RESTATEMENT 大於 基準日期的Records，但刪除後該季應該至少剩下一筆資料。
        # base_date = df['BASE_DATE'].unique()
        # adjusted_df = pd.DataFrame(columns=['QUARTER','FORM_TYPE','DATE_FILED','FILING_ID','HEDGE_FUND','BASE_DATE',])
        # for each_base_date in base_date:
        #     each_df = df[df['BASE_DATE']==each_base_date]
        #     if len(each_df) == 1:
        #         pass
        #     else:
        #         each_df = each_df[~((each_df['FORM_TYPE'] == 'RESTATEMENT') & (df['DATE_FILED'] > df['BASE_DATE']))]

        '''有時只有RESTATEMENT資料，故下式先移除'''
        # df = df[~((df['FORM_TYPE'] == 'RESTATEMENT') & (df['DATE_FILED'] > df['BASE_DATE']))] # 刪除 RESTATEMENT 大於 基準日期的Records。


        sorting_key = df.groupby('BASE_DATE')['FILING_ID'].idxmax() # 取group中最大FILING_ID，因為可能有多筆records，直接依據FILING_ID取最新的。
        df = df.loc[sorting_key].reset_index(drop=True)

        return df
    def adjust_holdings_time(self, holdings_time, sorted_dates, next_day=True):
        '''
        function:
            在輸入時間點為13F報告公布時間時，該日不一定有開市，所以依據時間調整。
        input:
            -. holdings_time(string):  該季資料之報告日期
            -. sorted_dates(pd.Series(pd.datetime)):  price data所有日期，即有開市日期
        '''
        index = sorted_dates.searchsorted(holdings_time) # 找到日期在排序後的列表中的位置
        adjust_date  = sorted_dates[index] if index < len(sorted_dates) else sorted_dates[-1] # 如果日期正好在列表中，返回該日期；否則返回下一個最接近的日期
        # print('原始日期:', holdings_time)
        # print('index: ', index)
        # print('修正日期:', adjust_date)
        # print(sorted_dates[index-1], sorted_dates[index], sorted_dates[index+1], )
        '''依照實際情況，13F報告公布後隔天買入，故應使用index+1日(sorted_dates[index+1])；而若本來就沒有開市，則使用下個開市日(adjust_date)'''
        if next_day:
            if holdings_time != adjust_date:
                result_date = adjust_date
            else:
                result_date = sorted_dates[index+1]
        else:
            result_date = adjust_date
        # print('使用日期:', result_date)
        return result_date    
    def create_query_holdings(self, fund, quarter, filing_number):
        '''
        依據fund和quarter篩選holdings資料表的query語句
        '''
        data_table = self.config_obj.holdings_data_table
        query = '''SELECT * FROM {} WITH(NOLOCK) 
        WHERE [HEDGE_FUND] = '{}' 
        AND [QUARTER] = '{}' 
        AND [FILING_ID] = '{}'
        AND [OPTION_TYPE] IS NULL
        AND SUBSTRING([CUSIP], 7, 2) = '10'
        AND [SHARES] IS NOT NULL 
        '''.format(data_table, fund, quarter, filing_number)
        return query
    def holdings_data_adjust(self, df):
        '''
        function:
            依據SYM進行groupby，因為13F報告中，同一SYM可能會列舉多筆。故依據SYM加總VALUE、Percentile、SHARES等三個欄位數值(預先轉換為Numeric確保可加性。
        input:
            holdings_data
        output:
            adjusted holdings_data
        '''
        df['VALUE'] = pd.to_numeric(df['VALUE'], errors='coerce')  # 将VALUE列转换为数值，将无法转换的值设为NaN
        df['Percentile'] = pd.to_numeric(df['Percentile'], errors='coerce')  # 将Percentile列转换为数值，将无法转换的值设为NaN
        df['SHARES'] = pd.to_numeric(df['SHARES'], errors='coerce')  # 将SHARES列转换为数值，将无法转换的值设为NaN

        df = df.groupby('SYM', as_index=False).agg({'VALUE':'sum', 'Percentile':'sum', 'SHARES':'sum'})
        return df
    
    def holdings_data_adjust_for_customized_fund(self, df):
        '''
        function:
            同上，但來源為customized fund holdings。
        '''
        df = df.rename(columns={"shares_to_buy": "SHARES"})
        df = df.drop(['QUARTER', 'date', 'price', 'suggested_invest_amount'], axis=1)
        df['SHARES'] = pd.to_numeric(df['SHARES'], errors='coerce')  # 将SHARES列转换为数值，将无法转换的值设为NaN
        return df
    
    def shares_difference_between_quarters(self, previous_holdings, holdings_data):

        previous_holdings = previous_holdings[['SYM', 'SHARES']]
        holdings_data = holdings_data[['SYM', 'SHARES']]

        # 將兩個資料表合併
        merged_data = holdings_data.merge(previous_holdings, on=['SYM'], how='outer', suffixes=('_current', '_previous'))
        # merged_data['SHARES_current'].fillna(0, inplace=True)
        # merged_data['SHARES_previous'].fillna(0, inplace=True)
        merged_data.fillna({'SHARES_current': 0, 'SHARES_previous': 0}, inplace=True)
        merged_data = merged_data.astype({'SHARES_current': int, 'SHARES_previous': int})
        # 計算持股數量變化
        merged_data['SHARES_CHANGE'] = merged_data['SHARES_current'] - merged_data['SHARES_previous']
        return merged_data
    def create_query_get_open_price_by_join_holdings_n_price(self, SYMs_tuple, date, fund, quarter, filing_number):
        '''
        依據holdings去查表price，透過stock_id(即holdings表中的SYM) join兩張表格，並加入SHARES資訊至price表。
        '''
        price_table = self.config_obj.us_stock_price_table
        holdings_table = self.config_obj.holdings_data_table
        query = ''' SELECT DISTINCT tb_price.[date], tb_price.[stock_id], tb_price.[Open], tb_holdings.[SHARES]
            FROM {} tb_price WITH(NOLOCK)
			INNER JOIN {} tb_holdings WITH(NOLOCK) on tb_price.stock_id = tb_holdings.SYM
			WHERE tb_price.[stock_id] IN {}
            AND tb_price.[date] = '{}'
            AND tb_holdings.[HEDGE_FUND] = '{}' 
            AND tb_holdings.[QUARTER] = '{}' 
            AND tb_holdings.[FILING_ID] = '{}'
            AND tb_holdings.[OPTION_TYPE] IS NULL
            AND tb_holdings.[SHARES] IS NOT NULL
            AND SUBSTRING(tb_holdings.[CUSIP], 7, 2) = '10'
            '''.format(price_table, holdings_table, SYMs_tuple, date, fund, quarter, filing_number)
        return query
    
    def create_query_get_open_price_for_customized_fund(self, sym_str, holdings_time):

        price_table = self.config_obj.us_stock_price_table
        query = '''
        SELECT [date], [stock_id] SYM, [Open] FROM {} WITH(NOLOCK)
        WHERE [stock_id] IN {}
        AND [date] = '{}'
        '''.format(price_table, sym_str, holdings_time)
        return query

    def create_query_get_open_price_by_date_n_sym(self, SYMs_tuple, date):
        '''
        function:
            建立query string: 以SYM字串以及date查詢price table裡面的開盤價。
        input:
            -. SYM tuple
            -. date
        output:
            query(string)
        '''
        price_table = self.config_obj.us_stock_price_table
        query = ''' SELECT [date], [stock_id] SYM, [Open]
            FROM {} WITH(NOLOCK) WHERE [date] = '{}' AND [stock_id] IN {}
            '''.format(price_table, date, SYMs_tuple)
        return query

    def calculate_scaling_in_and_out(self, merged_data, price_data):
        ''''''
        scaling_in = {}
        scaling_out = {}
        scaling_even = {}
        market_value = 0
        scaling_data = merged_data.merge(price_data, on=['SYM'])

        # 根據持股數量變化分類
        for index, row in scaling_data.iterrows():
            stock_id = row['SYM']
            shares_change = row['SHARES_CHANGE']
            Open_current = row['Open']
            SHARES_current = row['SHARES_current']

            if shares_change > 0:
                scaling_in[stock_id] = shares_change * Open_current
            elif shares_change < 0:
                scaling_out[stock_id] = abs(shares_change) * Open_current
            else:
                scaling_even[stock_id] = 0
            market_value = market_value + SHARES_current * Open_current
        # print("scaling_in")
        # print(scaling_in)
        # print("scaling_out")
        # print(scaling_out)
        # print("scaling_even")
        # print(scaling_even)
        return market_value, scaling_in, scaling_out, scaling_even
    def get_price_change_ratio(self, previous_holdings_time, holdings_time, sym_str_combined):
        '''
        計算本季度與前一季度持股漲跌幅度
        '''
        if len(sym_str_combined) == 1:
            sym_str_combined = str(sym_str_combined)
            sym_str_combined = sym_str_combined.replace(',', '')
        query = self.create_query_get_open_price_by_date_n_sym(sym_str_combined, previous_holdings_time)
        previous_price_data = self.sql_execute(query)
        previous_price_data = pd.DataFrame(previous_price_data)
        query = self.create_query_get_open_price_by_date_n_sym(sym_str_combined, holdings_time)
        price_data = self.sql_execute(query)
        price_data = pd.DataFrame(price_data)

        # 將兩個資料表合併
        merged_data = price_data.merge(previous_price_data, on=['SYM'], how='outer', suffixes=('_current', '_previous'))

        # merged_data['SHARES_current'].fillna(0, inplace=True)
        # merged_data['SHARES_previous'].fillna(0, inplace=True)
        # merged_data = merged_data.astype({'SHARES_current': int, 'SHARES_previous': int})
        # 計算price change ratio
        merged_data['price_change_ratio'] = (merged_data['Open_current'] - merged_data['Open_previous']) / merged_data['Open_previous']
        
        return merged_data
    def arrange_corr_analysis_data(self, price_ratio_data, shares_data):
        '''
        function:

        input:
            -. price_ratio_data
                date_current    SYM  Open_current date_previous  Open_previous  price_change_ratio
            -. shares_data
                SYM  SHARES_current  SHARES_previous  shares_change
        '''
        merged_data = price_ratio_data.merge(shares_data, on=['SYM'], how='outer')
        merged_data['scaling'] = merged_data['SHARES_CHANGE'] * merged_data['Open_current']
        merged_data = merged_data.dropna() #TBD: NA部分為SYM在price表中查不到資料者。
        # print(merged_data)
        '''
        merged data columns:
            date_current    SYM  Open_current date_previous  Open_previous  price_change_ratio  SHARES_current  SHARES_previous  shares_change       scaling
        '''
        # merged_data = merged_data[['SYM', 'price_change_ratio', 'scaling']]
        merged_data = merged_data[['price_change_ratio', 'scaling']]
        return merged_data
    def market_value_by_join_holdings_and_price(self, holdings_data, price_data):
        '''
        function:
            計算股票市值，藉由holdings_data:SHARES、price_data:Open相乘來計算市值，並加總。
        input:
            -. holdings_data
            -. price_data
        output:
            -. market_value
        '''
        market_value = 0
        merged_data = holdings_data.merge(price_data, on=['SYM'])
        # print('Holdings & Price:')
        # print(merged_data)

        for index, row in merged_data.iterrows():
            shares = row['SHARES']
            Open_current = row['Open']
            market_value = market_value + shares * Open_current
        return market_value
    
    def calculate_XIRR(self, data, holdings_time, market_value):
        '''
        將data資料加上最新時間/市值兌現，計算XIRR值。
        '''
        data['date'].append(holdings_time)
        data['amounts'].append(market_value)
        f = lambda d: d.date() if isinstance(d, pd._libs.tslibs.timestamps.Timestamp) else d
        python_date = [f(d) for d in data['date']]
        amounts = [int(a) for a in data['amounts']]
        result = xirr(python_date, amounts)
        # x = {'date':python_date, 'amounts':amounts}
        # print(pd.DataFrame.from_dict(x))
        # print("XIRR:", result)
        return result

    def summary_statistical_calculates(self, hedge_summary):
        # 新增欄位 A 為 [加碼-減碼]
        hedge_summary['淨投入額'] = hedge_summary['加碼'] - hedge_summary['減碼']

        # 新增欄位 為 A/上一個row的市值
        hedge_summary['淨投入額占比'] = hedge_summary['淨投入額'] / hedge_summary['市值'].shift(1)
        return hedge_summary
   
    def individual_stock_summary(self, original_date_list, market, sym_str):
        '''
        function:
            依據13F報告時間點，計算投資某個股的XIRR加入進行比較。
            由於TWS與US的開市時間不同，包裝一個函數能夠修改original_date_list內元素，調整為有開市的時間，才找得到price。
        input:
            -. original_date_list: 包含13F各報告節點的截止時間，設定為買進個股的時間點。
            -. market(string): 時間要調整到的目標市場 ex. 'tw', 'us'
            -. sym_str: 該個股股票代碼
        output:
            individual_summary(pd.DataFrame): {'date': holdings_time, '市值': price, '加碼': 0, '減碼': 0, 'XIRR': xirr, '淨投入額': 0, '淨投入額占比': 0, }
        '''
        summary_data = []
        individual_data = {'date': None, '市值': None, '加碼': None, '減碼': None, 'XIRR': None, '淨投入額': None, '淨投入額占比': None, }

        if market == 'tw':
            price_table = self.config_obj.tw_stock_price_table
        elif market == 'us':
            price_table = self.config_obj.us_stock_price_table
        else:
            price_table = self.config_obj.us_stock_price_table

        adjusted_date_list = self.adjust_date_str_for_market(original_date_list, market=market) # 台股與美股開市時間差異
        data_date_str = tuple(adjusted_date_list)
        query = self.create_query_stock_price_data(price_table, sym_str, data_date_str)
        price_data = self.sql_execute(query)
        price_data = pd.DataFrame(price_data)
        xirr_calculate_dict = {'date':[], 'amounts':[]}
        for index, row in price_data.iterrows():
            holdings_time = row['date']
            price = row['Open']
            if index == 0:
                xirr_calculate_dict['date'].append(holdings_time)
                xirr_calculate_dict['amounts'].append(-price)
            else:
                xirr_calculate_dict['date'].append(holdings_time)
                xirr_calculate_dict['amounts'].append(0)
            temp_xirr_calculate_dict = copy.deepcopy(xirr_calculate_dict)
            if index == 0: # 第一季直接帶pyxirr公式計算結果為10%，沒有研究計算公式，故直接assign 0。
                xirr = 0
            else:
                xirr = self.calculate_XIRR(temp_xirr_calculate_dict, holdings_time, price)
            individual_data = {'date': holdings_time, '市值': price, '加碼': 0, '減碼': 0, 'XIRR': xirr, '淨投入額': 0, '淨投入額占比': 0, }
            summary_data.append({'hedge_fund': str(sym_str), **individual_data})
        individual_summary = pd.DataFrame(summary_data)
        return individual_summary
    
    def customized_fund_stock_summary(self, plan_name, customized_fund_data):
        summary_data = []
        previous_holdings = None
        xirr_calculate_dict = {'date':[], 'amounts':[]}
        previous_holdings_time = None
        previous_sym_str = tuple() #TBD
        fund_data = {'date': None, '市值': None, '加碼': None, '減碼': None, 'XIRR': None, '淨投入額': None, '淨投入額占比': None, }
        quarters_list = customized_fund_data['QUARTER'].drop_duplicates().values
        date_list = customized_fund_data['date'].drop_duplicates().values

        '''2.3. 各Quarter計算迴圈'''
        for idx_q, (quarter, holdings_time) in enumerate(zip(quarters_list, date_list)):
            '''2.3.1 定義回圈內參數'''
            fund_data = {'date': None, '市值': None, '加碼': None, '減碼': None, 'XIRR': None, '淨投入額': None, '淨投入額占比': None, }

            holdings_data = customized_fund_data[customized_fund_data['QUARTER']==quarter]
            holdings_data = self.holdings_data_adjust_for_customized_fund(holdings_data)
            
            '''
                SYM  SHARES
            0   AMT    1609
            1   CCI    1860
            2   FLR    1922

            '''
            if idx_q > 0: #扣除第一季，每季要計算的內容
                shares_data = self.shares_difference_between_quarters(previous_holdings, holdings_data)
                sym_str = shares_data['SYM'].dropna().values
                
                # print("SYMs:", sym_str)
                if len(sym_str) == 1:
                    sym_str = str(sym_str)
                    sym_str = sym_str.replace('[', '(')
                    sym_str = sym_str.replace(']', ')')
                else:
                    sym_str = tuple(sym_str)
                
                query = self.create_query_get_open_price_by_date_n_sym(sym_str, holdings_time)
                price_data = self.sql_execute(query)
                price_data = pd.DataFrame(price_data)
                # print('price_data:')
                # print(price_data)
                '''
                        date   SYM    Open
                    0   2016-11-14  AAPL   26.93
                    1   2016-11-14   ALL   69.76
                    2   2016-11-14    AY   16.98
                    3   2016-11-14   BAC   19.41
                '''
                market_value, scaling_in, scaling_out, scaling_even = self.calculate_scaling_in_and_out(shares_data, price_data)
                # print("Shares Increased:", scaling_in)
                # print("Shares Decreased:", scaling_out)
                # print("Shares Unchanged:", scaling_even)
                scaling_in_sum = sum([i for i in  scaling_in.values()])
                scaling_out_sum = sum([i for i in  scaling_out.values()])
                
                xirr_calculate_dict['date'].append(holdings_time)
                xirr_calculate_dict['amounts'].append(-(scaling_in_sum - scaling_out_sum))

                ### correlation analysis TBD
                current_sym_str = holdings_data['SYM'].dropna().values 
                current_sym_str = tuple(current_sym_str)
                # print("SYMs:", current_sym_str)            
                # print("previous_sym_str:", previous_sym_str)
                sym_str_combined = set(previous_sym_str) | set(current_sym_str)
                sym_str_combined = tuple(sym_str_combined)
                
            else: #第一季要計算的內容

                current_sym_str = holdings_data['SYM'].dropna().values # TBD: 確認Drop數量
                if len(current_sym_str) == 1:
                    current_sym_str = str(current_sym_str)
                    current_sym_str = current_sym_str.replace('[', '(')
                    current_sym_str = current_sym_str.replace(']', ')')
                else:
                    current_sym_str = tuple(current_sym_str)
                query = self.create_query_get_open_price_by_date_n_sym(current_sym_str, holdings_time)
                price_data = self.sql_execute(query)
                price_data = pd.DataFrame(price_data)
                market_value = self.market_value_by_join_holdings_and_price(holdings_data, price_data)

                scaling_in_sum = 0
                scaling_out_sum = 0
                xirr_calculate_dict['date'].append(holdings_time)
                xirr_calculate_dict['amounts'].append(-market_value)
            '''2.3.5 以xirr_calculate_dict計算XIRR值'''
            temp_xirr_calculate_dict = copy.deepcopy(xirr_calculate_dict)
            if idx_q == 0: # 第一季直接帶pyxirr公式計算結果為10%，沒有研究計算公式，故直接assign 0。
                xirr = 0
            else:
                xirr = self.calculate_XIRR(temp_xirr_calculate_dict, holdings_time, market_value)
            '''2.3.6 將本季度holdings data/price_data暫存，作為下一季度計算使用。'''
            previous_holdings = holdings_data.copy()
            previous_holdings_time = holdings_time
            previous_sym_str = current_sym_str # correlation analysis使用
            '''2.3.7 將統計數值回存至summary_data'''
            fund_data = {'date': holdings_time, '市值': market_value, '加碼': scaling_in_sum, '減碼': scaling_out_sum, 'XIRR':xirr}
            summary_data.append({'hedge_fund': plan_name, **fund_data})
        
        
        holdings_time = self.max_date#'2024-03-06'#self.max_date # 可以自訂，此處以DB中最大有交易日期為主(2024-01-09)

        # query = self.create_query_get_open_price_by_join_holdings_n_price(sym_str, holdings_time, hedge_fund, quarter, filing_number)
        # price_data = self.sql_execute(query)
        # price_data = pd.DataFrame(price_data)
        query = self.create_query_get_open_price_for_customized_fund(current_sym_str, holdings_time)
        price_data = self.sql_execute(query)
        price_data = pd.DataFrame(price_data)
        price_data = price_data.merge(holdings_data, on=['SYM'], how='left')

        market_value = sum(price_data['Open'] * price_data['SHARES'])
        xirr = self.calculate_XIRR(xirr_calculate_dict, holdings_time, market_value)
        hedge_fund_data = {'date': holdings_time, '市值': market_value, '加碼': 0, '減碼': 0, 'XIRR':xirr}
        summary_data.append({'hedge_fund': plan_name, **hedge_fund_data})
        customized_fund_summary = pd.DataFrame(summary_data)
        customized_fund_summary = self.summary_statistical_calculates(customized_fund_summary)

        return customized_fund_summary
    def adjust_date_str_for_market(self, base_13F_date_list, market):
        adjusted_data_list = []
        if market == 'tw':
            soruce_date = self.tws_sorted_dates
        elif market == 'us':
            soruce_date = self.us_sorted_dates
        else:
            soruce_date = self.us_sorted_dates
        
        for date in base_13F_date_list:
            index = soruce_date.searchsorted(date)
            adjust_date  = soruce_date[index] if index < len(soruce_date) else soruce_date[-1] # 如果日期正好在列表中，返回該日期；否則返回下一個最接近的日期
            adjusted_data_list.append(str(adjust_date))
        return adjusted_data_list

    def create_query_stock_price_data(self, price_table, sym_str, date_str):
        '''
        依據price table查詢股價
        '''
        query = '''
        SELECT [date],[stock_id],[Open]
        FROM {} WHERE stock_id = '{}' AND [date] IN {} 
        '''.format(price_table, sym_str, date_str)
        return query

    def create_query_holdings_with_gics_n_price(self, hedge_fund, quarter, filing_number, holdings_time):

        holdings_table = self.config_obj.holdings_data_table
        gics_table = self.config_obj.us_stock_gics_table
        price_table = self.config_obj.us_stock_price_table
        query = '''
        SELECT tb_hedge.[SYM], tb_hedge.[SHARES], tb_hedge.[HEDGE_FUND], tb_hedge.[QUARTER], tb_hedge.[GICS], tb_price.[date] date, tb_price.[Open] price FROM
        (SELECT tb_holdings.[SYM], tb_holdings.[SHARES], tb_holdings.[HEDGE_FUND], tb_holdings.[QUARTER], tb_gics.[GICS_2_digit] GICS
        FROM {} tb_holdings WITH(NOLOCK)
        INNER JOIN {} tb_gics WITH(NOLOCK)
        ON tb_holdings.SYM = tb_gics.Ticker 
        WHERE 
            tb_holdings.[HEDGE_FUND] = '{}' 
            AND tb_holdings.[QUARTER] = '{}' 
            AND tb_holdings.[FILING_ID] = '{}'
            AND tb_holdings.[OPTION_TYPE] IS NULL
            AND tb_holdings.[SHARES] IS NOT NULL
            AND SUBSTRING(tb_holdings.[CUSIP], 7, 2) = '10'
            ) tb_hedge
        INNER JOIN {} tb_price WITH(NOLOCK) 
        ON tb_hedge.SYM = tb_price.stock_id 
        WHERE tb_price.[date] = '{}'
        '''.format(holdings_table, gics_table, hedge_fund, quarter, filing_number, price_table, holdings_time)
        return query

    def get_top_gics_from_holdings(self, holdings_data, industry_top_selection):
        '''
        function:
            依據GICs去計算Hedge持股的產業分布，列舉出前(industry_top_selection)市值高的產業。
        input:
            -. holdings_data:
            -. industry_top_selection: int
        output:
            -. top_gics: list
                前(industry_top_selection)產業list，如['20' '25' '60']。
        '''
        if len(holdings_data) == 0:
            return None
        # top_gics = []
        holdings_data['market_price'] = holdings_data['price'] * holdings_data['SHARES']
        df = holdings_data.groupby('GICS', as_index=False).agg({'market_price':'sum'})
        df = df.sort_values('market_price', ascending=False)
        top_gics = df['GICS'][:industry_top_selection].values
        # print(top_gics)
        return top_gics

    def group_by_GICs_from_holdings_data(self, holdings_data):
        '''
        FUNCTION:
            1. holdings_data新增欄位
                holdings_data新增market_price、WEIGHTS、WEIGHTS_diff
            2. 建置gics_df
                gics_df是group by holdings_data on GICS，欄位包含['GICS','INDUSTRY', 'WEIGHTS', 'WEIGHTS_diff','market_price','HEDGE_FUND','QUARTER','date',]
        INPUT:
            -. holdings_data: pd.DataFrame
        OUTPUT:
            -. holdings_data: pd.DataFrame
            -. gics_df: pd.DataFrame
        '''

        # holdings_data新增欄位：計算個股市值(market_price) & 對應的權重(WEIGHTS)
        holdings_data['market_price'] = holdings_data['price'] * holdings_data['SHARES']
        company_market_values = holdings_data['market_price'].sum()
        holdings_data['WEIGHTS'] = holdings_data['market_price'] / company_market_values
        
        # holdings_data新增欄位：加入各GICs英文名稱欄位(INDUSTRY)
        def get_GICs_industry_name(row):
            return self.config_obj.gics_dict[row['GICS']]
        holdings_data['INDUSTRY'] = holdings_data.apply(get_GICs_industry_name, axis=1)

        # holdings_data新增欄位：以市值排序並生成WEIGHTS_diff欄位
        holdings_data = holdings_data.sort_values('market_price', ascending=False)
        holdings_data['WEIGHTS_diff'] = holdings_data['WEIGHTS'].diff()

        #新增DataFrame：Group by產業別，產製新的DataFrame(gics_df)，並計算產業市值(SUM UP market_price)
        gics_df = holdings_data.groupby('GICS', as_index=False).agg({'INDUSTRY':'first', 'market_price':'sum', 'HEDGE_FUND':'first', 'QUARTER':'first', 'date':'first'})
        gics_df = gics_df.sort_values('market_price', ascending=False)

        # gics_df新增欄位：計算產業市值(market_price) & 對應的權重(WEIGHTS)
        # gics_df新增欄位：生成WEIGHTS_diff欄位
        industry_market_values = gics_df['market_price'].sum()
        gics_df['WEIGHTS'] = gics_df['market_price'] / industry_market_values
        gics_df['WEIGHTS_diff'] = gics_df['WEIGHTS'].diff()
        gics_df = gics_df[['GICS','INDUSTRY', 'WEIGHTS', 'WEIGHTS_diff','market_price','HEDGE_FUND','QUARTER','date',]]

        return holdings_data, gics_df
    
    def select_company_from_holdings(self, holdings_data, top_gics, company_top_selection):
        '''
        function:
            依據top GICs，去篩選holdings_data。列篩選前(company_top_selection)市值高的公司，輸出table。
        input:
            -. holdings_data: df
            -. top_gics: list
            -. company_top_selection: int
        output:
            customized_holdings: df
        '''
        customized_holdings = holdings_data[holdings_data['GICS'].isin(top_gics)]
        customized_holdings = customized_holdings.sort_values(by=['GICS', 'market_price'], ascending=False)
        # print(customized_holdings)
        customized_holdings = customized_holdings.groupby('GICS').apply(lambda x: x.nlargest(company_top_selection, 'market_price')).reset_index(drop=True)
        return customized_holdings
    
    def select_company_from_holdings_adjusted(self, holdings_data, holdings_GICs_data, industry_top_selection, company_top_selection):
        '''
        FUNCTION:
            依據3個邏輯進行產業篩選：
            1. 剔除WEIGHTS_diff小於-30%的產業，即市值不能差前面的太多。(weights_diff_filter())
            2. 加入WEIGHTS_diff大於-2%的產業，即市值如果只差前面一個產業2%以內，就納入。(weights_diff_addition())
            3. 取前industry_top_selection數量的產業。
                先執行(1)，再執行(3)，最後執行(2)，獲得selected_industries

            依據2個邏輯進行產業篩選：
            1. 依據selected_industries，篩選holdings_data裡面的產業(GICs)。
            2. (FOR迴圈)每個產業，依市值計算個股的WEIGHTS、WEIGHTS_diff
                依照上面產業篩選的3個邏輯進行 [產業內的個股] 篩選。
                先執行(1)，再執行(3)，最後執行(2)，獲得company_result_data
            
            篩選出高價股，以利後續刪除(現在刪除的話，若刪除後剩下Empty dataframe會使得enter_cost在calculate_customized_shares執行時分配不均)

        INPUT:
            -. holdings_data, 
            -. holdings_GICs_data, 
            -. industry_top_selection, 
            -. company_top_selection
        OUTPUT:
            -. processed_data, 
            -. industry_result_data, 
            -. company_result_data
        '''
        holdings_data = holdings_data.reset_index(drop=True)
        holdings_GICs_data = holdings_GICs_data.reset_index(drop=True)
        # print('holdings_GICs_data', holdings_GICs_data)
        '''1. 產業篩選'''
        def weights_diff_filter(data): #Top1若 > Top2 30%以上，則不繼續取Top2，依此類推
            indexes = []
            for index, row in data.iterrows(): 
                if index == 0: #情況一: 第一筆資料，納入
                    indexes.append(index)
                    pass
                elif row['WEIGHTS_diff'] > -0.3: #情況二: 後續資料都要 > -30% 才納入
                    indexes.append(index)
                else:   #情況三: 有發生 < -30% 就break
                    break
            return indexes
        industry_weights_diff_filter_idx = weights_diff_filter(holdings_GICs_data)
        # print('industry_weights_diff_filter_idx', industry_weights_diff_filter_idx)
        def weights_diff_addition(data): #Top1~3之外，若第4名差距<2% --> 納入，依此類推
            marked_indexes = []
            for index, row in data.iterrows():
                if row['WEIGHTS_diff'] > -0.02:
                    marked_indexes.append(index)
            return marked_indexes
        industry_weights_diff_addition_idx = weights_diff_addition(holdings_GICs_data)
        # print('industry_weights_diff_addition_idx', industry_weights_diff_addition_idx)
        #捨棄與前面差距30%以上的產業
        filtered_data = holdings_GICs_data.loc[industry_weights_diff_filter_idx]
        #套用自訂參數，選擇前industry_top_selection
        top_filtered_data = filtered_data.iloc[0:industry_top_selection,:]

        # print('top_filtered_data', top_filtered_data)
        #加入與前項差距小於2%的產業
        from_idx = len(top_filtered_data)
        addition_indexes = []
        for index, row in holdings_GICs_data.iterrows():
            if (index >= from_idx) & (index in industry_weights_diff_addition_idx):
                addition_indexes.append(index)
            elif (index in top_filtered_data.index):
                pass
            else:
                break
        addtion_rows = holdings_GICs_data.loc[addition_indexes]
        industry_result_data = pd.concat([top_filtered_data, addtion_rows], ignore_index=True)
        # print('industry_result_data', industry_result_data)
        selected_industries = industry_result_data['GICS'].values

        '''2. 公司篩選'''
        company_result_data = None
        processed_data = None

        for ind in selected_industries:
            # print('產業', ind)
            sub_data = holdings_data[holdings_data['GICS'] == ind].copy() # 前一步驟所篩選的產業
            sub_data = sub_data.reset_index(drop=True)

            #重新計算占比
            company_market_values = sub_data['market_price'].sum()
            sub_data['WEIGHTS'] = sub_data['market_price'] / company_market_values
            sub_data['WEIGHTS_diff'] = sub_data['WEIGHTS'].diff()
            if processed_data is None:
                processed_data = sub_data
            else:
                processed_data = pd.concat([processed_data, sub_data], ignore_index=True)

            company_weights_diff_filter_idx = weights_diff_filter(sub_data)
            company_weights_diff_addition_idx = weights_diff_addition(sub_data)
            # print('company_weights_diff_filter_idx')
            # print(company_weights_diff_filter_idx)
            # print('company_weights_diff_addition_idx')
            # print(company_weights_diff_addition_idx)
            #捨棄與前面差距30%以上的公司
            filtered_data = sub_data.loc[company_weights_diff_filter_idx]
            #套用自訂參數，選擇前company_top_selection
            top_filtered_data = filtered_data.iloc[0:company_top_selection,:]
            #加入與前項差距小於2%的公司
            from_idx = len(top_filtered_data)
            addition_indexes = []
            for index, row in sub_data.iterrows():
                if (index >= from_idx) & (index in company_weights_diff_addition_idx):
                    addition_indexes.append(index)
                elif (index in top_filtered_data.index):
                    pass
                else:
                    break
            # print('addition_indexes')
            # print(addition_indexes)
            addtion_rows = sub_data.loc[addition_indexes]
            sub_result_data = pd.concat([top_filtered_data, addtion_rows], ignore_index=True)
            # print('sub_result_data')
            # print(sub_result_data)

            if company_result_data is None:
                company_result_data = sub_result_data
            else:
                company_result_data = pd.concat([company_result_data, sub_result_data], ignore_index=True)
        
        # 篩選出高價股
        # filtered_result_data = company_result_data[company_result_data['price_13F'] >= 1000].copy()
        # if filtered_result_data.empty:
        #     pass
        # else:
        #     print('company_result_data', company_result_data)
        #     print('filtered_result_data', filtered_result_data)
        return processed_data, industry_result_data, company_result_data
    
    def calculate_customized_shares(self, data, enter_cost, hedge_num, mcap_weighted_flag=True):
        '''
        FUNCTION:
            依據投資時間(即13F報告時間)，進行篩選額外操作(若需要，例如高價股刪除)，將資金分配至個股，並計算建議購買股數(shares_to_buy)。
            0. 依據價格篩選個股(2024-06-15:由於有些股價過高，造成資金分配後無法購買)
            1. 計算13F報告時間市值。
            2. (FOR迴圈)依據產業分配資金，(FOR迴圈)再依據產業內個股分配資金
            3. customized_holdings新增欄位: investment_amount投資金額
            4. customized_holdings新增欄位: shares_to_buy建議購買股數
        INPUT:
            -. customized_holdings: pd.DataFrame
                ['SYM', 'SHARES', 'HEDGE_FUND', 'QUARTER', 'GICS', 'date', 'price', 'date_13F', 'price_13F', 'market_price', 'WEIGHTS', 'INDUSTRY', 'WEIGHTS_diff']
            -. enter_cost: int
                入場資金
            -. hedge_num: int
                參考的基金數量，用來分配資金
            -. mcap_weighted_flag
        OUTPUT:
            -. customized_holdings: pd.DataFrame
                更新後的customized_holdings
        '''
        customized_holdings = data[['SYM', 'SHARES', 'HEDGE_FUND', 'QUARTER', 'GICS', 'date_13F', 'price_13F', ]].copy()
        # market price是季末(date)的市值，用來篩選產業、公司；而date_13F是相隔1.5個月的時間，應該計算新的市值market price 13F date
        customized_holdings.loc[:, 'market_price_13F_date'] = customized_holdings['SHARES'] * customized_holdings['price_13F']
        if mcap_weighted_flag:# 計算每個持股的投資金額(方法1. 加權分配)
            industry_market_values = customized_holdings.groupby('GICS')['market_price_13F_date'].sum()
            weights = industry_market_values / industry_market_values.sum()
            industry_allocation = enter_cost / hedge_num * weights #各產業依照比重分配資金
            # 根据每个GICS的市值分配資金到每支股票
            for gics, allocation in industry_allocation.items():
                subset = customized_holdings[customized_holdings['GICS'] == gics].copy()
                total_market_value = subset['market_price_13F_date'].sum()
                subset['investment_amount'] = allocation * (subset['market_price_13F_date'] / total_market_value)
                customized_holdings.loc[customized_holdings['GICS'] == gics, 'investment_amount'] = subset['investment_amount']
        else:# 計算每個持股的投資金額(方法2. 平均分配)
            customized_holdings['investment_amount'] = enter_cost / hedge_num / len(customized_holdings)
        customized_holdings['shares_to_buy'] = customized_holdings['investment_amount'] / customized_holdings['price_13F']

        # 取整數
        # na_inf_records = customized_holdings[customized_holdings['shares_to_buy'].isna() | np.isinf(customized_holdings['shares_to_buy'])]
        # print(na_inf_records)
        customized_holdings['shares_to_buy'] = customized_holdings['shares_to_buy'].replace([np.inf, -np.inf], np.nan)
        customized_holdings['shares_to_buy'] = customized_holdings['shares_to_buy'].fillna(0).astype(int)
        customized_holdings['shares_to_buy'] = customized_holdings['shares_to_buy'].astype(int) #取整數

        customized_holdings['shares_origin'] = customized_holdings['shares_to_buy'].copy()

        return customized_holdings

    def individual_stock_filtering(self, q_customized_table, upper_price_limit):
        '''
        FUNCTION:
            針對每季篩選出的數據(個股)進行額外篩選，並且重新分配多出的資金給其他個股
            (internal表示分給該基金內其他個股；external表示分給該基金以外的其他基金個股)
        INPUT:
            q_customized_table: pd.DataFrame
                篩選前的df
        OUTPUT:
            data: pd.DataFrame
                篩選後的df
        '''
        
        data = q_customized_table[['SYM', 'QUARTER', 'HEDGE_FUND','date_13F', 'price_13F', 'investment_amount', 'shares_to_buy', 'shares_origin']].copy()
        data['invest_adj'] = data['investment_amount']
        data['shares_adj'] = 0
        hedge_list = data['HEDGE_FUND'].unique()
        # 多的資金紀錄，internal表示分給該基金內其他個股；external表示分給該基金以外的其他基金個股
        extra_cost_dict = {
            'internal': [], # 因為要區隔每個基金，list裡面預計放{hedge名字: 多的分配金額}
            'external': 0
        }

        # 針對本季每個hedge，去分析幾個保留、幾個濾除，並記錄多的資金分配額度。
        # 針對本季每個hedge，去分析幾個保留、幾個濾除，並記錄多的資金分配額度。
        final_kept_data = pd.DataFrame()  # 儲存保留的records
        final_delete_data = pd.DataFrame()  # 儲存濾除的records

        for idx, each_hedge in enumerate(hedge_list):
            # print('The {} hedge: {} {}'.format(idx, each_hedge, data['QUARTER'].values[0]))
            sub_data = data[data['HEDGE_FUND'] == each_hedge]

            kept_data = sub_data[sub_data['price_13F'] < upper_price_limit]
            delete_data = sub_data[sub_data['price_13F'] >= upper_price_limit]

            if not delete_data.empty:  # 有個股被濾除
                extra_cost = delete_data['investment_amount'].sum()
                if kept_data.empty:
                    extra_cost_dict['external'] += extra_cost
                else:
                    extra_cost_dict['internal'].append({each_hedge: extra_cost})

            final_kept_data = pd.concat([final_kept_data, kept_data], ignore_index=True)
            final_delete_data = pd.concat([final_delete_data, delete_data], ignore_index=True)

        # 2. 開始資金分配 internal
        for item in extra_cost_dict['internal']:
            hedge_ = list(item.keys())[0]
            value_ = list(item.values())[0]
            # print('額外資金分配:{} {}'.format(hedge_, value_))
            
            # 將value平均分配給final_kept_data[final_kept_data['HEDGE_FUND'] == hedge_]的資料中
            mask = final_kept_data['HEDGE_FUND'] == hedge_
            count = mask.sum()
            if count > 0:
                final_kept_data.loc[mask, 'invest_adj'] += value_ / count

        # 3. 分配資金 external
        external_value = extra_cost_dict['external']
        if not final_kept_data.empty:
            count = len(final_kept_data)
            final_kept_data['invest_adj'] += external_value / count

        # 4. 用投資額'invest_adj'和價格'price_13F'計算應該投資的股數'shares_adj'
        final_kept_data['shares_adj'] = (final_kept_data['invest_adj'] / final_kept_data['price_13F'])

        final_kept_data['shares_adj'] = final_kept_data['shares_adj'].replace([np.inf, -np.inf], np.nan)
        final_kept_data['shares_adj'] = final_kept_data['shares_adj'].fillna(0).astype(int)
        final_kept_data['shares_adj'] = final_kept_data['shares_adj'].astype(int)

        # 查看結果
        # print('final_kept_data\n', final_kept_data)
        result_data = pd.concat([final_kept_data, final_delete_data], ignore_index=True)
        result_data = result_data[['SYM', 'QUARTER', 'HEDGE_FUND','date_13F', 'price_13F', 'invest_adj', 'shares_adj', 'shares_origin']]
        result_data = result_data.rename(columns={'invest_adj':'investment_amount', 'shares_adj':'shares_to_buy'})
        return result_data
    def arrange_customized_table(self, customized_table):
        '''
        FUNCTION:
            各基金可能有投資相同的公司股票，故此步驟是要合併這些records。
            以['QUARTER', 'SYM'] 進行GROUP BY，SUM UP ['shares_to_buy', 'shares_origin']  (區分兩種是因為shares_to_buy有可能是被調整過的)
        INPUT:
        OUTPUT:

        '''
        merge_shares_to_buy = customized_table.groupby(['QUARTER', 'SYM'], as_index=False)[['shares_to_buy', 'shares_origin']].sum()
        # 將 merge_shares_to_buy 與原始 DataFrame 合併，以保留 SYM、QUARTER、date、price 欄位
        merged_df = pd.merge(merge_shares_to_buy, customized_table[['SYM', 'QUARTER', 'date_13F', 'price_13F']], on=['SYM', 'QUARTER'], how='left')
        merged_df = merged_df.drop_duplicates()
        merged_df['suggested_invest_amount'] = merged_df['shares_to_buy'] * merged_df['price_13F']
        merged_df = merged_df.sort_values(by=['date_13F'], ascending=True)
        merged_df = merged_df.rename(columns={'date_13F': 'date', 'price_13F': 'price'})
        return merged_df

    def calculate_market_price_growth(self, table): #下一季的投資金額
        
        q_customized_table = table.copy()
        def calculate_next_q_date(row):
            year = int(row['QUARTER'].split()[1])
            if 'Q1' in row['QUARTER']:
                return datetime.datetime(year, 8, 14)
            elif 'Q2' in row['QUARTER']:
                return datetime.datetime(year, 11, 14)
            elif 'Q3' in row['QUARTER']:
                return datetime.datetime(year+1, 2, 14)
            elif 'Q4' in row['QUARTER']:
                return datetime.datetime(year+1, 5, 15)
        # 新增欄位NEXT_QUARTER_DATE
        q_customized_table['NEXT_QUARTER_DATE'] = q_customized_table.apply(calculate_next_q_date, axis=1)
        current_sym_str = q_customized_table['SYM'].dropna().values
        if len(current_sym_str) == 1:
            current_sym_str = str(current_sym_str)
            current_sym_str = current_sym_str.replace('[', '(')
            current_sym_str = current_sym_str.replace(']', ')')
        else:
            current_sym_str = tuple(current_sym_str)

        holdings_time = q_customized_table['NEXT_QUARTER_DATE'][0]
        holdings_time = self.adjust_holdings_time(holdings_time, self.us_sorted_dates, next_day=False)

        query = self.create_query_get_open_price_for_customized_fund(current_sym_str, holdings_time)
        price_data = self.sql_execute(query)
        price_data = pd.DataFrame(price_data)
        price_data = price_data.merge(q_customized_table, on=['SYM'], how='left')
        market_value = sum(price_data['Open'] * price_data['shares_origin'])
        
        return market_value

    def arragne_output_XIRR_excel_format(self, summary_table):
        summary_table = summary_table[summary_table['date'] == self.max_date]
        summary_table = summary_table.sort_values(by=['XIRR'], ascending=False)
        return summary_table
    

    def modify_customized_fund_data_to_holdings_data_structures(self, hedge_fund, customized_fund_data):

        df = customized_fund_data
        df['ISSUER_NAME'] = '-'
        df['CL'] = '-'
        df['CUSIP'] = '-'
        df['PRINCIPAL'] = '-'
        df['OPTION_TYPE'] = '-'
        df['FORM_TYPE'] = '-'
        df['HEDGE_FUND'] = hedge_fund
        df['FILING_ID'] = '-'

        df.rename(columns={'suggested_invest_amount': 'VALUE'}, inplace=True)
        df['VALUE'] = df['VALUE'].astype(int)

        total_value_by_quarter = df.groupby('QUARTER')['VALUE'].transform('sum')
        df['Percentile'] = np.where(total_value_by_quarter == 0, 0, df['VALUE'] / total_value_by_quarter)

        df.rename(columns={'shares_to_buy': 'SHARES'}, inplace=True)

        df = df[['SYM', 'ISSUER_NAME', 'CL', 'CUSIP', 'VALUE', 'Percentile', 'SHARES', 'PRINCIPAL', 'OPTION_TYPE', 'HEDGE_FUND', 'QUARTER', 'FORM_TYPE', 'FILING_ID']]
        df = df.reset_index(drop=True)
        return df
    
    def arrage_customized_fund_portfolio_data(self, hedge_fund, holdings_data):

        current_date = datetime.datetime.now().date()

        df = holdings_data.groupby('QUARTER').agg(
            HOLDINGS=('SYM', 'count'),
            VALUE=('VALUE', 'sum'),
            TOP_HOLDINGS=('SYM', lambda x: ', '.join(x[:3])),
        ).reset_index()

        df['FORM_TYPE'] = '-'

        def get_date(row):
            year = int(row['QUARTER'].split()[1])
            if 'Q1' in row['QUARTER']:
                return datetime.datetime(year, 5, 15)
            elif 'Q2' in row['QUARTER']:
                return datetime.datetime(year, 8, 14)
            elif 'Q3' in row['QUARTER']:
                return datetime.datetime(year, 11, 14)
            elif 'Q4' in row['QUARTER']:
                return datetime.datetime(year+1, 2, 14)
        df['DATE_FILED'] = df.apply(get_date, axis=1)
        # df['DATE_FILED'] = current_date
        df['FILING_ID'] = '-'
        df['HEDGE_FUND'] = hedge_fund
        df = df[['QUARTER', 'HOLDINGS', 'VALUE', 'TOP_HOLDINGS', 'FORM_TYPE', 'DATE_FILED', 'FILING_ID', 'HEDGE_FUND']]
        df = df.sort_values(by=['DATE_FILED'], ascending=True).reset_index(drop=True)
        return df
    
    def insert_records_to_DB(self, table_name, data):
        
        if self.config_obj.LOCAL_FLAG:
            conn = pymssql.connect(host='localhost', user = 'myfirstjump', password='myfirstjump', database='US_DB')
        else:
            conn = pymssql.connect(host='localhost', user = 'stock_search', password='1qazZAQ!', database='STOCK_SKILL_DB')
        
        cursor = conn.cursor(as_dict=True)

        data_tuple = [tuple(row) for row in data.values]
        # print(hedge_tuple)
        if table_name == '[US_DB].[dbo].[HEDGE_FUND_PORTFOLIO]':

            cursor.executemany(
                """INSERT INTO [US_DB].[dbo].[HEDGE_FUND_PORTFOLIO]
                (
                [QUARTER]
                ,[HOLDINGS]
                ,[VALUE]
                ,[TOP_HOLDINGS]
                ,[FORM_TYPE]
                ,[DATE_FILED]
                ,[FILING_ID]
                ,[HEDGE_FUND]
                ) 
                VALUES (%s,%d,%d,%s,%s,%s,%s,%s)"""
                , data_tuple
            )
            conn.commit()
        
        elif table_name == '[US_DB].[dbo].[HOLDINGS_DATA]':
            
            cursor.executemany(
                    """INSERT INTO [US_DB].[dbo].[HOLDINGS_DATA]
                    (
                    [SYM]
                    ,[ISSUER_NAME]
                    ,[CL]
                    ,[CUSIP]
                    ,[VALUE]
                    ,[Percentile]
                    ,[SHARES]
                    ,[PRINCIPAL]
                    ,[OPTION_TYPE]
                    ,[HEDGE_FUND]
                    ,[QUARTER]
                    ,[FORM_TYPE]
                    ,[FILING_ID]
                    ) 
                    VALUES(%s,%s,%s,%s,%d,%d,%d,%s,%s,%s,%s,%s,%s)"""
                    , data_tuple
            )
            conn.commit()
        
        elif table_name == self.config_obj.customized_fund_portfolio_table:
            cursor.executemany(
                """INSERT INTO [US_DB].[dbo].[CUSTOMIZED_HEDGE_FUND_PORTFOLIO]
                (
                [QUARTER]
                ,[HOLDINGS]
                ,[VALUE]
                ,[TOP_HOLDINGS]
                ,[FORM_TYPE]
                ,[DATE_FILED]
                ,[FILING_ID]
                ,[HEDGE_FUND]
                ) 
                VALUES (%s,%d,%d,%s,%s,%s,%s,%s)"""
                , data_tuple
            )
            conn.commit()
        elif table_name == self.config_obj.customized_holdings_data_table:
            cursor.executemany(
                    """INSERT INTO [US_DB].[dbo].[CUSTOMIZED_HOLDINGS_DATA]
                    (
                    [SYM]
                    ,[ISSUER_NAME]
                    ,[CL]
                    ,[CUSIP]
                    ,[VALUE]
                    ,[Percentile]
                    ,[SHARES]
                    ,[PRINCIPAL]
                    ,[OPTION_TYPE]
                    ,[HEDGE_FUND]
                    ,[QUARTER]
                    ,[FORM_TYPE]
                    ,[FILING_ID]
                    ) 
                    VALUES(%s,%s,%s,%s,%d,%d,%d,%s,%s,%s,%s,%s,%s)"""
                    , data_tuple
            )
            conn.commit()
        
        inserted_rows = cursor.rowcount
        cursor.close()
        conn.close()

        return table_name, inserted_rows