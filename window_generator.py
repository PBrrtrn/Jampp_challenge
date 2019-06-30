import pandas as pd
from datetime import timedelta

def generate_windows_pd(df, str_timestamp):
    INITIAL_DATE = df[str_timestamp].min().date()
    
    DATE_PLUS_ONE_DAY = pd.to_datetime(INITIAL_DATE + timedelta(days=1) ) 
    DATE_PLUS_TWO_DAYS = pd.to_datetime(INITIAL_DATE + timedelta(days=2) )
    DATE_PLUS_THREE_DAYS = pd.to_datetime(INITIAL_DATE + timedelta(days=3) )
    DATE_PLUS_FOUR_DAYS = pd.to_datetime(INITIAL_DATE + timedelta(days=4) )
    DATE_PLUS_FIVE_DAYS = pd.to_datetime(INITIAL_DATE + timedelta(days=5) )
    DATE_PLUS_SIX_DAYS = pd.to_datetime(INITIAL_DATE + timedelta(days=6) )
    DATE_PLUS_SEVEN_DAYS = pd.to_datetime(INITIAL_DATE + timedelta(days=7) )
    DATE_PLUS_EIGHT_DAYS = pd.to_datetime(INITIAL_DATE + timedelta(days=8) )
    DATE_PLUS_NINE_DAYS = pd.to_datetime(INITIAL_DATE + timedelta(days=9) )
    
    window1 = df.loc[df[str_timestamp] < DATE_PLUS_THREE_DAYS]
    window1 = window1.loc[df[str_timestamp] > INITIAL_DATE]
    
    window2 = df.loc[df[str_timestamp] < DATE_PLUS_FOUR_DAYS]
    window2 = window2.loc[df[str_timestamp] > DATE_PLUS_ONE_DAY]
    
    window3 = df.loc[df[str_timestamp] < DATE_PLUS_FIVE_DAYS]
    window3 = window3.loc[df[str_timestamp] > DATE_PLUS_TWO_DAYS]
    
    window4 = df.loc[df[str_timestamp] < DATE_PLUS_SIX_DAYS]
    window4 = window4.loc[df[str_timestamp] > DATE_PLUS_THREE_DAYS]
    
    window5 = df.loc[df[str_timestamp] < DATE_PLUS_SEVEN_DAYS]
    window5 = window5.loc[df[str_timestamp] > DATE_PLUS_FOUR_DAYS]
    
    window6 = df.loc[df[str_timestamp] < DATE_PLUS_EIGHT_DAYS]
    window6 = window6.loc[df[str_timestamp] > DATE_PLUS_FIVE_DAYS]
    
    window7 = df.loc[df[str_timestamp] < DATE_PLUS_NINE_DAYS]
    window7 = window7.loc[df[str_timestamp] > DATE_PLUS_SIX_DAYS]
    
    return window1, window2, window3, window4, window5, window6, window7

def generate_windows_sc(rdd):
    INITIAL_DATE = pd.to_datetime('2019-04-20 00:00:00.00')
    
    DATE_PLUS_ONE_DAY = pd.to_datetime(INITIAL_DATE + timedelta(days=1) ) 
    DATE_PLUS_TWO_DAYS = pd.to_datetime(INITIAL_DATE + timedelta(days=2) )
    DATE_PLUS_THREE_DAYS = pd.to_datetime(INITIAL_DATE + timedelta(days=3) )
    DATE_PLUS_FOUR_DAYS = pd.to_datetime(INITIAL_DATE + timedelta(days=4) )
    DATE_PLUS_FIVE_DAYS = pd.to_datetime(INITIAL_DATE + timedelta(days=5) )
    DATE_PLUS_SIX_DAYS = pd.to_datetime(INITIAL_DATE + timedelta(days=6) )
    DATE_PLUS_SEVEN_DAYS = pd.to_datetime(INITIAL_DATE + timedelta(days=7) )
    DATE_PLUS_EIGHT_DAYS = pd.to_datetime(INITIAL_DATE + timedelta(days=8) )
    DATE_PLUS_NINE_DAYS = pd.to_datetime(INITIAL_DATE + timedelta(days=9) )
    
    window1 = rdd.filter(lambda x: x[1] < DATE_PLUS_THREE_DAYS)
    window1 = window1.filter(lambda x: x[1] > INITIAL_DATE)
    
    window2 = rdd.filter(lambda x: x[1] < DATE_PLUS_FOUR_DAYS)
    window2 = window2.filter(lambda x: x[1] > DATE_PLUS_ONE_DAY)
    
    window3 = rdd.filter(lambda x: x[1] < DATE_PLUS_FIVE_DAYS)
    window3 = window3.filter(lambda x: x[1] > DATE_PLUS_TWO_DAYS)
    
    window4 = rdd.filter(lambda x: x[1] < DATE_PLUS_SIX_DAYS)
    window4 = window4.filter(lambda x: x[1] > DATE_PLUS_THREE_DAYS)
    
    window5 = rdd.filter(lambda x: x[1] < DATE_PLUS_SEVEN_DAYS)
    window5 = window5.filter(lambda x: x[1] > DATE_PLUS_FOUR_DAYS)
    
    window6 = rdd.filter(lambda x: x[1] < DATE_PLUS_EIGHT_DAYS)
    window6 = window6.filter(lambda x: x[1] > DATE_PLUS_FIVE_DAYS)
    
    window7 = rdd.filter(lambda x: x[1] < DATE_PLUS_NINE_DAYS)
    window7 = window7.filter(lambda x: x[1] > DATE_PLUS_SIX_DAYS)
    
    return window1, window2, window3, window4, window5, window6, window7
    