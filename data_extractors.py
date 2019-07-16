import pandas as pd

def extract_data(w1, w2, w3, w4, w5, w6, w7, goal_column_name):
    ''' Module receives seven windows, returning the data within; i.e., without the goal column'''
    w1_data = w1.drop(columns = [goal_column_name])
    w2_data = w2.drop(columns = [goal_column_name])
    w3_data = w3.drop(columns = [goal_column_name])
    w4_data = w4.drop(columns = [goal_column_name])
    w5_data = w5.drop(columns = [goal_column_name])
    w6_data = w6.drop(columns = [goal_column_name])
    w7_data = w7.drop(columns = [goal_column_name])
    
    return w1_data.values, w2_data.values, w3_data.values, w4_data.values, w5_data.values, w6_data.values, w7_data.values

def extract_goals(w1,w2,w3,w4,w5,w6,w7,goal_column_name):
    w1_goal = w1[goal_column_name]
    w2_goal = w2[goal_column_name]
    w3_goal = w3[goal_column_name]
    w4_goal = w4[goal_column_name]
    w5_goal = w5[goal_column_name]
    w6_goal = w6[goal_column_name]
    w7_goal = w7[goal_column_name]
    
    return w1_goal.values, w2_goal.values, w3_goal.values, w4_goal.values, w5_goal.values, w6_goal.values, w7_goal.values