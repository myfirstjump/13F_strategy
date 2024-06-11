bg_code = '#EEF3F9'
dark_code = '#C1DEF4' 
light_code = '#E1EBF9' #
border_code = '#A8D3F4'
emphsis_code = '#1EABF4'


top_div_bg = '#212130' #1

filter_condition_bg = '#212130' #2
inner_frame_bg = '#17171E' #3
item_bg = '#A9DBFC' #4
result_bg = '#2E2E40' #5
result_words = '#2399E7' #6

query_blocks_bg = '#212130'

header_div_style = {
    'background-color': top_div_bg,
    'margin':'10px 15px 10px 30px', 
    'padding':'10px',
    #'border':'solid 1px',  
    'display': 'flex',  # 使用 flex 布局
    'flexWrap': 'wrap',  # 允许换行
}

header_text_style = {
    'color': 'white',
}

top_div_style = {
    'background-color': top_div_bg,
    'height': '1540px', 
    #'border':'solid 1px',  
}



result_content = {
    'margin': '2%',
    'font-size': '28px',
    'color': 'black',
    'height': '500px',
    'width': '100%',
    # 'background-color': '#17171E',
    # 'border': 'solid 1px white',
}


frame_text_style = {
    'font-size': '20px',
    'color': 'white',
    'verticalAlign':'middle',
    'margin': '1%',
    # 'border-bottom': 'dashed 5px #B2CCF2',
    # 'border-radius':'20px',
    #'border':'solid 1px',  
}


large_dropdown_style = {
    'verticalAlign': 'middle',
    # 'padding':'0% 1% 0% 1%',
    'color':'black', 
    'width': '350px',
    'font-size':'15px',
    'background-color': dark_code,
    'border-radius': '8px',
}

dp_div_style = { #dropdown外層div
    'verticalAlign': 'middle', 
    'display':'inline-block',
    #'border':'solid 1px',
    'border-color': emphsis_code,
    'border-radius': '7px',
    'margin': '1%',
}

content_div_style = {
    'verticalAlign': 'middle', 
    'display':'inline-block',
    # 'border':'solid 1px white',
    # 'border-color': emphsis_code,
    # 'border-radius': '7px',
    'margin': '1%',
    'width': '33%',
    'boxSizing': 'border-box',  # 包括内边距和边框在内的宽度和高度计算
}

large_input_style = {
    'display':'inline-block',
    'verticalAlign': 'middle',
    'width':'350px',
    'height': '27px',
    #'border':'solid 1px',
    'font-size':'15px',
    'background-color':dark_code,
    'border-radius': '7px',
    'margin': '1%',
}
