import xlwings as xw

if __name__ == '__main__':

    # 尝试连接到已经打开的Excel应用，如果不存在则创建一个新的
    app = xw.apps.active if xw.apps.count > 0 else xw.App(visible=True, add_book=False)

    # 打开一个现有的Excel文件  
    workbook = app.books.open("D:/NanChangWork/网络质量报表10月份超时明细分析表V2.xlsx")
    # 激活第一个工作表
    sheet = workbook.sheets[0]

    # 获取第一行的列名，找到“新客服工单号”所在的列
    column_names = sheet.range('A1').expand('table').value
    # print("列名：", column_names)
    # new_ticket_column_index = None
    # for index, name in enumerate(column_names[0]):
    #     if name == '新客服工单号':
    #         new_ticket_column_index = index + 1  # xlwings索引从1开始
    #         break
    # 
    # if new_ticket_column_index:
    #     print(f"新客服工单号列的索引是：{new_ticket_column_index}")
    # 
    #     # 检查是否已经应用了筛选，且筛选条件为“#N/A”
    #     current_filter = sheet.api.AutoFilter.Filters
    #     if current_filter and current_filter(new_ticket_column_index).On:
    #         current_criteria = current_filter(new_ticket_column_index).Criteria1
    #         if current_criteria == '#N/A':
    #             print("筛选条件已是#N/A，无需取消筛选")
    #         else:
    #             # 如果当前筛选条件不是#N/A，则取消筛选并重新应用#N/A筛选
    #             print("正在取消筛选并应用#N/A筛选")
    #             sheet.api.AutoFilterMode = False
    #             sheet.range('A1:CE415').api.AutoFilter(Field=new_ticket_column_index, Criteria1='=#N/A')
    #     else:
    #         # 如果没有筛选条件或筛选条件不是#N/A，应用#N/A筛选
    #         print("没有筛选或筛选条件不是#N/A，正在应用#N/A筛选")
    #         sheet.range('A1:CE415').api.AutoFilter(Field=new_ticket_column_index, Criteria1='=#N/A')
    # 
    #     # 设置筛选范围并应用筛选条件
    #     # sheet.range('A1:CE415').api.AutoFilter(Field=new_ticket_column_index, Criteria1='=#N/A')
    # else:
    #     print("没有找到新客服工单号列")
    # 确定“新客服工单号”所在的列（这里假设它在 D 列）
    new_ticket_column_index = 71  # D 列的索引是 4（xlwings 的索引从 1 开始）
    # 获取 E 列的前 214 行数据（按筛选后的可见数据）
    for index in range(2, 415):  # E1 到 E214
        # 获取当前行的行号
        row = sheet.range(f'E{index}').row
        if not sheet.api.Rows(row).Hidden:  # 判断该行是否被隐藏（未通过筛选）
            business_number = sheet.range(f'E{index}').value
            print(business_number)
            # 如果业务号码（E列）符合条件，就修改新客服工单号（D列）
            if business_number:  # 如果 E 列有值
                # 更新新客服工单号列的值
                temp_value = sheet.range(row, new_ticket_column_index).value
                if temp_value == "#N/A":
                    temp_value = "hello"  # 这里可以替换成你想要写入的数据
                
                    print(f"已更新新客服工单号（D列）行号：{row}")