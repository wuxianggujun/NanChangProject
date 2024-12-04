import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# 获取总部工单下的总工单号
def get_ticket_number(driver):
    # 等待表格加载，确保工单数据已经渲染
    # 等待表格数据加载
    WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.XPATH, '//tr[@class="ant-table-row ant-table-row-level-0"]'))
    )

    # 获取所有工单号的 <span> 标签
    work_order_elements = driver.find_elements(By.XPATH,
                                               '//tr[@class="ant-table-row ant-table-row-level-0"]//span[@title]')

    # 提取工单号
    work_order_numbers = [element.get_attribute('title') for element in work_order_elements]

    # 打印所有工单号
    print("找到的所有工单号：")
    for work_order_number in work_order_numbers:
        print(work_order_number)

    # 获取表格中的总行数（数据条数）
    total_rows = len(work_order_numbers)
    print(f"总共有 {total_rows} 条数据。")



chrome_options = Options()
chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
driver = webdriver.Chrome(chrome_options=chrome_options)
# 接着运行
print(driver.current_url)
print(driver.title)

# 设置 Edge 的远程调试端口
debugger_address = "localhost:9222"  # 这里的端口号需要和你启动浏览器时使用的端口一致

# 设置 Edge 浏览器选项
options = Options()
options.add_experimental_option("debuggerAddress", debugger_address)

# 使用 Remote WebDriver 连接到已经启动的浏览器
driver = webdriver.Edge(options=options)


# 初始化浏览器为Edge
browser = webdriver.Edge()
browser.get("http://10.188.34.1/cs/workbench/page/workbench.html?t=linestaging&layout=react")
# # 获取工单流水号输入框
# intput_box = browser.find_element(By.ID, "sheetCode")
# # 或者使用 name 来定位
# # input_box = driver.find_element(By.NAME, "like.tbl_gdzx_sheet_main__sheet_code")
# intput_box.send_keys("TS202411291115256212")

# 提示用户手动登录
input("请手动登录并按回车继续执行自动化操作...")

# 获取业务号码输入框
# intput_box = browser.find_element(By.ID, "sheetCode")
# 或者使用 name 来定位
input_box = browser.find_element(By.ID, "phoneSeacher")
input_box.send_keys("TS202411291115256212")

# 输入工单开始时间
start_time_input_box = browser.find_element(By.NAME, "timeBetween.tbl_gdzx_sheet_main__accept_time1")
start_time_input_box.send_keys("2024-10-01")

start_time_input_box.send_keys(Keys.RETURN)
# 输入工单结束时间
end_time_input_box = browser.find_element(By.NAME, "timeBetween.tbl_gdzx_sheet_main__accept_time2")

# 输入结束时间（确保日期格式符合页面要求）
end_time_input_box.send_keys("2024-12-31")  # 假设日期格式为 'YYYY-MM-DD'
end_time_input_box.send_keys(Keys.RETURN)

# 根据 onclick 属性来定位并点击查询按钮
query_button = browser.find_element(By.XPATH, '//*[@onclick="datagrid.search();"]')
query_button.click()

# 等待页面加载并找到所有符合条件的 <a> 标签
try:
    # 获取所有的 <a> 标签
    complaint_links = WebDriverWait(browser, 10).until(
        EC.presence_of_all_elements_located((By.XPATH, '//a[starts-with(@onclick, "datagrid.openNewDetail")]'))
    )

    # 如果有符合条件的链接，点击第一个
    if complaint_links:

        first_complaint = complaint_links[0]
        print("即将点击的数据链接：", first_complaint.get_attribute('href'))  # 打印链接信息，调试用
        first_complaint.click()

        # 等待总部工单信息部分加载
        WebDriverWait(browser, 10).until(
            EC.presence_of_element_located((By.XPATH, '//div[@role="tab" and contains(text(), "总部工单信息")]'))
        )

        # 获取“总部工单信息”中的数字，表示关联的工单数量
        related_work_orders_text = browser.find_element(By.XPATH,
                                                        '//div[@role="tab" and contains(text(), "总部工单信息")]/span/span[@class="orange fontWeight6"]').text

        # 转换为数字类型
        related_work_orders_count = int(related_work_orders_text)

        # 判断关联的工单数量
        if related_work_orders_count > 0:
            print(f"有 {related_work_orders_count} 个关联工单。")
            # 可以在此处继续操作，如点击查看更多关联工单等
            
            get_ticket_number(browser)
            
        else:
            print("没有关联工单。")

        print("已点击第一条数据链接。")
    else:
        print("没有符合条件的数据。")

except Exception as e:
    print("加载页面失败或发生错误：", e)

# 关闭浏览器
# driver.quit()
