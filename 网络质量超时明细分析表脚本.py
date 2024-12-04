import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import xlwings as xw


if __name__ == '__main__':
    # 设置 Edge 的远程调试端口
    debugger_address = "localhost:9222"  # 这里的端口号需要和你启动浏览器时使用的端口一致

    # 设置 Edge 浏览器选项
    options = Options()
    options.add_experimental_option("debuggerAddress", debugger_address)

    driver = webdriver.Edge(options=options)
    # driver.get("http://10.188.34.1/cs/workbench/page/workbench.html?t=linestaging&layout=react")

    
    input_box = driver.find_element(By.NAME, "and.tbl_gdzx_sheet_main__business_no")
    input_box.send_keys("TS202411291115256212")
    
    
    