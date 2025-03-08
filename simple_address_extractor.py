# coding=utf-8
import polars as pl
import os
from aliyun_llm import AliyunLLM
import time

def extract_address(text, llm):
    """使用AI从文本中提取地址"""
    if not text or isinstance(text, str) and text.strip() == "":
        return "解析地址失败"
        
    prompt = f"""
请从以下文本中提取地址信息。只需返回地址，不要包含任何其他内容。
如果无法识别出地址，只回复"解析地址失败"，不要解释原因。

文本: {text}

提取规则：
1. 移除"答复口径："、"投诉内容："等前缀
2. 提取完整的地理位置信息，包括省、市、区/县、街道/路/小区/建筑等
3. 注意处理如"南昌市西湖区丰和北大道与长春路交界处"这样的路口位置
4. 注意处理如"江西工业职业技术学院(瑶湖校区)"这样带括号的位置
5. 如果有GPS坐标(如115.836749,28.596142)，请忽略坐标部分，只提取地址
6. 如果文本中有多个地址，提取最主要的一个（通常是投诉发生的具体地点）

例如：
例1: "答复口径：用户反映江西省南昌市西湖区丰和北大道与长春路交界处移动信号差"
应返回: "江西省南昌市西湖区丰和北大道与长春路交界处"

例2: "答复口径：中国移动南昌红谷滩10086营业厅用户反馈网络问题"
应返回: "南昌红谷滩"

例3: "投诉内容：南昌青山湖区政务中心无法办理业务"
应返回: "南昌青山湖区政务中心"

例4: "用户反应在红谷滩区:中耀建业上网信号差"
应返回: "红谷滩区中耀建业"

例5: "回单内容：南昌县新力象湖湾12栋1单元2604室（115.836749,28.596142）"
应返回: "南昌县新力象湖湾12栋1单元2604室"

例6: "江西工业职业技术学院(瑶湖校区)出口处信号差"
应返回: "江西工业职业技术学院(瑶湖校区)"

请注意，我只需要提取出准确的地址信息，不需要额外的说明或解释。只需返回地址本身，如果无法识别出地址，请只回复"解析地址失败"四个字。
"""
    # 使用大模型解析地址
    try:
        result = llm.chat_without_streaming_display(prompt)
        # 如果结果不是"解析地址失败"但内容很长，可能是AI添加了额外说明
        if result and result != "解析地址失败" and len(result) > 100:
            # 尝试只保留第一行内容
            first_line = result.strip().split('\n')[0]
            if len(first_line) < 100:
                return first_line
        return result.strip()
    except Exception as e:
        print(f"地址解析出错: {e}")
        return "解析地址失败"


def main():
    print("=" * 50)
    print("简易投诉热点地址提取工具")
    print("=" * 50)
    
    # 初始化大模型
    llm = AliyunLLM()
    
    # 直接指定文件路径
    file_path = "WorkDocument/投诉热点明细分析/source/202403-202502投诉热点明细表-终稿.xlsx"
    print(f"\n读取文件: {file_path}")
    
    try:
        # 尝试读取Excel文件
        data = pl.read_excel(file_path)
        print(f"成功读取数据，共 {data.height} 条记录")
        
        # 检查是否存在"答复口径"列
        if "答复口径" not in data.columns:
            print(f"错误: 文件中不包含'答复口径'列。")
            print(f"可用的列: {data.columns}")
            return
            
        # 询问需要处理的记录数量
        limit_input = input("要处理的记录数量 (直接回车表示全部): ")
        limit = int(limit_input) if limit_input.strip() else data.height
        
        # 创建结果集
        results = []
        count = 0
        successful = 0
        
        # 处理每一行数据
        for i, row in enumerate(data.iter_rows(named=True)):
            if i >= limit:
                break
                
            text = row.get("答复口径", "")
            if not text or str(text).strip() == "":
                print(f"[{i+1}/{limit}] 跳过空内容")
                continue
                
            print(f"\n[{i+1}/{limit}] 处理内容: {text[:100]}..." if len(str(text)) > 100 else text)
            
            # 解析地址
            address = extract_address(text, llm)
            count += 1
            
            # 只展示地址
            if address != "解析地址失败":
                successful += 1
                print(f"✓ 地址: {address}")
                results.append({"原始文本": text, "地址": address})
            else:
                print("✗ 解析地址失败")
                results.append({"原始文本": text, "地址": address})
                
            # 避免API请求过快
            time.sleep(0.5)
        
        # 保存结果
        if results:
            output_file = "地址解析结果.csv"
            pl.DataFrame(results).write_csv(output_file, encoding="utf-8-sig")
            print(f"\n解析完成，共处理 {count} 条记录，成功解析 {successful} 条地址。")
            print(f"结果已保存至: {output_file}")
            
            # 输出成功率
            if count > 0:
                success_rate = (successful / count) * 100
                print(f"地址解析成功率: {success_rate:.2f}%")
        
    except Exception as e:
        print(f"程序执行错误: {e}")


if __name__ == "__main__":
    main() 