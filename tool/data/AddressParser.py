import re
from typing import Dict, Optional

class AddressParser:
    def __init__(self):
        self.address_keywords = ['省', '市', '区', '县', '镇', '乡', '村', '社区', '街道']
        self.separators = ['|', ':', '：', ';', '；']
        
    def extract_address(self, text: str) -> str:
        """从文本中提取有效地址"""
        if not isinstance(text, str) or not text.strip() or text == '无':
            return ''
        
        # 清理和标准化文本
        text = self.clean_text(text)
        
        # 处理特殊格式的地址
        if '投诉地址：' in text:
            match = re.search(r'投诉地址：([^;；|]+)', text)
            if match:
                text = match.group(1)
        
        # 处理带分隔符的地址
        for sep in self.separators:
            if sep in text:
                parts = text.split(sep)
                # 选择包含最多地址关键词的部分
                text = max(parts, 
                          key=lambda x: sum(1 for k in self.address_keywords if k in x))
        
        # 提取包含地址关键词的片段
        address_parts = []
        for part in re.split(r'[,，、]', text):
            if any(keyword in part for keyword in self.address_keywords):
                # 清理提取的地址部分
                cleaned_part = self.clean_address_part(part)
                if cleaned_part:
                    address_parts.append(cleaned_part)
        
        # 组合并优化地址
        if address_parts:
            combined_address = ''.join(address_parts)
            return self.optimize_address(combined_address)
            
        return ''
    
    def optimize_address(self, address: str) -> str:
        """优化地址格式，去除重复部分"""
        if not address:
            return ''
            
        # 去除重复的省份信息
        if address.count('省') > 1:
            # 找到最后一个省的位置
            last_province_idx = address.rfind('省')
            # 从最后一个省之前的文本中删除其他省份信息
            prefix = re.sub(r'[^省市区县]+?省', '', address[:last_province_idx])
            address = prefix + address[last_province_idx:]
        
        # 去除重复的市信息
        if address.count('市') > 1:
            # 找到最后一个市的位置
            last_city_idx = address.rfind('市')
            # 从最后一个市之前的文本中删除其他市信息
            prefix = re.sub(r'[^市区县]+?市', '', address[:last_city_idx])
            address = prefix + address[last_city_idx:]
            
        # 去除重复的区县信息
        for suffix in ['区', '县']:
            if address.count(suffix) > 1:
                last_idx = address.rfind(suffix)
                prefix = re.sub(f'[^区县]+?{suffix}', '', address[:last_idx])
                address = prefix + address[last_idx:]
        
        # 移除多余的符号
        address = re.sub(r'[【】\[\]]', '', address)
        
        return address.strip()
    
    def clean_text(self, text: str) -> str:
        """清理文本"""
        # 移除特殊字符和多余空格
        text = re.sub(r'[\r\n\t]', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        # 移除无用的前缀
        text = re.sub(r'^(位于|在|送达|地址[是为：:]+)', '', text.strip())
        return text.strip()
    
    def clean_address_part(self, part: str) -> str:
        """清理地址片段"""
        # 移除无关信息
        part = re.sub(r'(用户[^省市区县]*|客户[^省市区县]*|业务号码[^省市区县]*)', '', part)
        # 移除括号内容
        part = re.sub(r'\([^)]*\)', '', part)
        part = re.sub(r'（[^）]*）', '', part)
        # 移除数字和字母开头的无关信息
        part = re.sub(r'\d+[.、].*?(?=[省市区县]|$)', '', part)
        part = re.sub(r'[A-Za-z]+[.、].*?(?=[省市区县]|$)', '', part)
        
        # 保留包含地址关键词的最小有效部分
        valid_part = ''
        for keyword in self.address_keywords:
            if keyword in part:
                match = re.search(f'[^，,、]*?{keyword}', part)
                if match:
                    valid_part += match.group(0)
        
        return valid_part.strip()