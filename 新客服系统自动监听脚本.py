#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
微信自动监听脚本
功能：监控微信窗口，获取会话列表信息，监控有无新消息
"""

import uiautomation as auto
import time
import logging
import os
import re
from datetime import datetime
import win32gui
import win32con
import random
from collections import OrderedDict

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('wechat_monitor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 微信配置
class WeChatConfig:
    PROCESS_NAME = 'WeChat.exe'
    WINDOW_NAME = '微信'
    WINDOW_CLASSNAME = 'WeChatMainWndForPC'
    # 最大会话数量
    MAX_CONVERSATIONS = 30
    # 排除的会话名称关键词
    EXCLUDE_KEYWORDS = ['公众号', '订阅号', '微信团队', '折叠置顶', '服务通知']

class WeChatMonitor:
    def __init__(self):
        """初始化微信监控器"""
        self.wechat_window = None
        # 用户名（用于检测@消息）
        self.username = "我"  # 默认为"我"，可根据实际情况修改
        # 会话列表控件
        self.conversation_list = None
        # 调试模式
        self.debug = True
        # 会话数据 - 使用OrderedDict保持顺序性
        self.conversation_dict = OrderedDict()
        # 当前会话列表的所有项目
        self.all_conversation_items = []
        # 最大滚动尝试次数
        self.max_scroll_attempts = 30
        # 自动点击开关
        self.auto_click_enabled = False
        # 已处理会话ID
        self.processed_ids = set()
        # 上次检查时间
        self.last_check_time = datetime.now()
        # 会话位置索引
        self.conversation_positions = {}
        
    def update_exclude_keywords(self):
        """更新排除关键词，添加自己的用户名"""
        if self.username and self.username not in WeChatConfig.EXCLUDE_KEYWORDS:
            # 添加用户名到排除列表，避免检测自己发给自己的消息
            WeChatConfig.EXCLUDE_KEYWORDS.append(self.username)
            logger.info(f"已添加用户名 '{self.username}' 到排除列表")
        
    def wake_up_window(self):
        """激活微信窗口，使用win32gui方法"""
        try:
            hwnd = win32gui.FindWindow(WeChatConfig.WINDOW_CLASSNAME, WeChatConfig.WINDOW_NAME)
            if hwnd:
                # 恢复窗口
                win32gui.ShowWindow(hwnd, win32con.SW_RESTORE)
                # 尝试将窗口置前
                try:
                    win32gui.SetForegroundWindow(hwnd)
                    logger.info("已成功激活微信窗口")
                    time.sleep(1)  # 等待窗口激活
                    return True
                except Exception as e:
                    logger.error(f"尝试将窗口置前时出错: {e}")
                    return False
            else:
                logger.warning("未找到微信窗口")
                return False
        except Exception as e:
            logger.error(f"激活微信窗口时出错: {e}")
            return False
    
    def find_wechat_window(self):
        """查找并获取微信主窗口"""
        try:
            # 先尝试使用win32gui激活窗口
            if not self.wake_up_window():
                logger.warning("无法激活微信窗口，尝试直接查找")
            
            # 尝试查找微信主窗口
            wechat_window = auto.WindowControl(ClassName=WeChatConfig.WINDOW_CLASSNAME)
            if wechat_window.Exists(3):
                logger.info("找到微信窗口")
                
                # 再次尝试激活窗口
                try:
                    wechat_window.SetActive()
                    time.sleep(1)
                    logger.info("通过UIAutomation激活了微信窗口")
                except Exception as e:
                    logger.warning(f"通过UIAutomation激活窗口失败: {e}")
                
                self.wechat_window = wechat_window
                return True
            else:
                logger.warning("未找到微信窗口，请确保微信已启动")
                return False
        except Exception as e:
            logger.error(f"查找微信窗口时出错: {e}")
            return False
    
    def print_control_info(self, control, prefix=""):
        """打印控件信息"""
        try:
            logger.info(f"{prefix}控件: Name='{control.Name}', ClassName='{control.ClassName}', ControlType='{control.ControlTypeName}'")
            
            try:
                children = control.GetChildren()
                for i, child in enumerate(children):
                    try:
                        self.print_control_info(child, prefix=f"{prefix}    {i+1}. ")
                    except:
                        logger.info(f"{prefix}    {i+1}. [无法获取子控件信息]")
            except Exception as e:
                logger.debug(f"{prefix}[无法获取子控件: {e}]")
                
        except Exception as e:
            logger.error(f"打印控件信息出错: {e}")
    
    def find_conversation_list(self):
        """查找会话列表控件"""
        if not self.wechat_window:
            return None
        
        try:
            # 直接通过Name查找会话列表
            conv_list = self.wechat_window.ListControl(Name="会话")
            if conv_list.Exists(1):
                children_count = len(conv_list.GetChildren())
                logger.info(f"找到会话列表 'Name=会话', 包含 {children_count} 个可见会话")
                self.conversation_list = conv_list
                return conv_list
                
            # 尝试通过其他方式查找
            all_lists = self.wechat_window.GetChildren()
            for item in all_lists:
                if item.ControlTypeName == "ListControl":
                    children = item.GetChildren()
                    if len(children) > 0:
                        logger.info(f"找到可能的会话列表: Name='{item.Name}', 包含 {len(children)} 个可见项目")
                        self.conversation_list = item
                        return item
            
            logger.warning("未找到会话列表控件")
            return None
        except Exception as e:
            logger.error(f"查找会话列表时出错: {e}")
            return None
    
    def check_conversation_unread(self, conversation_item):
        """检查会话是否有未读消息"""
        try:
            # 方法1: 检查是否有未读计数
            if hasattr(conversation_item, "Name") and conversation_item.Name:
                if re.search(r"\(\d+\)", conversation_item.Name):
                    return True
                    
            # 方法2: 检查是否有特殊标记
            children = conversation_item.GetChildren()
            for child in children:
                try:
                    # 检查是否有红点标记（通常是小的图片控件）
                    if (child.ControlTypeName == "ImageControl" or 
                        child.ControlTypeName == "TextControl"):
                        # 红点通常很小，且名称可能是空或特殊值
                        if hasattr(child, "BoundingRectangle"):
                            try:
                                rect = child.BoundingRectangle
                                # 尝试不同的方式获取宽高
                                if hasattr(rect, 'width') and hasattr(rect, 'height'):
                                    width = rect.width
                                    height = rect.height
                                else:
                                    width = rect.right - rect.left
                                    height = rect.bottom - rect.top
                                
                                # 红点通常很小
                                if width < 20 and height < 20 and width > 0 and height > 0:
                                    return True
                            except:
                                pass
                except:
                    continue
                    
            return False
        except Exception as e:
            logger.error(f"检查会话未读状态时出错: {e}")
            return False
    
    def generate_conversation_id(self, conversation_item):
        """为会话项生成唯一ID，用于跟踪已处理的会话"""
        try:
            # 优先使用名称
            name = conversation_item.Name if hasattr(conversation_item, "Name") else ""
            if name:
                return name
                
            # 如果没有名称，使用边界矩形
            if hasattr(conversation_item, "BoundingRectangle"):
                rect = conversation_item.BoundingRectangle
                if hasattr(rect, 'left') and hasattr(rect, 'top'):
                    return f"item_{rect.left}_{rect.top}"
                    
            # 最后使用对象ID
            return str(id(conversation_item))
        except:
            return str(id(conversation_item))
    
    def should_exclude_conversation(self, name):
        """检查会话是否应该被排除"""
        # 名称为空的会话不排除
        if not name:
            return False
            
        # 检查是否包含排除关键词
        for keyword in WeChatConfig.EXCLUDE_KEYWORDS:
            if keyword in name:
                logger.info(f"排除会话: {name} (包含关键词: {keyword})")
                return True
                
        return False
    
    def scroll_conversation_list(self, direction="down"):
        """滚动会话列表
        
        Args:
            direction: 滚动方向，"up"向上，"down"向下
        
        Returns:
            bool: 是否成功滚动
        """
        if not self.conversation_list:
            logger.warning("找不到会话列表，无法滚动")
            return False
            
        try:
            # 获取滚动前的会话项
            before_items = [item.Name for item in self.conversation_list.GetChildren() if hasattr(item, "Name")]
            
            # 用简单可靠的方法滚动
            try:
                # 确保控件有焦点
                self.conversation_list.SetFocus()
                time.sleep(0.2)
                
                # 选择滚动方向
                if direction == "down":
                    # 尝试滚轮方法
                    self.conversation_list.WheelDown(wheelTimes=3)
                else:
                    # 向上滚动
                    self.conversation_list.WheelUp(wheelTimes=3)
                
                time.sleep(0.5)  # 等待滚动生效
            except Exception as e:
                logger.warning(f"使用内置滚轮方法失败: {e}，尝试键盘方法")
                
                try:
                    # 使用键盘滚动
                    self.conversation_list.SetFocus()
                    time.sleep(0.2)
                    
                    if direction == "down":
                        for _ in range(5):
                            auto.SendKeys('{Down}')
                            time.sleep(0.1)
                    else:
                        for _ in range(5):
                            auto.SendKeys('{Up}')
                            time.sleep(0.1)
                except Exception as e:
                    logger.error(f"使用键盘方法滚动失败: {e}")
                    return False
            
            # 检查滚动后的会话项
            time.sleep(0.5)
            after_items = [item.Name for item in self.conversation_list.GetChildren() if hasattr(item, "Name")]
            
            # 检查是否有变化
            if before_items == after_items:
                logger.info(f"滚动{direction}后会话列表没有变化")
                return False
            else:
                new_count = len(set(after_items) - set(before_items))
                logger.info(f"滚动{direction}后会话列表有 {new_count} 个新会话")
                return True
                
        except Exception as e:
            logger.error(f"滚动会话列表时出错: {e}")
            return False
    
    def click_conversation(self, conversation_item):
        """点击指定的会话项
        
        Args:
            conversation_item: 要点击的会话控件
            
        Returns:
            bool: 是否成功点击
        """
        if not self.auto_click_enabled:
            logger.info("自动点击功能已禁用，跳过点击操作")
            return False
            
        if not conversation_item:
            logger.warning("无效的会话项，无法点击")
            return False
            
        try:
            name = conversation_item.Name if hasattr(conversation_item, "Name") else "未知会话"
            logger.info(f"尝试点击会话: {name}")
            
            # 使用控件自带的点击方法
            try:
                conversation_item.Click(simulateMove=False)
                logger.info(f"已点击会话: {name}")
                time.sleep(0.5)  # 等待界面响应
                return True
            except Exception as e:
                logger.warning(f"直接点击会话控件失败: {e}，尝试其他方法")
                
                try:
                    # 尝试获取中心点坐标点击
                    rect = conversation_item.BoundingRectangle
                    center_x = int(rect.left + (rect.right - rect.left) * 0.5)
                    center_y = int(rect.top + (rect.bottom - rect.top) * 0.5)
                    
                    # 设置点击点，避开可能的未读标记
                    click_x = center_x
                    click_y = center_y
                    
                    # 确保窗口活动
                    self.wechat_window.SetActive()
                    time.sleep(0.2)
                    
                    # 使用坐标点击
                    auto.Click(click_x, click_y)
                    logger.info(f"已通过坐标点击会话: {name} 在位置 ({click_x}, {click_y})")
                    time.sleep(0.5)
                    return True
                except Exception as e:
                    logger.error(f"通过坐标点击会话失败: {e}")
                    return False
        except Exception as e:
            logger.error(f"点击会话过程中出错: {e}")
            return False
    
    def get_conversation_messages(self, conversation=None):
        """获取指定会话的消息内容（如果已打开）
        
        Args:
            conversation: 会话信息字典，可选
            
        Returns:
            list: 消息内容列表
        """
        try:
            # 查找可能的聊天窗口
            chat_area = None
            for pane in self.wechat_window.GetChildren():
                if pane.ControlTypeName == "PaneControl":
                    # 检查是否有消息列表
                    for child in pane.GetChildren():
                        if child.ControlTypeName == "ListControl" and child.Name != "会话":
                            chat_area = pane
                            break
                    if chat_area:
                        break
            
            if not chat_area:
                logger.info("未找到聊天窗口")
                return []
                
            # 查找消息列表
            message_list = None
            for control in chat_area.GetChildren():
                if control.ControlTypeName == "ListControl":
                    message_list = control
                    break
                    
            if not message_list:
                logger.info("未找到消息列表控件")
                return []
                
            # 获取最新消息
            messages = []
            message_items = message_list.GetChildren()
            
            # 获取最新的几条消息
            recent_count = min(5, len(message_items))
            if recent_count == 0:
                return []
                
            recent_message_items = message_items[-recent_count:]
            
            # 记录消息内容
            for i, msg in enumerate(recent_message_items):
                try:
                    message_text = msg.Name
                    if message_text:
                        messages.append(message_text)
                        
                        # 检查是否有@我的消息
                        if conversation and f"@{self.username}" in message_text:
                            sender_match = re.search(r"^(.*?):", message_text)
                            sender = sender_match.group(1) if sender_match else "未知发送者"
                            
                            logger.info(f"[通知] 检测到@消息: {sender} 在 {conversation.get('name', '未知会话')} 中@了你")
                            logger.info(f"[通知] 消息全文: {message_text}")
                except Exception as e:
                    logger.error(f"获取消息内容时出错: {e}")
            
            return messages
        except Exception as e:
            logger.error(f"获取会话消息时出错: {e}")
            return []
    
    def create_conversation_info(self, item, index=0):
        """从会话项创建会话信息字典
        
        Args:
            item: 会话控件项
            index: 会话索引
            
        Returns:
            dict: 会话信息字典
        """
        try:
            # 获取会话名称
            name = item.Name if hasattr(item, "Name") else f"会话 {index}"
            
            # 生成会话ID
            conv_id = self.generate_conversation_id(item)
            
            # 检查是否有未读标记
            has_unread = self.check_conversation_unread(item)
            
            # 创建会话信息
            return {
                "index": index,
                "name": name,
                "id": conv_id,
                "has_unread": has_unread,
                "item": item,  # 存储控件引用
                "last_update": datetime.now(),  # 记录最后更新时间
                "position": index,  # 记录位置
                "is_valid": True  # 标记是否有效
            }
        except Exception as e:
            logger.error(f"创建会话信息时出错: {e}")
            return None
    
    def get_initial_conversations(self):
        """获取初始的会话列表（仅获取前30个非排除会话）"""
        if not self.find_conversation_list():
            logger.error("无法获取会话列表")
            return False
            
        try:
            logger.info("开始获取初始会话列表...")
            
            # 清空之前的会话数据
            self.conversation_dict = OrderedDict()
            self.all_conversation_items = []
            self.conversation_positions = {}
            valid_conversations = 0
            
            # 确保更新排除关键词
            self.update_exclude_keywords()
            
            # 滚动到顶部
            logger.info("尝试滚动到列表顶部...")
            for _ in range(10):
                if not self.scroll_conversation_list("up"):
                    break
                time.sleep(0.2)
            
            logger.info("从顶部开始获取会话...")
            scroll_attempts = 0
            
            # 滚动并获取会话，直到达到所需数量或无法继续滚动
            while valid_conversations < WeChatConfig.MAX_CONVERSATIONS and scroll_attempts < self.max_scroll_attempts:
                # 获取当前可见的会话项
                current_items = self.conversation_list.GetChildren()
                if not current_items:
                    logger.warning("当前没有可见的会话项")
                    break
                    
                logger.info(f"当前可见会话数: {len(current_items)}")
                
                # 处理当前可见的会话项
                for item in current_items:
                    try:
                        # 获取会话名称
                        name = item.Name if hasattr(item, "Name") else f"会话 {len(self.conversation_dict)}"
                        
                        # 生成会话ID
                        conv_id = self.generate_conversation_id(item)
                        
                        # 如果已处理过该会话或应该被排除，则跳过
                        if conv_id in self.processed_ids or self.should_exclude_conversation(name):
                            continue
                            
                        # 标记为已处理
                        self.processed_ids.add(conv_id)
                        
                        # 创建会话信息
                        conversation_info = self.create_conversation_info(item, valid_conversations)
                        if not conversation_info:
                            continue
                        
                        # 添加到会话字典 - 使用ID作为键
                        self.conversation_dict[conv_id] = conversation_info
                        self.all_conversation_items.append(item)
                        
                        # 更新位置索引
                        self.conversation_positions[conv_id] = valid_conversations
                        
                        # 输出会话信息
                        unread_status = "【有未读】" if conversation_info["has_unread"] else ""
                        logger.info(f"{valid_conversations + 1}. {name} {unread_status}")
                        
                        # 计数有效会话
                        valid_conversations += 1
                        
                        # 如果已达到目标数量，结束处理
                        if valid_conversations >= WeChatConfig.MAX_CONVERSATIONS:
                            break
                            
                    except Exception as e:
                        logger.error(f"处理会话项时出错: {e}")
                
                # 如果已达到目标数量，结束处理
                if valid_conversations >= WeChatConfig.MAX_CONVERSATIONS:
                    break
                    
                # 尝试向下滚动获取更多会话
                if not self.scroll_conversation_list("down"):
                    logger.info("无法再向下滚动，可能已到达列表底部")
                    scroll_attempts += 1
                    
                    if scroll_attempts >= 3:
                        logger.info("多次尝试滚动无效，停止获取")
                        break
                else:
                    # 成功滚动，重置尝试计数
                    scroll_attempts = 0
                
                # 短暂暂停，避免滚动过快
                time.sleep(0.2)
            
            logger.info(f"==== 初始会话列表获取完成 (共 {len(self.conversation_dict)} 个) ====")
            return True
            
        except Exception as e:
            logger.error(f"获取初始会话列表时出错: {e}")
            return False
    
    def get_current_visible_conversations(self):
        """获取当前可见的会话信息"""
        if not self.conversation_list:
            if not self.find_conversation_list():
                logger.error("无法获取会话列表")
                return []
        
        # 获取当前可见的会话项
        current_items = self.conversation_list.GetChildren()
        
        # 收集可见会话信息
        visible_conversations = []
        for i, item in enumerate(current_items):
            try:
                name = item.Name if hasattr(item, "Name") else f"会话 {i}"
                conv_id = self.generate_conversation_id(item)
                
                # 如果应该被排除，则跳过
                if self.should_exclude_conversation(name):
                    continue
                
                # 检查是否有未读
                has_unread = self.check_conversation_unread(item)
                
                visible_conversations.append({
                    "name": name,
                    "id": conv_id,
                    "has_unread": has_unread,
                    "item": item,
                    "index": i
                })
            except Exception as e:
                logger.error(f"获取可见会话 {i} 信息时出错: {e}")
        
        return visible_conversations
    
    def update_conversation_dict(self, visible_conversations):
        """根据当前可见会话更新会话字典
        
        Args:
            visible_conversations: 当前可见的会话列表
            
        Returns:
            bool: 是否有更新
        """
        if not visible_conversations:
            return False
            
        has_updates = False
        current_time = datetime.now()
        
        # 检查所有可见会话
        for i, conv in enumerate(visible_conversations):
            conv_id = conv["id"]
            
            # 如果是新会话
            if conv_id not in self.conversation_dict:
                # 如果已经有30个会话，找到最老的会话移除
                if len(self.conversation_dict) >= WeChatConfig.MAX_CONVERSATIONS:
                    # 找到最老的会话（最后更新时间最早的）
                    oldest_id = None
                    oldest_time = None
                    
                    for cid, cinfo in self.conversation_dict.items():
                        if oldest_time is None or cinfo["last_update"] < oldest_time:
                            oldest_time = cinfo["last_update"]
                            oldest_id = cid
                    
                    # 如果找到最老的会话，移除它
                    if oldest_id:
                        logger.info(f"移除最旧的会话: {self.conversation_dict[oldest_id]['name']}")
                        del self.conversation_dict[oldest_id]
                
                # 添加新会话
                logger.info(f"添加新会话: {conv['name']}")
                
                # 创建会话信息
                conversation_info = {
                    "index": len(self.conversation_dict),
                    "name": conv["name"],
                    "id": conv_id,
                    "has_unread": conv["has_unread"],
                    "item": conv["item"],
                    "last_update": current_time,
                    "position": i,
                    "is_valid": True
                }
                
                # 添加到会话字典
                self.conversation_dict[conv_id] = conversation_info
                has_updates = True
            else:
                # 更新现有会话
                existing_conv = self.conversation_dict[conv_id]
                
                # 检查是否有状态变化
                if existing_conv["has_unread"] != conv["has_unread"] or existing_conv["position"] != i:
                    logger.info(f"更新会话状态: {conv['name']}")
                    
                    # 更新会话信息
                    existing_conv["has_unread"] = conv["has_unread"]
                    existing_conv["position"] = i
                    existing_conv["last_update"] = current_time
                    existing_conv["item"] = conv["item"]  # 更新控件引用
                    has_updates = True
        
        # 如果有更新，重新排序会话字典
        if has_updates:
            # 基于位置排序，确保最新的会话在前面
            sorted_items = sorted(self.conversation_dict.items(), 
                                key=lambda x: x[1]["position"] if "position" in x[1] else float('inf'))
            
            # 重建有序字典
            self.conversation_dict = OrderedDict()
            for i, (conv_id, conv_info) in enumerate(sorted_items):
                conv_info["index"] = i  # 更新索引
                self.conversation_dict[conv_id] = conv_info
        
        return has_updates
    
    def check_conversation_updates(self):
        """检查会话更新状态并更新会话字典"""
        if not self.conversation_dict:
            logger.warning("没有会话可检查")
            return
            
        try:
            logger.info("开始检查会话更新状态...")
            
            # 确保会话列表存在
            if not self.conversation_list or not self.conversation_list.Exists():
                if not self.find_conversation_list():
                    logger.error("无法找到会话列表")
                    return
                    
            # 滚动到顶部以获取最新的会话
            logger.info("尝试滚动到列表顶部...")
            for _ in range(5):
                if not self.scroll_conversation_list("up"):
                    break
                time.sleep(0.2)
            
            # 获取当前可见的会话
            visible_conversations = self.get_current_visible_conversations()
            if not visible_conversations:
                logger.warning("未获取到可见会话")
                return
                
            # 更新会话字典
            has_updates = self.update_conversation_dict(visible_conversations)
            
            # 检查未读消息
            unread_count = 0
            checked_count = 0
            
            # 遍历会话字典
            for conv_id, conv in self.conversation_dict.items():
                if conv["has_unread"]:
                    unread_count += 1
                    logger.info(f"[通知] 检测到未读会话: {conv['name']}")
                    
                    # 如果启用了自动点击，点击会话查看内容
                    if self.auto_click_enabled and hasattr(conv["item"], "Click"):
                        if self.click_conversation(conv["item"]):
                            messages = self.get_conversation_messages(conv)
                            if messages:
                                logger.info(f"==== {conv['name']} 的最新消息 ====")
                                for i, msg in enumerate(messages):
                                    logger.info(f"  {i+1}. {msg}")
                
                checked_count += 1
            
            if has_updates:
                logger.info(f"会话列表已更新，现在有 {len(self.conversation_dict)} 个会话")
            
            logger.info(f"会话检查完成: 监控 {checked_count} 个会话，发现 {unread_count} 个有未读消息")
            
        except Exception as e:
            logger.error(f"检查会话更新状态时出错: {e}")
    
    def print_conversation_stats(self):
        """打印会话统计信息"""
        if not self.conversation_dict:
            logger.info("没有会话数据")
            return
            
        logger.info(f"\n==== 会话统计 (共 {len(self.conversation_dict)} 个) ====")
        logger.info(f"监控时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 计算未读会话数
        unread_count = sum(1 for conv in self.conversation_dict.values() if conv["has_unread"])
        logger.info(f"未读会话数: {unread_count}")
        
        # 打印前5个会话
        top_convs = list(self.conversation_dict.values())[:5]
        logger.info("前5个会话:")
        for i, conv in enumerate(top_convs):
            unread_mark = "【有未读】" if conv["has_unread"] else ""
            logger.info(f"  {i+1}. {conv['name']} {unread_mark}")
    
    def monitor(self, check_interval=5):
        """开始监控微信会话列表
        
        Args:
            check_interval: 检查间隔，单位为秒
        """
        logger.info("开始监控微信会话列表...")
        
        # 初始计数器，用于定期输出心跳信息
        heartbeat_counter = 0
        
        # 确保更新排除关键词
        self.update_exclude_keywords()
        
        # 首先获取初始会话列表
        if not self.get_initial_conversations():
            logger.error("初始化会话列表失败")
            return
        
        while True:
            try:
                # 确保微信窗口存在
                if not self.wechat_window or not self.wechat_window.Exists():
                    if not self.find_wechat_window():
                        logger.warning("微信窗口不存在，等待下一次检查...")
                        time.sleep(check_interval)
                        continue
                
                # 心跳信息，让用户知道脚本在运行
                heartbeat_counter += 1
                if heartbeat_counter % 12 == 0:  # 大约每分钟输出一次
                    logger.info("监控脚本正在运行中...")
                    self.print_conversation_stats()
                
                # 检查会话更新状态
                self.check_conversation_updates()
                
                # 等待下一次检查
                time.sleep(check_interval)
                
            except Exception as e:
                logger.error(f"监控过程中出错: {e}")
                time.sleep(check_interval)
            except KeyboardInterrupt:
                logger.info("用户中断，退出监控")
                break

    def check_send_button_exists(self):
        """检查当前打开的聊天窗口是否存在发送按钮
        
        Returns:
            bool: 是否存在发送按钮
        """
        try:
            # 寻找聊天窗口区域
            chat_area = None
            for pane in self.wechat_window.GetChildren():
                if pane.ControlTypeName == "PaneControl":
                    chat_area = pane
                    break
                    
            if not chat_area:
                logger.warning("未找到聊天窗口区域")
                return False
                
            # 查找可能的发送按钮 - 通常是一个按钮控件
            # 微信发送按钮通常有几种形式：文本为"发送"的按钮或特定的图标按钮
            send_button = None
            
            # 方法1：直接寻找发送按钮
            try:
                send_button = chat_area.ButtonControl(Name="发送")
                if send_button.Exists(1):
                    logger.info("找到发送按钮 (Name='发送')")
                    return True
            except:
                pass
                
            # 方法2：查找所有按钮，检查可能的发送按钮
            try:
                all_buttons = []
                # 递归查找所有按钮
                def find_buttons(control):
                    if control.ControlTypeName == "ButtonControl":
                        all_buttons.append(control)
                    
                    children = control.GetChildren()
                    for child in children:
                        find_buttons(child)
                
                # 从聊天区域开始查找按钮
                find_buttons(chat_area)
                
                # 检查是否有发送按钮特征
                for button in all_buttons:
                    # 检查按钮名称
                    if hasattr(button, "Name") and button.Name:
                        name = button.Name.lower()
                        if "发送" in name or "send" in name:
                            logger.info(f"找到可能的发送按钮: {name}")
                            return True
                    
                    # 检查按钮大小和位置（发送按钮通常在窗口底部并且有一定大小）
                    if hasattr(button, "BoundingRectangle"):
                        rect = button.BoundingRectangle
                        # 假设窗口底部的按钮可能是发送按钮
                        if rect.bottom > (chat_area.BoundingRectangle.bottom - 100):
                            logger.info("找到可能的发送按钮(根据位置判断)")
                            return True
            except Exception as e:
                logger.error(f"查找发送按钮出错: {e}")
            
            # 方法3：尝试查找输入框，通常输入框旁边就是发送按钮
            try:
                # 查找编辑框或富文本编辑框
                edit_box = chat_area.EditControl()
                if edit_box.Exists(1):
                    logger.info("找到输入框，可能是聊天界面")
                    return True
                    
                rich_edit = chat_area.DocumentControl()
                if rich_edit.Exists(1):
                    logger.info("找到富文本输入框，可能是聊天界面")
                    return True
            except:
                pass
                
            logger.info("未找到发送按钮，可能是公众号或其他非聊天界面")
            return False
            
        except Exception as e:
            logger.error(f"检查发送按钮时出错: {e}")
            return False
            
    def validate_conversation(self, conv_id):
        """验证会话是否有效（是否是可聊天的会话而非公众号）
        
        Args:
            conv_id: 会话ID
            
        Returns:
            bool: 是否为有效会话
        """
        if conv_id not in self.conversation_dict:
            logger.warning(f"会话ID {conv_id} 不存在")
            return False
            
        try:
            conv = self.conversation_dict[conv_id]
            logger.info(f"正在验证会话: {conv['name']}")
            
            # 尝试点击会话
            item = conv["item"]
            if not item or not hasattr(item, "Click"):
                logger.warning(f"会话项无效，无法点击")
                return False
                
            # 记录实际要点击的控件名称
            actual_name = item.Name if hasattr(item, "Name") else "未知会话"
            logger.info(f"实际点击的会话控件名称: {actual_name}")
            
            # 确保微信窗口处于活动状态
            self.wake_up_window()
            time.sleep(0.5)
            
            # 增强版点击会话功能
            if not self.enhanced_click_conversation(item):
                logger.warning(f"点击会话 '{conv['name']}' 失败")
                return False
                
            # 等待界面加载，延长等待时间
            time.sleep(2)
            
            # 检查是否有发送按钮
            is_valid = self.enhanced_check_send_button()
            
            # 更新会话信息
            conv["is_valid"] = is_valid
            
            # 记录验证结果
            status = "有效会话" if is_valid else "无效会话(可能是公众号)"
            logger.info(f"会话 '{conv['name']}' 验证结果: {status}")
            
            return is_valid
            
        except Exception as e:
            logger.error(f"验证会话 {conv_id} 时出错: {e}")
            return False
            
    def enhanced_click_conversation(self, conversation_item):
        """增强版点击会话方法
        
        Args:
            conversation_item: 会话控件
            
        Returns:
            bool: 是否成功点击
        """
        if not self.auto_click_enabled:
            logger.info("自动点击功能已禁用，跳过点击操作")
            return False
            
        if not conversation_item:
            logger.warning("无效的会话项，无法点击")
            return False
            
        try:
            name = conversation_item.Name if hasattr(conversation_item, "Name") else "未知会话"
            logger.info(f"尝试增强点击会话: {name}")
            
            # 尝试多种点击方式
            success = False
            
            # 方法1: 使用控件的Click方法
            try:
                # 确保聚焦在会话项上
                conversation_item.SetFocus()
                time.sleep(0.3)
                
                # 使用模拟移动的点击方式
                conversation_item.Click(simulateMove=True)
                logger.info(f"方法1: 已尝试点击会话 '{name}'")
                success = True
            except Exception as e:
                logger.warning(f"方法1点击失败: {e}")
                
            # 如果方法1失败，尝试方法2
            if not success:
                try:
                    # 获取坐标并点击
                    rect = conversation_item.BoundingRectangle
                    center_x = int(rect.left + (rect.right - rect.left) * 0.5)
                    center_y = int(rect.top + (rect.bottom - rect.top) * 0.5)
                    
                    # 确保窗口活动
                    self.wechat_window.SetActive()
                    time.sleep(0.2)
                    
                    # 模拟鼠标移动并点击
                    auto.MoveTo(center_x, center_y, duration=0.1)
                    time.sleep(0.2)
                    auto.Click(center_x, center_y)
                    logger.info(f"方法2: 已通过坐标点击会话 '{name}' 在位置 ({center_x}, {center_y})")
                    success = True
                except Exception as e:
                    logger.warning(f"方法2点击失败: {e}")
            
            # 如果方法2失败，尝试方法3
            if not success:
                try:
                    # 使用父元素点击
                    parent = conversation_item.GetParentControl()
                    if parent:
                        parent.Click()
                        logger.info(f"方法3: 已尝试点击会话 '{name}' 的父元素")
                        success = True
                except Exception as e:
                    logger.warning(f"方法3点击失败: {e}")
            
            # 等待点击响应
            time.sleep(1)
            
            # 验证点击是否成功 - 检查聊天窗口区域变化
            chat_area = None
            for pane in self.wechat_window.GetChildren():
                if pane.ControlTypeName == "PaneControl":
                    chat_area = pane
                    break
            
            if chat_area:
                logger.info(f"点击后找到了聊天窗口区域")
            else:
                logger.warning(f"点击后未找到聊天窗口区域，可能点击失败")
                
            return success
        except Exception as e:
            logger.error(f"增强点击会话过程中出错: {e}")
            return False
    
    def enhanced_check_send_button(self):
        """增强版检查发送按钮存在性
        
        Returns:
            bool: 是否存在发送按钮
        """
        try:
            # 获取窗口标题，判断是否含有公众号或订阅号文字
            window_title = ''
            try:
                for title_control in self.wechat_window.GetChildren():
                    if title_control.ControlTypeName == "TitleBarControl" and hasattr(title_control, "Name"):
                        window_title = title_control.Name
                        logger.info(f"当前窗口标题: {window_title}")
                        if '公众号' in window_title or '订阅号' in window_title:
                            logger.info(f"通过标题检测到公众号: {window_title}")
                            return False
            except Exception as e:
                logger.warning(f"获取窗口标题出错: {e}")
                
            # 检查聊天区域特征
            chat_area = None
            chat_areas = []
            for control in self.wechat_window.GetChildren():
                if control.ControlTypeName == "PaneControl":
                    chat_areas.append(control)
                    
            if not chat_areas:
                logger.warning("未找到聊天区域")
                return False
                
            # 找到底部的输入区域和发送按钮
            bottom_panel = None
            for area in chat_areas:
                # 尝试找到底部的面板
                children = area.GetChildren()
                if children:
                    # 通常底部面板是最后的子元素
                    bottom_candidate = children[-1]
                    if bottom_candidate.ControlTypeName in ["PaneControl", "CustomControl"]:
                        bottom_panel = bottom_candidate
                        break
            
            # 输出调试信息
            if bottom_panel:
                logger.info(f"找到底部面板: {bottom_panel.ControlTypeName}")
                # 列出底部面板的所有子控件
                for child in bottom_panel.GetChildren():
                    logger.info(f"底部面板子控件: {child.ControlTypeName} - {child.Name if hasattr(child, 'Name') else '无名称'}")
                    
                    # 查找发送按钮 - 通常是最右侧的按钮
                    if child.ControlTypeName == "ButtonControl":
                        button_name = child.Name if hasattr(child, "Name") else ""
                        if "发送" in button_name or button_name == "":  # 微信发送按钮可能没有文字
                            logger.info(f"找到疑似发送按钮: {button_name}")
                            return True
                            
                    # 检查是否有文本输入区域和表情按钮 - 这些是聊天窗口的标志
                    if child.ControlTypeName in ["EditControl", "DocumentControl"]:
                        logger.info(f"找到输入区域: {child.ControlTypeName}")
                        
                        # 公众号通常只有一个假的输入框，不能真正输入文字
                        # 进一步检查是否可以设置焦点或者输入文字
                        try:
                            child.SetFocus()
                            time.sleep(0.2)
                            # 尝试输入一个空格然后删除 - 如果成功说明是真的输入框
                            has_input_focus = False
                            try:
                                # 检查是否获得了焦点
                                focused_element = auto.GetFocusedElement()
                                if focused_element and focused_element == child:
                                    has_input_focus = True
                                    logger.info("输入区域成功获得焦点，应该是正常聊天窗口")
                                    return True
                            except:
                                pass
                                
                            if not has_input_focus:
                                logger.info("输入区域无法获得焦点，可能是公众号界面")
                                
                        except Exception as e:
                            logger.warning(f"测试输入区域时出错: {e}")
                            
            # 检查公众号特有标记
            try:
                # 公众号通常底部有"公众号"、"功能"等标签页
                tab_indicators = ["公众号", "功能", "小程序", "订阅号"]
                all_texts = []
                
                def collect_text_controls(control):
                    if control.ControlTypeName == "TextControl" and hasattr(control, "Name"):
                        all_texts.append(control.Name)
                    for child in control.GetChildren():
                        collect_text_controls(child)
                
                # 从窗口收集所有文本
                collect_text_controls(self.wechat_window)
                
                # 检查是否包含公众号标记
                for text in all_texts:
                    for indicator in tab_indicators:
                        if indicator in text:
                            logger.info(f"找到公众号标记: {text}")
                            return False
            except Exception as e:
                logger.warning(f"检查公众号标记时出错: {e}")
            
            # 特征检查：对于个人和群聊，通常有"发起语音通话"按钮
            try:
                call_button = self.wechat_window.ButtonControl(Name="发起语音通话")
                if call_button.Exists(1):
                    logger.info("找到'发起语音通话'按钮，应该是正常聊天窗口")
                    return True
            except:
                pass
                
            # 特征检查：个人聊天通常有"视频通话"按钮
            try:
                video_button = self.wechat_window.ButtonControl(Name="视频通话")
                if video_button.Exists(1):
                    logger.info("找到'视频通话'按钮，应该是正常聊天窗口")
                    return True
            except:
                pass
                
            # 如果以上特征检查都没有明确结果，尝试检查更多通用特征
            
            # 公众号通常底部只有一个不可编辑的输入提示框
            # 而个人聊天和群聊有真正的输入框和多个功能按钮
            buttons_count = 0
            edit_controls_count = 0
            
            if bottom_panel:
                for control in bottom_panel.GetChildren():
                    if control.ControlTypeName == "ButtonControl":
                        buttons_count += 1
                    elif control.ControlTypeName in ["EditControl", "DocumentControl"]:
                        edit_controls_count += 1
            
            logger.info(f"底部区域按钮数量: {buttons_count}, 输入框数量: {edit_controls_count}")
            
            # 个人聊天和群聊通常在底部有3个以上的按钮(表情、语音、更多等)
            if buttons_count >= 3:
                logger.info("按钮数量符合正常聊天窗口特征")
                return True
                
            # 公众号通常底部有且仅有一个输入提示框，而真正聊天界面有输入框和其他按钮
            if edit_controls_count == 1 and buttons_count <= 1:
                logger.info("界面特征符合公众号")
                return False
                
            # 如果还不确定，则检查是否有其他特征表明这是聊天界面
            try:
                # 尝试找到表情按钮或者附件按钮，这些在正常聊天窗口中存在
                emoji_button = bottom_panel.ButtonControl(Name="表情")
                if emoji_button.Exists(1):
                    logger.info("找到表情按钮，应该是正常聊天窗口")
                    return True
                    
                attach_button = bottom_panel.ButtonControl(Name="附件")
                if attach_button.Exists(1):
                    logger.info("找到附件按钮，应该是正常聊天窗口")
                    return True
            except:
                pass
                
            # 所有检查都不确定，默认判断为无效
            logger.warning("无法确定界面类型，默认认为不是聊天界面")
            return False
            
        except Exception as e:
            logger.error(f"检查发送按钮时出错: {e}")
            return False
            
    def collect_conversation_details(self, conv_id):
        """收集会话的详细信息
        
        Args:
            conv_id: 会话ID
        """
        if conv_id not in self.conversation_dict:
            return
            
        try:
            conv = self.conversation_dict[conv_id]
            detail_info = {
                "id": conv_id,
                "name": conv["name"],
                "is_valid": conv.get("is_valid", "未验证"),
                "has_unread": conv.get("has_unread", False),
                "unread_count": conv.get("unread_count", 0),
                "position": conv.get("position", -1),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            
            # 存储详细信息
            if not hasattr(self, "conversation_details"):
                self.conversation_details = {}
                
            self.conversation_details[conv_id] = detail_info
            logger.info(f"已收集会话 '{conv['name']}' 的详细信息")
            
        except Exception as e:
            logger.error(f"收集会话详细信息时出错: {e}")
            
    def validate_all_conversations(self, max_count=30):
        """验证所有会话的有效性
        
        Args:
            max_count: 最多验证的会话数
        """
        if not self.conversation_dict:
            logger.warning("没有会话可验证")
            return
            
        try:
            logger.info("====== 开始验证所有会话 ======")
            
            # 确保微信窗口激活
            if not self.find_wechat_window():
                logger.error("无法找到微信窗口")
                return
                
            # 获取最新的可见会话列表，确保验证的是当前可见的会话
            visible_conversations = self.get_current_visible_conversations()
            if not visible_conversations:
                logger.error("无法获取可见会话列表")
                return
                
            logger.info(f"获取到 {len(visible_conversations)} 个当前可见会话")
            
            # 记录验证结果
            valid_count = 0
            invalid_count = 0
            count = 0
            
            # 构建实际要验证的会话列表 - 从当前可见会话开始
            to_validate = []
            
            # 添加可见会话到验证列表
            for conv in visible_conversations:
                # 我们现在知道每个会话是一个字典，包含item属性
                if isinstance(conv, dict) and 'item' in conv and conv['item']:
                    # 获取会话名称
                    name = conv.get('name', '未知会话')
                    logger.info(f"处理会话: {name}")
                    
                    # 添加到验证列表
                    to_validate.append(conv)
                else:
                    logger.warning(f"无效的会话项: {conv}")
                
                # 如果达到最大数量，停止添加
                if len(to_validate) >= max_count:
                    break
            
            logger.info(f"准备验证 {len(to_validate)} 个会话")
            
            # 反转顺序，从底部往上验证（即从当前可见的最后一个会话开始）
            to_validate.reverse()
            logger.info("已将验证顺序反转，从底部往上验证会话")
            
            # 遍历会话列表进行验证
            for i, conv in enumerate(to_validate):
                try:
                    count += 1
                    
                    # 获取会话名称和项目
                    name = conv.get('name', '未知会话')
                    item = conv.get('item')
                    
                    if not item:
                        logger.warning(f"会话 '{name}' 没有有效的控件项")
                        continue
                    
                    # 打印完整会话信息用于调试
                    logger.info(f"-------- 会话 {count}/{len(to_validate)} --------")
                    logger.info(f"会话名称: {name}")
                    logger.info(f"控件类型: {type(item).__name__}")
                    
                    # 验证会话
                    is_valid = self.validate_conversation_item(item, name)
                    
                    # 更新原始会话数据
                    conv['is_valid'] = is_valid
                    
                    # 统计结果
                    if is_valid:
                        valid_count += 1
                    else:
                        invalid_count += 1
                        
                    # 短暂暂停，避免操作过快
                    time.sleep(random.uniform(1.5, 3.0))
                    
                except Exception as e:
                    logger.error(f"验证会话时出错: {e}")
            
            # 输出验证结果摘要
            logger.info(f"====== 会话验证完成 ======")
            logger.info(f"验证会话数: {count}")
            logger.info(f"有效会话数: {valid_count}")
            logger.info(f"无效会话数: {invalid_count}")
            
            # 保存会话详细信息
            self.save_validation_results(to_validate)
            
        except Exception as e:
            logger.error(f"验证所有会话时出错: {e}")
            
    def validate_conversation_item(self, conversation_item, name):
        """直接验证会话项是否有效（是否是可聊天的会话而非公众号）
        
        Args:
            conversation_item: 会话控件
            name: 会话名称
            
        Returns:
            bool: 是否为有效会话
        """
        if not conversation_item or not isinstance(conversation_item, auto.Control):
            logger.warning(f"会话项 '{name}' 无效，无法点击")
            return False
            
        try:
            logger.info(f"正在验证会话: {name}")
            
            # 确保微信窗口处于活动状态
            self.wake_up_window()
            time.sleep(0.5)
            
            # 点击会话打开聊天窗口
            if not self.click_conversation_new(conversation_item, name):
                logger.warning(f"点击会话 '{name}' 失败")
                return False
                
            # 等待界面加载，延长等待时间
            time.sleep(2)
            
            # 检查是否有发送按钮
            is_valid = self.enhanced_check_send_button()
            
            # 记录验证结果
            status = "有效会话" if is_valid else "无效会话(可能是公众号)"
            logger.info(f"会话 '{name}' 验证结果: {status}")
            
            # 收集更多信息
            if not hasattr(self, "validation_results"):
                self.validation_results = []
                
            self.validation_results.append({
                "name": name,
                "is_valid": is_valid,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            
            return is_valid
            
        except Exception as e:
            logger.error(f"验证会话 '{name}' 时出错: {e}")
            return False
            
    def click_conversation_new(self, conversation_item, name):
        """改进的会话点击方法，借鉴WeChatMassTool的实现
        
        Args:
            conversation_item: 会话控件
            name: 会话名称
            
        Returns:
            bool: 是否成功点击
        """
        if not self.auto_click_enabled:
            logger.info("自动点击功能已禁用，跳过点击操作")
            return False
            
        if not conversation_item or not isinstance(conversation_item, auto.Control):
            logger.warning(f"会话项 '{name}' 无效，无法点击")
            return False
            
        try:
            logger.info(f"尝试点击会话: {name}")
            
            # 确保控件存在
            if not conversation_item.Exists(1):
                logger.warning(f"会话控件 '{name}' 不存在")
                return False
                
            # 确保窗口活动
            self.wechat_window.SetTopmost(True)
            self.wechat_window.SetActive()
            time.sleep(0.3)
            
            # 方法1: 直接点击控件
            try:
                # 先设置焦点
                conversation_item.SetFocus()
                time.sleep(0.3)
                
                # 执行点击
                conversation_item.Click(simulateMove=False)
                logger.info(f"方法1: 已点击会话 '{name}'")
                time.sleep(0.5)
                return True
            except Exception as e:
                logger.warning(f"方法1点击失败: {e}")
                
            # 方法2: 使用坐标点击
            try:
                # 获取控件中心点坐标
                rect = conversation_item.BoundingRectangle
                center_x = int(rect.left + (rect.right - rect.left) * 0.5)
                center_y = int(rect.top + (rect.bottom - rect.top) * 0.5)
                
                # 模拟鼠标移动和点击
                auto.MoveTo(center_x, center_y, duration=0.1)
                time.sleep(0.2)
                auto.Click(center_x, center_y)
                logger.info(f"方法2: 已通过坐标点击会话 '{name}' 在位置 ({center_x}, {center_y})")
                time.sleep(0.5)
                return True
            except Exception as e:
                logger.warning(f"方法2点击失败: {e}")
                
            # 方法3: 使用键盘选择和回车
            try:
                # 先设置焦点
                conversation_item.SetFocus()
                time.sleep(0.3)
                
                # 使用回车键选择
                self.wechat_window.SendKey(auto.Keys.VK_RETURN)
                logger.info(f"方法3: 已使用键盘选择会话 '{name}'")
                time.sleep(0.5)
                return True
            except Exception as e:
                logger.warning(f"方法3点击失败: {e}")
                
            logger.error(f"所有点击方法均失败")
            return False
            
        except Exception as e:
            logger.error(f"点击会话 '{name}' 过程中出错: {e}")
            return False
            
    def save_validation_results(self, validated_items):
        """保存验证结果到文件
        
        Args:
            validated_items: 已验证的会话列表
        """
        try:
            # 创建时间戳
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"conversation_validation_{timestamp}.txt"
            
            with open(filename, "w", encoding="utf-8") as f:
                f.write("===== 微信会话验证结果 =====\n")
                f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"总验证会话数: {len(validated_items)}\n\n")
                
                # 统计有效和无效会话
                valid_count = sum(1 for item in validated_items if item.get('is_valid', False))
                invalid_count = len(validated_items) - valid_count
                
                f.write(f"有效会话数: {valid_count}\n")
                f.write(f"无效会话数: {invalid_count}\n\n")
                
                # 写入每个验证过的会话信息
                for i, item in enumerate(validated_items):
                    name = item.get('name', '未知会话')
                    is_valid = item.get('is_valid', False)
                    
                    status = "有效会话" if is_valid else "无效会话(可能是公众号)"
                    f.write(f"{i+1}. {name} - {status}\n")
                    
                    # 写入其他信息
                    for key, value in item.items():
                        if key not in ['name', 'is_valid', 'item']:
                            f.write(f"   {key}: {value}\n")
                    
                    f.write("\n")
            
            logger.info(f"会话验证结果已保存到文件: {filename}")
            
        except Exception as e:
            logger.error(f"保存会话验证结果时出错: {e}")


if __name__ == "__main__":
    try:
        logger.info("启动微信监控脚本")
        monitor = WeChatMonitor()
        # 可以在这里设置用户名
        monitor.username = "Do you want to continue ?"
        
        # 是否启用自动点击功能（默认禁用）
        monitor.auto_click_enabled = True  # 修改为True，以便可以点击会话
        
        # 尝试查找微信窗口
        if monitor.find_wechat_window():
            # 获取初始会话列表
            if monitor.get_initial_conversations():
                # 验证所有会话（最多验证30个）
                monitor.validate_all_conversations(max_count=30)
                
                # 如果需要继续监控，取消下面的注释
                # monitor.monitor(check_interval=5)
            else:
                logger.error("无法获取初始会话列表")
        else:
            logger.error("无法找到微信窗口，请确保微信已经登录并正在运行")
    except KeyboardInterrupt:
        logger.info("用户中断，退出监控")
    except Exception as e:
        logger.error(f"程序运行出错: {e}")
