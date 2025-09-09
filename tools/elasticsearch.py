from collections.abc import Generator
from typing import Any

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage

# 导入 logging 和自定义处理器
import logging
from dify_plugin.config.logger_format import plugin_logger_handler

import requests
import json
import time

# 使用自定义处理器设置日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(plugin_logger_handler)

class ElasticsearchTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:

        es_server = tool_parameters.get("SERVER")
        es_index = tool_parameters.get("INDEX") + "_" + time.strftime("%Y.%m.%d", time.localtime())
        es_conversation_id = tool_parameters.get("CONVERSATION_ID", str(int(time.time() * 1000)))
        es_questions = tool_parameters.get("QUESTIONS")
        es_answer = tool_parameters.get("ANSWER")
        es_data = tool_parameters.get("DATA")

        es_conversation_id = es_conversation_id + "_" + str(int(time.time() * 1000))

        # 构建 Elasticsearch URL
        url = f"{es_server}/{es_index}/{es_index}/_bulk"

        qa_object = {
            "query": es_questions,
            "@timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        }

        start_tag = "<business>"
        end_tag = "</business>"
        has_start_tag = start_tag in es_answer
        has_end_tag = end_tag in es_answer

        if has_start_tag and has_end_tag:
            start_idx = es_answer.find(start_tag)
            end_idx = es_answer.find(end_tag)
            # 拆分内容
            answer = es_answer[:start_idx]  # strip()可选，去除首尾多余空格
            extended = es_answer[start_idx + len(start_tag): end_idx]
            extended_json = json.loads(extended)
            qa_object["answer"] = answer
            qa_object["preset"] = "true"
            qa_object.update(extended_json)
        else:
            qa_object["preset"] = "false"
            qa_object["answer"] = es_answer

        if es_data:
            try:
                other_dict = json.loads(es_data)
                qa_object.update(other_dict)
            except json.JSONDecodeError as e:
                result = {
                    "success": False,
                    "message": f"解析 DATA JSON 失败: {str(e)}",
                    "response": None
                }
                yield self.create_json_message(result)

        # 转换回 JSON 字符串
        payload = json.dumps(qa_object, ensure_ascii=False)

        command = {
            "index": {
                "_id":es_conversation_id
            }
        }
        command = json.dumps(command, ensure_ascii=False)

        # 构造 bulk 请求数据
        data = f"{command}\r\n{payload}\r\n"
        logger.info(f"发送数据到 Elasticsearch: {data}")
        # 设置请求头
        headers = {
            "Content-Type": "application/json"
        }

        try:
            # 发送数据到 Elasticsearch
            response = requests.post(url, headers=headers, data=data, timeout=30)
            logger.info(f"Elasticsearch 响应: {response.status_code} {response.reason}")
            response.raise_for_status()
            result = {
                "success": True,
                "message": "数据已成功保存到 Elasticsearch",
                "response": response.json()
            }
            yield self.create_json_message(result)
        except requests.exceptions.RequestException as e:
            logger.error(f"发送数据到 Elasticsearch 失败: {str(e)}")
            result = {
                "success": False,
                "message": f"发送数据到 Elasticsearch 失败: {str(e)}",
                "response": None
            }
            yield self.create_json_message(result)
        except Exception as e:
            logger.error(f"处理 Elasticsearch 请求时出错: {str(e)}")
            result = {
                "success": False,
                "message": f"处理 Elasticsearch 请求时出错: {str(e)}",
                "response": None
            }
            yield self.create_json_message(result)