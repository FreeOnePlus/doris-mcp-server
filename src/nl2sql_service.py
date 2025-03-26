"""
Apache Doris NL2SQL 服务主入口

整合LLM调用、元数据提取和NL2SQL转换功能
"""

import os
import sys
import json
import logging
import argparse
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# 导入相关模块
from src.utils.llm_client import get_llm_client, Message, LLMProvider
from src.utils.metadata_extractor import MetadataExtractor
from src.utils.nl2sql_processor import NL2SQLProcessor
from src.utils.db import execute_query, execute_query_df

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()

class NL2SQLService:
    """
    NL2SQL服务
    
    整合所有功能的主服务类
    """
    
    def __init__(self, db_name: Optional[str] = None, auto_refresh_metadata: bool = False):
        """
        初始化NL2SQL服务
        
        Args:
            db_name: 数据库名称，如果为None则从环境变量获取
            auto_refresh_metadata: 是否在初始化时自动刷新元数据，默认为False
        """
        self.db_name = db_name or os.getenv("DB_DATABASE", "")
        
        # 初始化元数据提取器
        self.metadata_extractor = MetadataExtractor(self.db_name)
        
        # 初始化NL2SQL处理器
        self.nl2sql_processor = NL2SQLProcessor(self.db_name)
        
        # 服务配置
        self.max_token_limit = int(os.getenv("MAX_TOKEN_LIMIT", "4000"))
        self.refresh_metadata_interval = int(os.getenv("REFRESH_METADATA_INTERVAL", "86400"))  # 默认1天
        self.llm_provider = os.getenv("LLM_PROVIDER", "openai")
        
        # 如果设置了自动刷新，则刷新元数据
        if auto_refresh_metadata:
            self._refresh_metadata(force=False)
            logger.info("服务初始化时完成元数据增量刷新")
        else:
            logger.info("跳过初始化时的元数据刷新")
    
    def _refresh_metadata(self, force: bool = False):
        """
        刷新并保存元数据
        
        Args:
            force: 是否强制执行全量刷新，默认为False(增量刷新)
        """
        try:
            logger.info(f"正在{'全量' if force else '增量'}刷新元数据...")
            
            # 使用增量刷新替代全量刷新，除非指定了force=True
            self.metadata_extractor.refresh_metadata(self.db_name, force=force)
            
            logger.info("元数据刷新完成")
        except Exception as e:
            logger.error(f"刷新元数据时出错: {str(e)}")
    
    def process_query(self, query: str) -> Dict[str, Any]:
        """
        处理自然语言查询
        
        Args:
            query: 自然语言查询
            
        Returns:
            Dict[str, Any]: 包含SQL、执行结果和元数据的字典
        """
        try:
            # 记录查询
            logger.info(f"收到查询: {query}")
            
            # 不再每次查询时刷新元数据
            # 直接处理查询
            result = self.nl2sql_processor.process(query)
            
            # 简化结果以减少token数量（如果需要）
            if result.get('success') and 'data' in result:
                data_size = len(json.dumps(result['data']))
                if data_size > self.max_token_limit:
                    # 只保留部分数据
                    truncated_data = result['data'][:20]  # 只保留前20条
                    result['data'] = truncated_data
                    result['truncated'] = True
                    result['original_row_count'] = result.get('row_count', 0)
                    result['displayed_row_count'] = len(truncated_data)
            
            return result
        except Exception as e:
            logger.error(f"处理查询时出错: {str(e)}")
            return {
                'success': False,
                'message': f'处理查询时出错: {str(e)}',
                'query': query
            }
    
    def get_available_llm_providers(self) -> List[str]:
        """
        获取可用的LLM提供商
        
        Returns:
            List[str]: 可用的LLM提供商列表
        """
        providers = []
        
        # 检查各提供商是否有API密钥
        for provider in LLMProvider:
            env_key = f"{provider.value.upper()}_API_KEY"
            if provider == LLMProvider.OLLAMA or os.getenv(env_key):
                providers.append(provider.value)
        
        return providers
    
    def set_llm_provider(self, provider_name: str) -> bool:
        """
        设置LLM提供商
        
        Args:
            provider_name: 提供商名称
            
        Returns:
            bool: 是否设置成功
        """
        try:
            # 验证提供商是否有效
            providers = self.get_available_llm_providers()
            if provider_name.lower() not in [p.lower() for p in providers]:
                return False
            
            # 更新环境变量
            os.environ["LLM_PROVIDER"] = provider_name
            self.llm_provider = provider_name
            
            logger.info(f"已切换到LLM提供商: {provider_name}")
            return True
        except Exception as e:
            logger.error(f"设置LLM提供商时出错: {str(e)}")
            return False
    
    def explain_table(self, table_name: str) -> Dict[str, Any]:
        """
        解释表结构
        
        Args:
            table_name: 表名
            
        Returns:
            Dict[str, Any]: 表结构解释
        """
        try:
            # 获取表结构
            schema = self.metadata_extractor.get_table_schema(table_name, self.db_name)
            
            if not schema:
                return {
                    'success': False,
                    'message': f'未找到表 {table_name}'
                }
            
            # 获取表关系
            relationships = self.metadata_extractor.get_table_relationships()
            table_relations = [r for r in relationships if r.get('table') == table_name or r.get('references_table') == table_name]
            
            # 获取业务元数据
            business_metadata = self.metadata_extractor.summarize_business_metadata(self.db_name)
            
            # 查找表的业务描述
            table_description = ""
            if business_metadata and 'tables_summary' in business_metadata:
                for table_info in business_metadata['tables_summary']:
                    if table_info.get('name') == table_name:
                        table_description = table_info.get('description', '')
                        break
            
            # 构建结果
            result = {
                'success': True,
                'table_name': table_name,
                'table_comment': schema.get('table_comment', ''),
                'business_description': table_description,
                'columns': schema.get('columns', []),
                'relationships': table_relations
            }
            
            return result
        except Exception as e:
            logger.error(f"解释表时出错: {str(e)}")
            return {
                'success': False,
                'message': f'解释表时出错: {str(e)}'
            }
    
    def list_tables(self) -> Dict[str, Any]:
        """
        列出数据库中的表
        
        Returns:
            Dict[str, Any]: 表列表
        """
        try:
            tables = self.metadata_extractor.get_database_tables(self.db_name)
            
            # 收集表的更多信息
            tables_info = []
            for table_name in tables:
                schema = self.metadata_extractor.get_table_schema(table_name, self.db_name)
                table_info = {
                    'name': table_name,
                    'comment': schema.get('table_comment', ''),
                    'column_count': len(schema.get('columns', []))
                }
                tables_info.append(table_info)
            
            # 将结果缓存起来，便于后续快速访问
            self._cached_tables = tables_info
            self._cached_table_count = len(tables)
            
            return {
                'success': True,
                'database': self.db_name,
                'tables': tables_info,
                'count': len(tables)
            }
        except Exception as e:
            logger.error(f"列出表时出错: {str(e)}")
            return {
                'success': False,
                'message': f'列出表时出错: {str(e)}'
            }
    
    def get_doris_version(self) -> str:
        """
        获取Doris数据库版本信息
        
        Returns:
            str: Doris版本信息
        """
        try:
            from src.utils.db import execute_query
            result = execute_query("SELECT VERSION() as version")
            if result and result[0]:
                return result[0].get('version', 'unknown')
            return 'unknown'
        except Exception as e:
            logger.error(f"获取Doris版本时出错: {str(e)}")
            return 'unknown'
    
    def get_server_time(self) -> str:
        """
        获取数据库服务器当前时间
        
        Returns:
            str: 服务器当前时间
        """
        try:
            from src.utils.db import execute_query
            result = execute_query("SELECT NOW() as current_time")
            if result and result[0]:
                return str(result[0].get('current_time', ''))
            return ''
        except Exception as e:
            logger.error(f"获取服务器时间时出错: {str(e)}")
            return ''
    
    def get_cached_table_count(self) -> int:
        """
        获取缓存的表数量，如果没有缓存则查询数据库
        
        Returns:
            int: 表数量
        """
        # 如果已有缓存，直接返回
        if hasattr(self, '_cached_table_count'):
            return self._cached_table_count
        
        try:
            # 如果没有缓存，查询数据库
            from src.utils.db import execute_query
            result = execute_query(f"SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = '{self.db_name}' AND table_type = 'BASE TABLE'")
            if result and result[0]:
                count = result[0].get('count', 0)
                self._cached_table_count = count
                return count
            return 0
        except Exception as e:
            logger.error(f"获取表数量时出错: {str(e)}")
            return 0
    
    def get_cached_tables(self) -> List[Dict[str, Any]]:
        """
        获取缓存的表信息，如果没有缓存则查询数据库
        
        Returns:
            List[Dict[str, Any]]: 表信息列表，每个表包含name、comment等信息
        """
        # 如果已有缓存，直接返回
        if hasattr(self, '_cached_tables'):
            return self._cached_tables
        
        try:
            # 如果没有缓存，调用list_tables获取
            result = self.list_tables()
            if result.get('success', False):
                return result.get('tables', [])
            return []
        except Exception as e:
            logger.error(f"获取表信息时出错: {str(e)}")
            return []
    
    def get_database_status(self) -> Dict[str, Any]:
        """
        获取数据库状态信息
        
        Returns:
            Dict[str, Any]: 数据库状态信息
        """
        try:
            return {
                'database_name': self.db_name,
                'connection_status': 'connected',
                'server_info': {
                    'version': self.get_doris_version(),
                    'server_time': self.get_server_time()
                },
                'table_count': self.get_cached_table_count(),
                'last_refresh_time': getattr(self.metadata_extractor, 'last_refresh_time', None)
            }
        except Exception as e:
            logger.error(f"获取数据库状态时出错: {str(e)}")
            return {
                'database_name': self.db_name,
                'connection_status': 'error',
                'error_message': str(e)
            }
    
    def get_business_overview(self) -> Dict[str, Any]:
        """
        获取业务概览
        
        Returns:
            Dict[str, Any]: 业务概览信息
        """
        try:
            # 先从元数据库中查询业务概览信息
            business_metadata = self.metadata_extractor.get_business_metadata_from_database(self.db_name)
            
            # 如果元数据库中存在业务概览信息，则直接返回
            if business_metadata and isinstance(business_metadata, dict) and 'business_domain' in business_metadata:
                logger.info(f"从元数据库中获取到业务概览信息")
                
                # 获取表数量
                tables = self.metadata_extractor.get_database_tables(self.db_name)
                table_count = len(tables)
                
                # 构建结果
                result = {
                    'success': True,
                    'database': self.db_name,
                    'table_count': table_count,
                    'business_domain': business_metadata.get('business_domain', '未知业务领域'),
                    'core_entities': business_metadata.get('core_entities', []),
                    'business_processes': business_metadata.get('business_processes', [])
                }
                
                return result
            
            # 如果元数据库中不存在业务概览信息，则刷新元数据
            logger.info(f"元数据库中不存在业务概览信息，将刷新元数据")
            
            # 刷新元数据
            refresh_success = self.metadata_extractor._update_sql_patterns_and_business_metadata(self.db_name)
            
            if refresh_success:
                # 再次尝试从元数据库中获取业务概览信息
                logger.info(f"元数据刷新成功，再次尝试获取业务概览信息")
                business_metadata = self.metadata_extractor.get_business_metadata_from_database(self.db_name)
                
                if business_metadata and isinstance(business_metadata, dict) and 'business_domain' in business_metadata:
                    logger.info(f"刷新后成功从元数据库中获取到业务概览信息")
                    
                    # 获取表数量
                    tables = self.metadata_extractor.get_database_tables(self.db_name)
                    table_count = len(tables)
                    
                    # 构建结果
                    result = {
                        'success': True,
                        'database': self.db_name,
                        'table_count': table_count,
                        'business_domain': business_metadata.get('business_domain', '未知业务领域'),
                        'core_entities': business_metadata.get('core_entities', []),
                        'business_processes': business_metadata.get('business_processes', [])
                    }
                    
                    return result
            
            # 如果刷新元数据失败或者刷新后仍无法获取业务概览信息，则调用LLM生成
            logger.info(f"元数据刷新后仍无法获取业务概览信息，将使用LLM生成")
            
            # 获取业务元数据
            business_metadata = self.metadata_extractor.summarize_business_metadata(self.db_name)
            
            # 获取表数量
            tables = self.metadata_extractor.get_database_tables(self.db_name)
            table_count = len(tables)
            
            # 构建结果
            result = {
                'success': True,
                'database': self.db_name,
                'table_count': table_count,
                'business_domain': business_metadata.get('business_domain', '未知业务领域'),
                'core_entities': business_metadata.get('core_entities', []),
                'business_processes': business_metadata.get('business_processes', [])
            }
            
            return result
        except Exception as e:
            logger.error(f"获取业务概览时出错: {str(e)}")
            return {
                'success': False,
                'message': f'获取业务概览时出错: {str(e)}'
            }

def main():
    """
    服务主函数
    """
    parser = argparse.ArgumentParser(description="Apache Doris NL2SQL 服务")
    parser.add_argument("--query", help="自然语言查询")
    parser.add_argument("--list-tables", action="store_true", help="列出数据库中的表")
    parser.add_argument("--explain-table", help="解释指定的表结构")
    parser.add_argument("--business-overview", action="store_true", help="获取业务概览")
    parser.add_argument("--database", help="指定数据库名")
    parser.add_argument("--refresh-metadata", action="store_true", help="增量刷新元数据")
    parser.add_argument("--force-refresh", action="store_true", help="强制全量刷新元数据")
    parser.add_argument("--auto-refresh", action="store_true", help="初始化时自动刷新元数据")
    parser.add_argument("--list-llm-providers", action="store_true", help="列出可用的LLM提供商")
    parser.add_argument("--set-llm-provider", help="设置LLM提供商")
    args = parser.parse_args()
    
    # 初始化服务
    service = NL2SQLService(args.database, auto_refresh_metadata=args.auto_refresh)
    
    # 处理命令行参数
    if args.list_llm_providers:
        providers = service.get_available_llm_providers()
        print("可用的LLM提供商:")
        for provider in providers:
            print(f"- {provider}")
        print(f"当前使用的提供商: {service.llm_provider}")
    elif args.set_llm_provider:
        success = service.set_llm_provider(args.set_llm_provider)
        if success:
            print(f"已切换到LLM提供商: {args.set_llm_provider}")
        else:
            print(f"切换LLM提供商失败，{args.set_llm_provider} 可能不可用")
    elif args.refresh_metadata or args.force_refresh:
        # 使用force参数
        force = args.force_refresh
        service._refresh_metadata(force=force)
        refresh_type = "全量" if force else "增量"
        print(f"{refresh_type}元数据刷新完成")
    elif args.list_tables:
        result = service.list_tables()
        if result['success']:
            print(f"数据库 {result['database']} 中的表 ({result['count']}):")
            for table in result['tables']:
                print(f"- {table['name']}" + (f" (说明: {table['comment']})" if table['comment'] else ""))
        else:
            print(f"错误: {result['message']}")
    elif args.explain_table:
        result = service.explain_table(args.explain_table)
        if result['success']:
            print(f"表 {result['table_name']} 的结构:")
            print(f"说明: {result['table_comment']}")
            if result['business_description']:
                print(f"业务描述: {result['business_description']}")
            print("\n列:")
            for column in result['columns']:
                name = column.get('name', '')
                type = column.get('type', '')
                comment = column.get('comment', '')
                print(f"- {name} ({type})" + (f" # {comment}" if comment else ""))
            if result['relationships']:
                print("\n关系:")
                for relation in result['relationships']:
                    if relation.get('table') == args.explain_table:
                        print(f"- 字段 {relation.get('column')} 引用 {relation.get('references_table')}.{relation.get('references_column')}")
                    else:
                        print(f"- 被 {relation.get('table')}.{relation.get('column')} 引用")
        else:
            print(f"错误: {result['message']}")
    elif args.business_overview:
        result = service.get_business_overview()
        if result['success']:
            print(f"数据库 {result['database']} 的业务概览:")
            print(f"业务领域: {result['business_domain']}")
            print(f"表数量: {result['table_count']}")
            
            print("\n核心业务实体:")
            for entity in result['core_entities']:
                print(f"- {entity.get('name', '')}: {entity.get('description', '')}")
            
            print("\n业务流程:")
            for process in result['business_processes']:
                print(f"- {process.get('name', '')}: {process.get('description', '')}")
        else:
            print(f"错误: {result['message']}")
    elif args.query:
        result = service.process_query(args.query)
        if result['success']:
            print(f"SQL查询:\n{result['sql']}\n")
            
            if 'data' in result:
                print(f"查询结果 ({result.get('row_count', 0)} 行):")
                if result.get('truncated'):
                    print(f"注意: 结果已截断，只显示 {result.get('displayed_row_count')} 行，共 {result.get('original_row_count')} 行")
                
                if result['data']:
                    # 打印列头
                    columns = result.get('columns', [])
                    if columns:
                        print(" | ".join(columns))
                        print("-" * (sum(len(col) for col in columns) + 3 * (len(columns) - 1)))
                    
                    # 打印数据
                    for row in result['data']:
                        print(" | ".join(str(row.get(col, "")) for col in columns))
                else:
                    print("查询结果为空")
            else:
                print("查询执行成功，但没有返回数据")
        else:
            print(f"错误: {result.get('message', '处理查询时出错')}")
            
            if 'errors' in result:
                print("\n错误详情:")
                for error in result['errors']:
                    print(f"- {error}")
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 