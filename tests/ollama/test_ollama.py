import requests
import time
import sys
import argparse
import os
import json
from pathlib import Path
from tqdm import tqdm

# 定义ANSI颜色代码
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def calculate_token_speed(tokens_generated, time_elapsed):
    """计算并返回token生成速度（tokens/second）"""
    if time_elapsed == 0:
        return 0
    return tokens_generated / time_elapsed

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Ollama模型测试工具 - 支持普通生成和流式生成')
    parser.add_argument('--prompt', type=str, default="Apache Doris 是什么？", 
                        help='提示词')
    parser.add_argument('--max_tokens', type=int, default=500,
                        help='最大生成token数')
    parser.add_argument('--model', type=str, default="qwq:latest",
                        help='模型名称')
    parser.add_argument('--temperature', type=float, default=0.7,
                        help='温度参数 (0.0-1.0)')
    parser.add_argument('--top_p', type=float, default=0.9,
                        help='核采样参数 (0.0-1.0)')
    parser.add_argument('--stream_only', action='store_true',
                        help='仅使用流式生成模式')
    parser.add_argument('--update_interval', type=int, default=5,
                        help='更新token速度的间隔(每N个token更新一次)')
    parser.add_argument('--no_color', action='store_true',
                        help='禁用彩色输出')
    parser.add_argument('--base_url', type=str, default="http://localhost:11434",
                        help='Ollama API基础URL')
    return parser.parse_args()

def colored_text(text, color, args):
    """根据参数返回彩色或普通文本"""
    if args.no_color:
        return text
    return f"{color}{text}{Colors.ENDC}"

def get_model_details(args):
    """获取模型详细信息"""
    try:
        response = requests.get(
            f"{args.base_url}/api/tags", 
            timeout=10
        )
        response.raise_for_status()
        
        models = response.json().get("models", [])
        for model in models:
            if model["name"] == args.model:
                return model
        
        return {"name": args.model}
    except Exception as e:
        print(colored_text(f"⚠️ 获取模型详情失败: {str(e)}", Colors.YELLOW, args))
        return {"name": args.model}

def generate_completion(args, stream=False):
    """生成回答"""
    data = {
        "model": args.model,
        "prompt": args.prompt,
        "stream": stream,
        "options": {
            "temperature": args.temperature,
            "top_p": args.top_p,
            "num_predict": args.max_tokens
        }
    }
    
    if stream:
        # 流式生成
        try:
            response = requests.post(
                f"{args.base_url}/api/generate",
                json=data,
                stream=True,
                timeout=60
            )
            response.raise_for_status()
            return response
        except Exception as e:
            print(colored_text(f"❌ 流式生成请求失败: {str(e)}", Colors.RED, args))
            sys.exit(1)
    else:
        # 普通生成
        try:
            response = requests.post(
                f"{args.base_url}/api/generate",
                json=data,
                timeout=60
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(colored_text(f"❌ 普通生成请求失败: {str(e)}", Colors.RED, args))
            sys.exit(1)

def main():
    # 解析命令行参数
    args = parse_arguments()
    
    # 设置终端大小
    terminal_width = os.get_terminal_size().columns
    
    # 获取模型信息
    model_details = get_model_details(args)
    
    # 显示模型信息
    print(colored_text("🧠 模型配置:", Colors.BOLD, args))
    print(colored_text(f"  - 模型名称: {args.model}", Colors.CYAN, args))
    print(colored_text(f"  - 温度: {args.temperature}", Colors.CYAN, args))
    print(colored_text(f"  - 核采样: {args.top_p}", Colors.CYAN, args))
    print(colored_text(f"  - 最大生成长度: {args.max_tokens}", Colors.CYAN, args))
    
    # 显示提示词
    print(colored_text(f"📝 提示词: ", Colors.BOLD, args) + args.prompt)
    
    # 普通生成模式
    if not args.stream_only:
        print(colored_text("\n=== 普通生成模式 ===", Colors.HEADER + Colors.BOLD, args))
        print(colored_text("⏳ 正在生成...", Colors.YELLOW, args))
        
        # 创建进度条但不知道总数，使用迭代模式
        progress_bar = tqdm(desc="生成进度", unit="token")
        
        start_time = time.time()
        result = generate_completion(args, stream=False)
        total_time = time.time() - start_time
        
        # 提取结果
        response_text = result.get("response", "")
        total_tokens = result.get("eval_count", 0) + result.get("prompt_eval_count", 0)
        completion_tokens = result.get("eval_count", 0)
        
        # 更新进度条
        progress_bar.total = completion_tokens
        progress_bar.update(completion_tokens)
        progress_bar.close()
        
        print(colored_text(f"⏱️ 生成完成！总用时: {total_time:.2f}秒", Colors.GREEN, args))
        print(colored_text(f"📊 生成了 {completion_tokens} 个tokens, 速度: {calculate_token_speed(completion_tokens, total_time):.2f} tokens/秒", Colors.GREEN, args))
        print(colored_text("📄 响应内容:", Colors.BOLD, args))
        print(colored_text(response_text, Colors.BLUE, args))
    
    # 流式生成模式
    print(colored_text("\n=== 流式生成模式 ===", Colors.HEADER + Colors.BOLD, args))
    print(colored_text(f"❓ 提问: ", Colors.YELLOW, args) + args.prompt)
    print(colored_text("🤖 AI正在回答: ", Colors.GREEN, args), end="", flush=True)
    
    # 流式生成
    tokens_generated = 0
    start_time = time.time()
    accumulated_text = ""
    last_speed_check = 0
    
    response = generate_completion(args, stream=True)
    
    try:
        for line in response.iter_lines():
            if not line:
                continue
                
            try:
                data = json.loads(line)
                
                if "error" in data:
                    print(colored_text(f"\n❌ 错误: {data['error']}", Colors.RED, args))
                    break
                
                # 提取新生成的文本
                new_text = data.get("response", "")
                accumulated_text += new_text
                tokens_generated += 1
                
                # 打印新生成的文本片段
                if not args.no_color:
                    sys.stdout.write(Colors.BLUE)
                sys.stdout.write(new_text)
                sys.stdout.flush()
                if not args.no_color:
                    sys.stdout.write(Colors.ENDC)
                
                # 每生成N个token计算一次速度
                current_time = time.time()
                if tokens_generated % args.update_interval == 0:
                    time_elapsed = current_time - start_time
                    token_speed = calculate_token_speed(tokens_generated, time_elapsed)
                    tokens_since_last = tokens_generated - last_speed_check
                    time_since_last = time_elapsed - (last_speed_check / token_speed if token_speed > 0 else 0)
                    
                    # 计算当前部分的速度
                    current_speed = tokens_since_last / time_since_last if time_since_last > 0 else 0
                    
                    # 更新上次检查点
                    last_speed_check = tokens_generated
                    
                    # 显示速度信息 (在新行显示)
                    speed_info = f"\n⚡ 当前速度: {current_speed:.2f} tokens/秒 | 平均: {token_speed:.2f} tokens/秒 | 已生成: {tokens_generated} tokens"
                    
                    print()  # 换行
                    print(colored_text(speed_info, Colors.YELLOW, args))
                
                # 检查生成是否结束
                if data.get("done", False):
                    break
                    
            except json.JSONDecodeError:
                # 忽略无法解析的行
                pass
    
    except Exception as e:
        print(colored_text(f"\n❌ 流式生成出错: {str(e)}", Colors.RED, args))
    
    total_time = time.time() - start_time
    print("\n")
    print(colored_text(f"✅ 生成完成！总用时: {total_time:.2f}秒", Colors.GREEN, args))
    print(colored_text(f"📈 总共生成了 {tokens_generated} 个tokens, 平均速度: {calculate_token_speed(tokens_generated, total_time):.2f} tokens/秒", Colors.GREEN, args))
    
    # 显示完整输出
    print(colored_text("\n📝 完整回答:", Colors.BOLD, args))
    print(colored_text(accumulated_text, Colors.BLUE, args))

if __name__ == "__main__":
    main() 