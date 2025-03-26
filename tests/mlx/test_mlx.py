from mlx_lm import load, generate, stream_generate
import time
import sys
import argparse
import os
import subprocess
import tempfile
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
    parser = argparse.ArgumentParser(description='MLX模型测试工具 - 支持普通生成和流式生成')
    parser.add_argument('--prompt', type=str, default="如何喂养宠物狐狸？", 
                        help='提示词')
    parser.add_argument('--max_tokens', type=int, default=200,
                        help='最大生成token数')
    parser.add_argument('--model_path', type=str, default="Qwen/QwQ-32B",
                        help='模型路径')
    parser.add_argument('--bit_width', type=int, choices=[16, 8, 4, 3], default=16,
                        help='模型量化位宽 (16=fp16, 8=int8, 4=int4, 3=int3)')
    parser.add_argument('--group_size', type=int, default=64,
                        help='量化分组大小 (仅在bit_width < 16时使用)')
    parser.add_argument('--stream_only', action='store_true',
                        help='仅使用流式生成模式')
    parser.add_argument('--update_interval', type=int, default=5,
                        help='更新token速度的间隔(每N个token更新一次)')
    parser.add_argument('--no_color', action='store_true',
                        help='禁用彩色输出')
    parser.add_argument('--cache_dir', type=str, default="./mlx_models",
                        help='模型缓存目录')
    parser.add_argument('--force_convert', action='store_true',
                        help='强制重新转换模型，即使缓存已存在')
    return parser.parse_args()

def colored_text(text, color, args):
    """根据参数返回彩色或普通文本"""
    if args.no_color:
        return text
    return f"{color}{text}{Colors.ENDC}"

def quantize_model(args):
    """下载并量化模型，返回本地模型路径"""
    # 创建模型缓存目录
    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    # 构建量化后的模型目录名
    if args.bit_width < 16:
        quantized_name = f"{args.model_path.replace('/', '_')}_q{args.bit_width}_g{args.group_size}"
    else:
        quantized_name = f"{args.model_path.replace('/', '_')}_fp16"
    
    quantized_path = cache_dir / quantized_name
    
    # 如果已经存在量化后的模型且不强制重新转换，则直接返回
    if quantized_path.exists() and not args.force_convert:
        return str(quantized_path)
    
    # 构建转换命令
    cmd = ["python", "-m", "mlx_lm.convert", "--hf-path", args.model_path]
    
    # 添加量化参数
    if args.bit_width < 16:
        cmd.extend(["-q", "--q-bits", str(args.bit_width), "--q-group-size", str(args.group_size)])
    
    # 添加输出目录
    cmd.extend(["--mlx-path", str(quantized_path)])
    
    # 执行转换命令
    print(colored_text(f"🔄 正在下载并转换模型: {args.model_path}", Colors.CYAN, args))
    print(colored_text(f"    命令: {' '.join(cmd)}", Colors.YELLOW, args))
    
    try:
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        # 实时输出转换进度
        for line in iter(process.stdout.readline, ''):
            print(colored_text(f"    {line.strip()}", Colors.YELLOW, args))
        
        process.wait()
        
        if process.returncode != 0:
            print(colored_text(f"❌ 模型转换失败，返回代码: {process.returncode}", Colors.RED, args))
            sys.exit(1)
            
        print(colored_text(f"✅ 模型转换完成！保存到: {quantized_path}", Colors.GREEN, args))
        return str(quantized_path)
    
    except Exception as e:
        print(colored_text(f"❌ 模型转换出错: {str(e)}", Colors.RED, args))
        sys.exit(1)

def main():
    # 解析命令行参数
    args = parse_arguments()
    
    # 设置终端大小
    terminal_width = os.get_terminal_size().columns
    
    # 显示模型信息
    print(colored_text("🧠 模型配置:", Colors.BOLD, args))
    print(colored_text(f"  - 原始模型: {args.model_path}", Colors.CYAN, args))
    print(colored_text(f"  - 量化位宽: {args.bit_width}位", Colors.CYAN, args))
    if args.bit_width < 16:
        print(colored_text(f"  - 量化分组大小: {args.group_size}", Colors.CYAN, args))
    
    # 量化模型并获取本地路径
    local_model_path = quantize_model(args)
    
    # 加载模型
    print(colored_text(f"🔄 正在加载模型: {local_model_path}", Colors.CYAN, args))
    start_load_time = time.time()
    model, tokenizer = load(local_model_path)
    load_time = time.time() - start_load_time
    print(colored_text(f"✅ 模型加载完成！用时: {load_time:.2f}秒", Colors.GREEN, args))
    
    # 显示提示词
    print(colored_text(f"📝 提示词: ", Colors.BOLD, args) + args.prompt)
    print(colored_text(f"🔢 最大生成token数: {args.max_tokens}", Colors.BOLD, args))
    
    # 普通生成模式
    if not args.stream_only:
        print(colored_text("\n=== 普通生成模式 ===", Colors.HEADER + Colors.BOLD, args))
        print(colored_text("⏳ 正在生成...", Colors.YELLOW, args))
        
        # 创建进度条但不知道总数，使用迭代模式
        progress_bar = tqdm(desc="生成进度", unit="token")
        
        start_time = time.time()
        response = generate(model, tokenizer, prompt=args.prompt, max_tokens=args.max_tokens)
        total_time = time.time() - start_time
        tokens_count = len(tokenizer.encode(response)) - len(tokenizer.encode(args.prompt))
        
        # 更新进度条
        progress_bar.total = tokens_count
        progress_bar.update(tokens_count)
        progress_bar.close()
        
        print(colored_text(f"⏱️ 生成完成！总用时: {total_time:.2f}秒", Colors.GREEN, args))
        print(colored_text(f"📊 生成了 {tokens_count} 个tokens, 速度: {calculate_token_speed(tokens_count, total_time):.2f} tokens/秒", Colors.GREEN, args))
        print(colored_text("📄 响应内容:", Colors.BOLD, args))
        print(colored_text(response, Colors.BLUE, args))
    
    # 流式生成模式
    print(colored_text("\n=== 流式生成模式 ===", Colors.HEADER + Colors.BOLD, args))
    print(colored_text(f"❓ 提问: ", Colors.YELLOW, args) + args.prompt)
    print(colored_text("🤖 AI正在回答: ", Colors.GREEN, args), end="", flush=True)
    
    # 流式生成
    tokens_generated = 0
    start_time = time.time()
    accumulated_text = ""
    last_speed_check = 0
    
    for response in stream_generate(model, tokenizer, prompt=args.prompt, max_tokens=args.max_tokens):
        # 获取当前输出的新文本片段
        new_text = response.text
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
    
    total_time = time.time() - start_time
    print("\n")
    print(colored_text(f"✅ 生成完成！总用时: {total_time:.2f}秒", Colors.GREEN, args))
    print(colored_text(f"📈 总共生成了 {tokens_generated} 个tokens, 平均速度: {calculate_token_speed(tokens_generated, total_time):.2f} tokens/秒", Colors.GREEN, args))
    
    # 显示完整输出
    print(colored_text("\n📝 完整回答:", Colors.BOLD, args))
    print(colored_text(accumulated_text, Colors.BLUE, args))

if __name__ == "__main__":
    main()