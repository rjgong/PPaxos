#!/usr/bin/env python3
"""
日志文件拆分工具
将大的日志文件按指定大小拆分成多个小文件
"""

import os
import sys
import argparse
from pathlib import Path

def split_log_file(input_file, chunk_size_mb=50, output_dir=None):
    """
    拆分日志文件
    
    Args:
        input_file (str): 输入文件路径
        chunk_size_mb (int): 每个分块的大小（MB）
        output_dir (str): 输出目录，如果为None则使用输入文件所在目录
    """
    input_path = Path(input_file)
    
    # 检查输入文件是否存在
    if not input_path.exists():
        print(f"错误: 文件 {input_file} 不存在")
        return False
    
    # 获取文件大小
    file_size = input_path.stat().st_size
    chunk_size = chunk_size_mb * 1024 * 1024  # 转换为字节
    
    # 如果文件小于分块大小，不需要拆分
    if file_size <= chunk_size:
        print(f"文件 {input_file} 大小 ({file_size / 1024 / 1024:.2f}MB) 小于分块大小 ({chunk_size_mb}MB)，无需拆分")
        return True
    
    # 确定输出目录
    if output_dir is None:
        output_dir = input_path.parent
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    # 计算需要多少个分块
    num_chunks = (file_size + chunk_size - 1) // chunk_size
    
    print(f"开始拆分文件: {input_file}")
    print(f"文件大小: {file_size / 1024 / 1024:.2f}MB")
    print(f"分块大小: {chunk_size_mb}MB")
    print(f"预计分块数: {num_chunks}")
    print(f"输出目录: {output_dir}")
    
    try:
        with open(input_file, 'rb') as f:
            for i in range(num_chunks):
                # 生成输出文件名
                output_filename = f"{input_path.stem}_part{i+1:03d}{input_path.suffix}"
                output_path = output_dir / output_filename
                
                print(f"正在写入: {output_filename}")
                
                with open(output_path, 'wb') as out_f:
                    # 读取并写入数据
                    remaining = min(chunk_size, file_size - i * chunk_size)
                    bytes_written = 0
                    
                    while bytes_written < remaining:
                        # 每次读取1MB，避免内存占用过大
                        read_size = min(1024 * 1024, remaining - bytes_written)
                        data = f.read(read_size)
                        if not data:
                            break
                        out_f.write(data)
                        bytes_written += len(data)
                
                print(f"完成: {output_filename} ({bytes_written / 1024 / 1024:.2f}MB)")
        
        print(f"\n拆分完成！共生成 {num_chunks} 个文件")
        return True
        
    except Exception as e:
        print(f"拆分过程中发生错误: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='将日志文件按指定大小拆分')
    parser.add_argument('input_file', help='要拆分的日志文件路径')
    parser.add_argument('-s', '--size', type=int, default=50, 
                       help='每个分块的大小（MB），默认100MB')
    parser.add_argument('-o', '--output', help='输出目录，默认使用输入文件所在目录')
    
    args = parser.parse_args()
    
    # 检查输入文件是否存在
    if not os.path.exists(args.input_file):
        print(f"错误: 文件 {args.input_file} 不存在")
        sys.exit(1)
    
    # 执行拆分
    success = split_log_file(args.input_file, args.size, args.output)
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()