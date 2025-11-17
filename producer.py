import argparse
import json
import time
import threading
from typing import Optional
from dataclasses import dataclass

import pulsar


@dataclass
class ProducerConfig:
    """Producer 配置类"""
    service_url: str = "pulsar://127.0.0.1:6650"
    topic: str = "test-lancedb-topic"
    num_producers: int = 10
    num_threads: int = 20
    batch_size: int = 10000
    message_size: int = 100  # 消息大小（字节），用于生成测试数据
    report_interval: int = 50000  # 报告间隔（消息数）
    report_time_interval: float = 1.0  # 报告时间间隔（秒）


class Producer:
    """Pulsar Producer 封装类"""
    
    def __init__(self, config: ProducerConfig):
        self.config = config
        self.producers = []
        self.clients = []
        self.message_count = 0
        self.bytes_sent = 0
        self._count_lock = threading.Lock()
        self._running = True
        self._init_producers()
    
    def _init_producers(self):
        """初始化多个 Producer 实例"""
        print(f"正在创建 {self.config.num_producers} 个 Producer 实例...")
        for i in range(self.config.num_producers):
            try:
                client = pulsar.Client(self.config.service_url)
                try:
                    producer = client.create_producer(
                        self.config.topic,
                        batching_enabled=True,
                        batching_max_messages=self.config.batch_size,
                        batching_max_publish_delay_ms=1,
                        max_pending_messages=100000
                    )
                except TypeError:
                    # 兼容旧版本 API
                    producer = client.create_producer(self.config.topic)
                
                self.clients.append(client)
                self.producers.append(producer)
            except Exception as e:
                print(f"警告: 创建 Producer {i} 失败: {e}")
        
        if not self.producers:
            raise RuntimeError("无法创建任何 Producer 实例，请检查 Pulsar 服务是否运行")
        
        print(f"✓ 成功创建 {len(self.producers)} 个 Producer 实例\n")
    
    def send_message(self, message_bytes: bytes, producer_idx: Optional[int] = None):
        """
        发送消息
        
        Args:
            message_bytes: 消息字节数据
            producer_idx: 指定使用的 producer 索引，如果为 None 则自动选择
        """
        if not self._running:
            return False
        
        if producer_idx is None:
            producer_idx = hash(message_bytes) % len(self.producers)
        
        try:
            self.producers[producer_idx].send(message_bytes)
            with self._count_lock:
                self.message_count += 1
                self.bytes_sent += len(message_bytes)
            return True
        except Exception:
            return False
    
    def get_stats(self):
        """获取发送统计信息"""
        with self._count_lock:
            return {
                "message_count": self.message_count,
                "bytes_sent": self.bytes_sent,
                "mb_sent": self.bytes_sent / (1024 * 1024)
            }
    
    def close(self):
        """关闭所有 Producer 和 Client"""
        self._running = False
        print("\n正在关闭 Producer...")
        
        for producer in self.producers:
            try:
                producer.close()
            except Exception:
                pass
        
        for client in self.clients:
            try:
                client.close()
            except Exception:
                pass
        
        print("✓ Producer 已关闭")


def generate_test_message(message_id: int, message_size: int) -> bytes:
    """
    生成测试消息
    
    Args:
        message_id: 消息 ID
        message_size: 目标消息大小（字节）
    
    Returns:
        消息字节数据
    """
    # 基础消息结构
    base_msg = {
        "id": message_id,
        "timestamp": int(time.time() * 1000),
        "value": message_id * 10
    }
    
    # 如果消息太小，添加填充数据
    base_json = json.dumps(base_msg).encode('utf-8')
    if len(base_json) < message_size:
        padding_size = message_size - len(base_json) - 20  # 预留一些空间
        if padding_size > 0:
            base_msg["data"] = "x" * padding_size
    
    return json.dumps(base_msg).encode('utf-8')


def send_worker(producer: Producer, thread_id: int, config: ProducerConfig):
    """发送消息的工作线程"""
    local_counter = thread_id * 1000000  # 每个线程有自己的起始 ID
    
    while producer._running:
        local_counter += 1
        message_bytes = generate_test_message(local_counter, config.message_size)
        producer.send_message(message_bytes, local_counter % len(producer.producers))


def run_producer(config: ProducerConfig):
    """运行 Producer 发送消息"""
    producer = Producer(config)
    
    try:
        print("=" * 60)
        print("Pulsar Producer 压力测试")
        print("=" * 60)
        print(f"Service URL: {config.service_url}")
        print(f"Topic: {config.topic}")
        print(f"Producer 数量: {config.num_producers}")
        print(f"线程数: {config.num_threads}")
        print(f"批量大小: {config.batch_size}")
        print(f"消息大小: ~{config.message_size} 字节")
        print("=" * 60)
        print("\n开始发送消息...")
        print("按 Ctrl+C 停止发送\n")
        
        start_time = time.time()
        last_report_time = start_time
        last_report_count = 0
        last_report_bytes = 0
        
        # 启动工作线程
        threads = []
        for i in range(config.num_threads):
            thread = threading.Thread(
                target=send_worker,
                args=(producer, i, config),
                daemon=True
            )
            thread.start()
            threads.append(thread)
        
        # 主线程负责报告
        try:
            while True:
                time.sleep(config.report_time_interval)
                
                current_time = time.time()
                stats = producer.get_stats()
                current_count = stats["message_count"]
                current_bytes = stats["bytes_sent"]
                
                # 计算速率
                elapsed = current_time - last_report_time
                if elapsed > 0:
                    count_diff = current_count - last_report_count
                    bytes_diff = current_bytes - last_report_bytes
                    
                    instant_msg_rate = count_diff / elapsed
                    instant_mbps = (bytes_diff / (1024 * 1024)) / elapsed
                    
                    total_elapsed = current_time - start_time
                    avg_msg_rate = current_count / total_elapsed if total_elapsed > 0 else 0
                    avg_mbps = (current_bytes / (1024 * 1024)) / total_elapsed if total_elapsed > 0 else 0
                    
                    # 输出报告
                    print(
                        f"[Producer] [{total_elapsed:.1f}s] "
                        f"已发送: {current_count:,} 条 | "
                        f"{stats['mb_sent']:.2f} MB | "
                        f"瞬时: {instant_msg_rate:,.0f} msg/s, {instant_mbps:.2f} MB/s | "
                        f"平均: {avg_msg_rate:,.0f} msg/s, {avg_mbps:.2f} MB/s"
                    )
                    
                    last_report_time = current_time
                    last_report_count = current_count
                    last_report_bytes = current_bytes
                
        except KeyboardInterrupt:
            print("\n收到停止信号，正在关闭...")
        
        finally:
            producer.close()
            
            # 最终统计
            total_time = time.time() - start_time
            final_stats = producer.get_stats()
            
            print("\n" + "=" * 60)
            print("最终统计")
            print("=" * 60)
            print(f"总发送消息数: {final_stats['message_count']:,} 条")
            print(f"总发送数据量: {final_stats['mb_sent']:.2f} MB")
            print(f"总耗时: {total_time:.2f} 秒")
            if total_time > 0:
                print(f"平均速率: {final_stats['message_count'] / total_time:,.0f} 消息/秒")
                print(f"平均吞吐量: {final_stats['mb_sent'] / total_time:.2f} MB/s")
            print("=" * 60)
    
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()
        producer.close()
        raise


def main():
    """主函数，支持命令行参数"""
    parser = argparse.ArgumentParser(
        description="Pulsar Producer 压力测试工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 使用默认配置
  python3 producer.py
  
  # 自定义配置
  python3 producer.py --topic my-topic --producers 20 --threads 50
  
  # 发送大消息测试
  python3 producer.py --message-size 1024 --threads 10
        """
    )
    
    parser.add_argument(
        "--service-url",
        type=str,
        default="pulsar://127.0.0.1:6650",
        help="Pulsar 服务 URL (默认: pulsar://127.0.0.1:6650)"
    )
    
    parser.add_argument(
        "--topic",
        type=str,
        default="test-lancedb-topic",
        help="Topic 名称 (默认: test-lancedb-topic)"
    )
    
    parser.add_argument(
        "--producers",
        type=int,
        default=10,
        help="Producer 实例数量 (默认: 10)"
    )
    
    parser.add_argument(
        "--threads",
        type=int,
        default=20,
        help="发送线程数 (默认: 20)"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="批量大小 (默认: 10000)"
    )
    
    parser.add_argument(
        "--message-size",
        type=int,
        default=100,
        help="消息大小（字节）(默认: 100)"
    )
    
    parser.add_argument(
        "--report-interval",
        type=int,
        default=50000,
        help="报告间隔（消息数）(默认: 50000)"
    )
    
    parser.add_argument(
        "--report-time",
        type=float,
        default=1.0,
        help="报告时间间隔（秒）(默认: 1.0)"
    )
    
    args = parser.parse_args()
    
    config = ProducerConfig(
        service_url=args.service_url,
        topic=args.topic,
        num_producers=args.producers,
        num_threads=args.threads,
        batch_size=args.batch_size,
        message_size=args.message_size,
        report_interval=args.report_interval,
        report_time_interval=args.report_time
    )
    
    try:
        run_producer(config)
    except KeyboardInterrupt:
        print("\nProducer 已停止")
    except Exception as e:
        print(f"\n发生错误: {e}")
        exit(1)


if __name__ == "__main__":
    main()
