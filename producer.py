import time
import threading
from typing import Dict, Any, List

import pulsar


class Producer:
    
    def __init__(self, service_url: str, topic: str, num_producers: int = 10, batch_size: int = 10000):
        self.service_url = service_url
        self.topic = topic
        self.num_producers = num_producers
        self.batch_size = batch_size
        self.producers = []
        self.clients = []
        self.message_count = 0
        self._count_lock = threading.Lock()
        self._running = True
        
    def _init_producers(self):
        for i in range(self.num_producers):
            try:
                client = pulsar.Client(self.service_url)
                try:
                    producer = client.create_producer(
                        self.topic,
                        batching_enabled=True,
                        batching_max_messages=self.batch_size,
                        batching_max_publish_delay_ms=1,
                        max_pending_messages=100000
                    )
                except TypeError:
                    producer = client.create_producer(self.topic)
                
                self.clients.append(client)
                self.producers.append(producer)
            except Exception as e:
                print(f"Failed to create producer {i}: {e}")
        
        print(f"已创建 {len(self.producers)} 个Producer实例")
    
    def send_sync(self, message_bytes: bytes, producer_idx: int = None):
        """同步发送消息（最快的方式）"""
        if producer_idx is None:
            producer_idx = hash(message_bytes) % len(self.producers)
        
        try:
            self.producers[producer_idx].send(message_bytes)
            with self._count_lock:
                self.message_count += 1
        except Exception:
            pass  # 忽略发送错误，继续发送
    
    def close(self):
        """关闭所有producer和client"""
        self._running = False
        for producer in self.producers:
            try:
                producer.close()
            except:
                pass
        for client in self.clients:
            try:
                client.close()
            except:
                pass
    


def send_messages_ultra_fast(
    service_url: str = "pulsar://127.0.0.1:6650",
    topic: str = "test-lancedb-topic",
    num_producers: int = 10,
    batch_size: int = 10000,
    num_threads: int = 20,
    report_interval: int = 100000
):
    """
    超高速发送消息到Pulsar topic（百万级速率）
    
    Args:
        service_url: Pulsar服务URL
        topic: 目标topic名称
        num_producers: Producer实例数量（多连接）
        batch_size: 批量大小
        num_threads: 发送线程数
        report_interval: 报告间隔（消息数）
    """
    producer = Producer(service_url, topic, num_producers, batch_size)
    
    try:
        # 在主线程初始化producers
        producer._init_producers()
        
        if not producer.producers:
            print("错误：无法创建任何producer")
            return
        
        print(f"开始超高速压力测试，topic: {topic}")
        print(f"Producer数量: {num_producers}, 线程数: {num_threads}, 批量大小: {batch_size}")
        print("按 Ctrl+C 停止发送...\n")
        
        start_time = time.time()
        message_id_counter = [0]
        message_id_lock = threading.Lock()
        last_report_time = start_time
        last_report_count = 0
        
        # 使用线程池进行并发发送
        def send_worker(thread_id):
            local_counter = thread_id * 1000000  # 每个线程有自己的起始ID
            while producer._running:

                local_counter += 1
                msg_id = local_counter

                msg = f'{{"id":{msg_id},"v":{msg_id*10}}}'.encode('utf-8')

                producer.send_sync(msg, msg_id % len(producer.producers))
                
                # 定期更新全局计数器（减少锁竞争）
                if local_counter % 1000 == 0:
                    with message_id_lock:
                        if local_counter > message_id_counter[0]:
                            message_id_counter[0] = local_counter
        
        # 启动多个发送线程
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=send_worker, args=(i,), daemon=True)
            t.start()
            threads.append(t)
        
        # 主线程负责报告
        try:
            while True:
                time.sleep(0.5)  # 每0.5秒检查一次，更及时的报告
                current_time = time.time()
                current_count = producer.message_count
                
                # 定期报告（每达到报告间隔时，或每秒报告一次）
                elapsed = current_time - last_report_time
                if current_count >= last_report_count + report_interval or elapsed >= 1.0:
                    count_diff = current_count - last_report_count
                    instant_rate = count_diff / elapsed if elapsed > 0 else 0
                    total_elapsed = current_time - start_time
                    avg_rate = current_count / total_elapsed if total_elapsed > 0 else 0
                    print(f"[Producer] [{total_elapsed:.1f}s] 已发送: {current_count:,} 条 | "
                          f"瞬时速率: {instant_rate:,.0f} 消息/秒 | "
                          f"平均速率: {avg_rate:,.0f} 消息/秒")
                    last_report_time = current_time
                    last_report_count = current_count
        except KeyboardInterrupt:
            print("\n收到停止信号，正在关闭...")
        finally:
            producer.close()
            total_time = time.time() - start_time
            print(f"\n总计发送 {producer.message_count:,} 条消息，耗时 {total_time:.2f} 秒")
            if total_time > 0:
                print(f"平均速率: {producer.message_count / total_time:,.0f} 消息/秒")
    
    except Exception as e:
        print(f"发送消息时出错: {e}")
        import traceback
        traceback.print_exc()
        producer.close()


def main():
    send_messages_ultra_fast(
        service_url="pulsar://127.0.0.1:6650",
        topic="test-lancedb-topic",
        num_producers=20,          # 多个producer实例，多连接
        batch_size=10000,          # 大批量
        num_threads=50,            # 多线程并发发送
        report_interval=100000     # 每10万条报告一次
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nProducer已停止")

