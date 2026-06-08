"""
Bosch Camera MQTT 觸發串流影像擷取系統

當收到 MQTT 訊息 (MotionAlarm State:true) 時，
以觸發時間為中心，往前1秒、往後1秒，每200ms抽取一張，
加上當下時間共11張照片，
存放至 C:\\Bosch\\ 並按照年月日小時區分資料夾。
每筆觸發事件獨立建立子資料夾，檔名含序號確保排序正確。

架構:
- 執行緒 1 (Reader): 專責快速讀取 RTSP 影格，避免內部緩衝區溢出
- 執行緒 2 (Validator): 驗證影格完整性，只保留有效影格
- 執行緒 3 (MQTT): 監聽觸發訊息
- 主執行緒: 保持程式運行
"""

import cv2
import numpy as np
import time
import json
import os
import logging
from datetime import datetime, timedelta
from threading import Thread, Lock, Event
from collections import deque
from queue import Queue, Empty
import paho.mqtt.client as mqtt


# ============================================================
# Logger
# ============================================================
class Logger:
    def __init__(self, log_dir="logs", name="bosch_camera"):
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"{name}_{datetime.now().strftime('%Y%m%d')}.log")
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        if not self.logger.handlers:
            fh = logging.FileHandler(log_file, encoding='utf-8')
            fh.setLevel(logging.DEBUG)
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            self.logger.addHandler(fh)
            self.logger.addHandler(ch)

    def info(self, msg): self.logger.info(msg)
    def error(self, msg): self.logger.error(msg)
    def warning(self, msg): self.logger.warning(msg)
    def debug(self, msg): self.logger.debug(msg)


# ============================================================
# StreamBuffer - 多執行緒 RTSP 環形緩衝區
# ============================================================
class StreamBuffer:
    """
    多執行緒架構:
    - Reader Thread: 專責讀取 RTSP，速度最快，不做任何額外處理
    - Validator Thread: 從原始佇列取影格，驗證後放入有效緩衝區
    """

    def __init__(self, rtsp_url, buffer_seconds=4.0):
        self.rtsp_url = rtsp_url
        self.buffer_seconds = buffer_seconds

        # 原始影格佇列 (Reader → Validator)
        self.raw_queue = Queue(maxsize=60)

        # 有效影格緩衝區 (已驗證，供抽取使用)
        self.valid_buffer = deque(maxlen=500)
        self.buffer_lock = Lock()

        self.cap = None
        self.running = False
        self.stop_event = Event()
        self.reader_thread = None
        self.validator_thread = None
        self.fps = 30

        # 統計
        self.total_read = 0
        self.total_valid = 0
        self.total_dropped = 0

    def start(self):
        self.running = True
        self.stop_event.clear()

        self.reader_thread = Thread(target=self._reader_loop, daemon=True, name="RTSP-Reader")
        self.reader_thread.start()

        self.validator_thread = Thread(target=self._validator_loop, daemon=True, name="Frame-Validator")
        self.validator_thread.start()

    def stop(self):
        self.running = False
        self.stop_event.set()
        if self.cap and self.cap.isOpened():
            self.cap.release()

    def _create_capture(self):
        """建立 RTSP 連線，強制 TCP 傳輸"""
        os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp"
        cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
        if cap.isOpened():
            cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        return cap

    def _reader_loop(self):
        """讀取執行緒: 盡可能快速地讀取影格"""
        retry_count = 0
        max_retries = 10

        while self.running and not self.stop_event.is_set():
            try:
                self.cap = self._create_capture()

                if not self.cap.isOpened():
                    retry_count += 1
                    print(f"  ⚠️ RTSP 連線失敗 (重試 {retry_count}/{max_retries})")
                    if retry_count >= max_retries:
                        print("  ❌ RTSP 連線重試次數已達上限")
                        break
                    time.sleep(2)
                    continue

                fps = self.cap.get(cv2.CAP_PROP_FPS)
                if fps > 0:
                    self.fps = fps
                print(f"  📹 RTSP 連線成功 (TCP) | FPS: {self.fps:.1f}")
                retry_count = 0

                # 丟棄前幾個影格，等待 I-frame
                for _ in range(10):
                    self.cap.grab()

                print("  ✅ Reader 執行緒啟動")

                while self.running and not self.stop_event.is_set():
                    grabbed = self.cap.grab()
                    if not grabbed:
                        print("  ⚠️ grab() 失敗，重新連線...")
                        break

                    ret, frame = self.cap.retrieve()
                    if not ret or frame is None:
                        continue

                    self.total_read += 1
                    timestamp = datetime.now()

                    if self.raw_queue.full():
                        try:
                            self.raw_queue.get_nowait()
                        except Empty:
                            pass
                    self.raw_queue.put((frame, timestamp), block=False)

            except Exception as e:
                print(f"  ❌ Reader 異常: {e}")
                time.sleep(2)
            finally:
                if self.cap and self.cap.isOpened():
                    self.cap.release()

    def _validator_loop(self):
        """驗證執行緒: 驗證完整性後放入有效緩衝區"""
        print("  ✅ Validator 執行緒啟動")

        while self.running and not self.stop_event.is_set():
            try:
                frame, timestamp = self.raw_queue.get(timeout=0.5)
            except Empty:
                continue

            if self._validate_frame(frame):
                self.total_valid += 1
                with self.buffer_lock:
                    self.valid_buffer.append((frame, timestamp))
            else:
                self.total_dropped += 1
                if self.total_dropped % 20 == 1:
                    total = max(self.total_read, 1)
                    drop_rate = (self.total_dropped / total) * 100
                    print(f"  ⚠️ 已丟棄 {self.total_dropped} 個損壞影格 (丟棄率: {drop_rate:.1f}%)")

    def _validate_frame(self, frame):
        """驗證影格完整性"""
        if frame is None:
            return False

        h, w = frame.shape[:2]
        if h == 0 or w == 0:
            return False

        mean_val = frame.mean()
        if mean_val < 3 or mean_val > 252:
            return False

        success, encoded = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
        if not success:
            return False

        file_size_kb = len(encoded) / 1024
        if file_size_kb < 8:
            return False

        return True

    def get_nearest_frame(self, target_time):
        """取得最接近 target_time 的已驗證影格"""
        with self.buffer_lock:
            if not self.valid_buffer:
                return None
            best_frame = None
            best_diff = None
            for frame, ts in self.valid_buffer:
                diff = abs((ts - target_time).total_seconds())
                if best_diff is None or diff < best_diff:
                    best_diff = diff
                    best_frame = (frame.copy(), ts)
            return best_frame

    def get_second_nearest_frame(self, target_time, exclude_time):
        """取得第二接近的已驗證影格"""
        with self.buffer_lock:
            if not self.valid_buffer:
                return None
            best_frame = None
            best_diff = None
            for frame, ts in self.valid_buffer:
                if abs((ts - exclude_time).total_seconds()) < 0.015:
                    continue
                diff = abs((ts - target_time).total_seconds())
                if best_diff is None or diff < best_diff:
                    best_diff = diff
                    best_frame = (frame.copy(), ts)
            return best_frame

    @property
    def is_running(self):
        return (self.running and
                self.reader_thread and self.reader_thread.is_alive())

    @property
    def buffer_size(self):
        with self.buffer_lock:
            return len(self.valid_buffer)


# ============================================================
# MQTTCaptureService - 主服務
# ============================================================
class MQTTCaptureService:
    """MQTT 觸發影像擷取服務"""

    MAX_RETRY_COUNT = 3
    RETRY_DELAY_SEC = 0.2
    CAPTURE_WAIT_SEC = 2.0

    def __init__(self, mqtt_ip, mqtt_port, mqtt_timeout,
                 rtsp_url, save_root=r"C:\Bosch"):
        self.mqtt_ip = mqtt_ip
        self.mqtt_port = mqtt_port
        self.mqtt_timeout = mqtt_timeout
        self.rtsp_url = rtsp_url
        self.save_root = save_root

        self.logger = Logger()
        self.stream_buffer = None
        self.mqtt_client = None
        self.stop_event = Event()
        self.trigger_count = 0

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info("✅ 成功連線到 MQTT Broker")
            client.subscribe("LPR/Response/#")
            self.logger.info("📡 已訂閱 LPR/Response/#")
        else:
            self.logger.error(f"❌ 連線失敗，錯誤代碼: {rc}")

    def on_message(self, client, userdata, msg):
        try:
            topic = msg.topic
            payload_str = msg.payload.decode('utf-8')
            self.logger.info(f"[MQTT] 主題: {topic} | 內容: {payload_str}")

            if self._should_capture(topic, payload_str):
                self.trigger_count += 1
                self.logger.info(f"🎯 觸發條件成立! (第 {self.trigger_count} 次) 開始擷取影像...")
                capture_thread = Thread(target=self._trigger_capture, daemon=True)
                capture_thread.start()

        except Exception as e:
            self.logger.error(f"❌ 訊息處理錯誤: {str(e)}")

    def _should_capture(self, topic, payload_str):
        """
        觸發條件:
        1. 主題包含: onvif-ej/VideoSource/MotionAlarm
        2. 內容包含: "Source":{"Source": "1"},"Data":{"State": "true"}
        """
        if "onvif-ej/VideoSource/MotionAlarm" not in topic:
            return False

        try:
            data = json.loads(payload_str)
            source = data.get("Source", {})
            data_field = data.get("Data", {})

            if (str(source.get("Source", "")) == "1" and
                    str(data_field.get("State", "")).lower() == "true"):
                return True
        except json.JSONDecodeError:
            if '"Source": "1"' in payload_str and '"State": "true"' in payload_str:
                return True

        return False

    def _capture_single_frame(self, target_time, offset_label):
        """擷取單一影格 (含重試)"""
        for attempt in range(1, self.MAX_RETRY_COUNT + 1):
            result = self.stream_buffer.get_nearest_frame(target_time)

            if result is None:
                if attempt < self.MAX_RETRY_COUNT:
                    time.sleep(self.RETRY_DELAY_SEC)
                continue

            frame, actual_time = result

            if frame is not None and frame.shape[0] > 0 and frame.shape[1] > 0:
                success, encoded = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 90])
                if success and len(encoded) / 1024 >= 8:
                    if attempt > 1:
                        self.logger.info(f"    ✅ [{offset_label}] 第{attempt}次重試成功")
                    return frame, actual_time

            if attempt < self.MAX_RETRY_COUNT:
                time.sleep(self.RETRY_DELAY_SEC)
                result2 = self.stream_buffer.get_second_nearest_frame(target_time, actual_time)
                if result2:
                    frame2, at2 = result2
                    success2, enc2 = cv2.imencode('.jpg', frame2, [cv2.IMWRITE_JPEG_QUALITY, 90])
                    if success2 and len(enc2) / 1024 >= 8:
                        self.logger.info(f"    ✅ [{offset_label}] 使用替代影格")
                        return frame2, at2

        self.logger.warning(f"    ⚠️ [{offset_label}] 重試失敗，跳過")
        return None

    def _trigger_capture(self):
        """
        觸發擷取: 一次抽 11 張照片
        每個觸發事件建立獨立子資料夾，檔名加序號確保排序
        """
        if self.stream_buffer is None:
            self.logger.error("❌ 串流緩衝區尚未初始化")
            return

        trigger_time = datetime.now()
        offsets_ms = [-1000, -800, -600, -400, -200, 0, +200, +400, +600, +800, +1000]

        self.logger.info(f"  ⏳ 等待 {self.CAPTURE_WAIT_SEC}s 讓 +1000ms 影格進入緩衝...")
        time.sleep(self.CAPTURE_WAIT_SEC)

        # 每筆觸發事件建立獨立資料夾
        event_folder_name = trigger_time.strftime("%Y%m%d_%H%M%S_%f")[:-3]
        save_dir = self._get_save_directory(trigger_time, event_folder_name)

        self.logger.info(f"  📁 事件資料夾: {save_dir}")

        saved_count = 0
        failed_count = 0

        for idx, offset in enumerate(offsets_ms, start=1):
            target_time = trigger_time + timedelta(milliseconds=offset)

            if offset == 0:
                offset_label = "0ms"
            elif offset > 0:
                offset_label = f"+{offset}ms"
            else:
                offset_label = f"{offset}ms"

            capture_result = self._capture_single_frame(target_time, offset_label)

            if capture_result is None:
                failed_count += 1
                continue

            frame, actual_time = capture_result
            # 檔名格式: 序號_偏移量.jpg (確保排序正確)
            filename = f"{idx:02d}_{offset_label}.jpg"
            filepath = os.path.join(save_dir, filename)

            try:
                cv2.imwrite(filepath, frame, [cv2.IMWRITE_JPEG_QUALITY, 90])
                file_size = os.path.getsize(filepath) / 1024
                saved_count += 1
                self.logger.info(f"  📷 已儲存: {filename} ({file_size:.1f}KB)")
            except Exception as e:
                self.logger.error(f"  ❌ 儲存失敗 {filepath}: {e}")
                failed_count += 1

        if failed_count == 0:
            self.logger.info(f"✅ 完美! 共儲存 {saved_count}/11 張影像至: {save_dir}")
        else:
            self.logger.warning(f"⚠️ 共儲存 {saved_count}/11 (失敗 {failed_count}) 至: {save_dir}")

    def _get_save_directory(self, dt, event_folder):
        """
        依照年/月/日/小時/事件時間戳 建立資料夾
        每個觸發事件一個獨立子資料夾
        """
        dir_path = os.path.join(
            self.save_root,
            dt.strftime("%Y"),
            dt.strftime("%m"),
            dt.strftime("%d"),
            dt.strftime("%H"),
            event_folder
        )
        os.makedirs(dir_path, exist_ok=True)
        return dir_path

    def start(self):
        """啟動服務"""
        print("=" * 55)
        print("  Bosch Camera MQTT 觸發串流影像擷取系統 (多執行緒版)")
        print("=" * 55)
        print(f"  RTSP: {self.rtsp_url}")
        print(f"  傳輸: TCP (避免 UDP 丟包)")
        print(f"  MQTT: {self.mqtt_ip}:{self.mqtt_port}")
        print(f"  存檔: {self.save_root}")
        print(f"  擷取: 前後各1秒, 每200ms一張, 共11張")
        print(f"  結構: 年/月/日/時/觸發時間戳/序號_偏移.jpg")
        print("=" * 55)
        print()

        # 1. 啟動串流緩衝區
        print("[1/3] 啟動 RTSP 串流 (多執行緒模式)...")
        self.stream_buffer = StreamBuffer(
            rtsp_url=self.rtsp_url,
            buffer_seconds=4.0
        )
        self.stream_buffer.start()
        time.sleep(4)

        if not self.stream_buffer.is_running:
            print("  ❌ RTSP 串流啟動失敗!")
            return

        print(f"  📊 緩衝區已有 {self.stream_buffer.buffer_size} 個有效影格")

        # 2. 測試擷取
        print("\n[2/3] 測試擷取照片...")
        test_dir = os.path.join(self.save_root, "test")
        os.makedirs(test_dir, exist_ok=True)
        result = self.stream_buffer.get_nearest_frame(datetime.now())
        if result:
            frame, ts = result
            test_path = os.path.join(test_dir, f"test_{ts.strftime('%Y%m%d_%H%M%S')}.jpg")
            cv2.imwrite(test_path, frame, [cv2.IMWRITE_JPEG_QUALITY, 90])
            file_size = os.path.getsize(test_path) / 1024
            print(f"  ✅ 測試照片: {test_path} ({file_size:.1f}KB)")
        else:
            print("  ⚠️ 測試擷取失敗，等待更多影格...")

        # 3. 啟動 MQTT
        print("\n[3/3] 啟動 MQTT 監聽...")
        mqtt_thread = Thread(target=self._run_mqtt, daemon=True, name="MQTT-Listener")
        mqtt_thread.start()
        print("  ✅ 系統已就緒! 等待 MQTT 觸發訊息...")
        print()

    def _run_mqtt(self):
        try:
            self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
            self.mqtt_client.on_connect = self.on_connect
            self.mqtt_client.on_message = self.on_message
            self.mqtt_client.connect(self.mqtt_ip, self.mqtt_port, self.mqtt_timeout)
            self.mqtt_client.loop_forever()
        except Exception as e:
            self.logger.error(f"❌ MQTT 執行異常: {e}")

    def stop(self):
        self.stop_event.set()
        if self.stream_buffer:
            self.stream_buffer.stop()
        if self.mqtt_client:
            self.mqtt_client.disconnect()
        self.logger.info("🛑 服務已停止")


# ============================================================
# 主程式
# ============================================================
if __name__ == "__main__":
    # ===== 設定 =====
    MQTT_IP = "127.0.0.1"
    MQTT_PORT = 1883
    MQTT_TIMEOUT = 60
    RTSP_URL = "rtsp://service:!QAZ2wsx@192.168.50.152/rtsp_tunnel"
    SAVE_ROOT = r"C:\Bosch"
    # =================

    service = MQTTCaptureService(
        mqtt_ip=MQTT_IP,
        mqtt_port=MQTT_PORT,
        mqtt_timeout=MQTT_TIMEOUT,
        rtsp_url=RTSP_URL,
        save_root=SAVE_ROOT
    )
    service.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        service.stop()
        print("\n🛑 程式已手動停止")
