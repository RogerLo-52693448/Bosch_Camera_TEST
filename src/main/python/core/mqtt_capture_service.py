"""
Bosch Camera MQTT 觸發串流影像擷取系統

當收到 MQTT 訊息 (MotionAlarm State:true) 時，
以觸發時間為中心，往前1秒、往後1秒，每200ms抽取一張，
加上當下時間共11張照片，
存放至 C:\\Bosch\\ 並按照年月日小時區分資料夾。
"""

import cv2
import time
import json
import os
import logging
from datetime import datetime, timedelta
from threading import Thread, Lock, Event
from collections import deque
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
# StreamBuffer - RTSP 環形緩衝區
# ============================================================
class StreamBuffer:
    def __init__(self, rtsp_url, buffer_seconds=3.0):
        self.rtsp_url = rtsp_url
        self.buffer_seconds = buffer_seconds
        self.buffer = deque(maxlen=1000)
        self.lock = Lock()
        self.cap = None
        self.running = False
        self.stop_event = Event()
        self.thread = None
        self.fps = 30

    def start(self):
        self.running = True
        self.stop_event.clear()
        self.thread = Thread(target=self._capture_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        self.stop_event.set()
        if self.cap and self.cap.isOpened():
            self.cap.release()

    def _capture_loop(self):
        retry_count = 0
        max_retries = 10
        while self.running and not self.stop_event.is_set():
            try:
                self.cap = cv2.VideoCapture(self.rtsp_url)
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
                max_frames = int(self.buffer_seconds * self.fps * 2)
                self.buffer = deque(maxlen=max(max_frames, 300))
                print(f"  📹 RTSP 連線成功 | FPS: {self.fps:.1f} | 緩衝: {self.buffer_seconds}s")
                retry_count = 0

                while self.running and not self.stop_event.is_set():
                    ret, frame = self.cap.read()
                    if not ret:
                        print("  ⚠️ 串流讀取失敗，嘗試重新連線...")
                        break
                    timestamp = datetime.now()
                    with self.lock:
                        self.buffer.append((frame, timestamp))

            except Exception as e:
                print(f"  ❌ 串流擷取異常: {e}")
                time.sleep(2)
            finally:
                if self.cap and self.cap.isOpened():
                    self.cap.release()

    def get_nearest_frame(self, target_time):
        """取得最接近 target_time 的單一影格"""
        with self.lock:
            if not self.buffer:
                return None
            best_frame = None
            best_diff = None
            for frame, ts in self.buffer:
                diff = abs((ts - target_time).total_seconds())
                if best_diff is None or diff < best_diff:
                    best_diff = diff
                    best_frame = (frame.copy(), ts)
            return best_frame

    @property
    def is_running(self):
        return self.running and self.thread and self.thread.is_alive()

    @property
    def buffer_size(self):
        with self.lock:
            return len(self.buffer)


# ============================================================
# MQTTCaptureService - 主服務
# ============================================================
class MQTTCaptureService:
    """MQTT 觸發影像擷取服務"""

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
                self.logger.info("🎯 觸發條件成立! 開始擷取影像...")
                self._trigger_capture()

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

    def _trigger_capture(self):
        """
        觸發擷取: 一次抽 11 張照片
        以觸發時間為中心，往前1秒、往後1秒，每200ms一張
        -1000ms, -800ms, -600ms, -400ms, -200ms, 0ms, +200ms, +400ms, +600ms, +800ms, +1000ms
        """
        if self.stream_buffer is None:
            self.logger.error("❌ 串流緩衝區尚未初始化")
            return

        trigger_time = datetime.now()

        # 定義 11 個擷取時間點 (相對觸發時間的偏移 ms)
        offsets_ms = [-1000, -800, -600, -400, -200, 0, +200, +400, +600, +800, +1000]

        # 等待往後 1000ms 的影格進入緩衝區
        time.sleep(1.1)

        save_dir = self._get_save_directory(trigger_time)
        timestamp_prefix = trigger_time.strftime("%Y%m%d_%H%M%S_%f")[:-3]
        saved_count = 0

        for offset in offsets_ms:
            target_time = trigger_time + timedelta(milliseconds=offset)
            result = self.stream_buffer.get_nearest_frame(target_time)

            if result is None:
                self.logger.warning(f"  ⚠️ 未找到 {offset:+d}ms 的影格")
                continue

            frame, actual_time = result

            if offset == 0:
                sign_str = "0"
            elif offset > 0:
                sign_str = f"+{offset}"
            else:
                sign_str = f"{offset}"

            filename = f"{timestamp_prefix}_{sign_str}ms.jpg"
            filepath = os.path.join(save_dir, filename)

            try:
                cv2.imwrite(filepath, frame)
                saved_count += 1
                self.logger.info(f"  📷 已儲存: {filename}")
            except Exception as e:
                self.logger.error(f"  ❌ 儲存失敗 {filepath}: {e}")

        self.logger.info(f"✅ 共儲存 {saved_count}/11 張影像至: {save_dir}")

    def _get_save_directory(self, dt):
        """依照年月日小時建立資料夾"""
        dir_path = os.path.join(
            self.save_root,
            dt.strftime("%Y"),
            dt.strftime("%m"),
            dt.strftime("%d"),
            dt.strftime("%H")
        )
        os.makedirs(dir_path, exist_ok=True)
        return dir_path

    def start(self):
        """啟動服務"""
        print("=" * 50)
        print("  Bosch Camera MQTT 觸發串流影像擷取系統")
        print("=" * 50)
        print(f"  RTSP: {self.rtsp_url}")
        print(f"  MQTT: {self.mqtt_ip}:{self.mqtt_port}")
        print(f"  存檔: {self.save_root}")
        print(f"  擷取: 前後各1秒, 每200ms一張, 共11張")
        print("=" * 50)
        print()

        # 1. 啟動串流緩衝區
        print("[1/3] 啟動 RTSP 串流連線...")
        self.stream_buffer = StreamBuffer(
            rtsp_url=self.rtsp_url,
            buffer_seconds=3.0  # 緩衝 3 秒確保前後 1 秒都有資料
        )
        self.stream_buffer.start()
        time.sleep(3)

        if not self.stream_buffer.is_running:
            print("  ❌ RTSP 串流啟動失敗!")
            return

        # 2. 測試擷取
        print("\n[2/3] 測試擷取照片...")
        test_dir = os.path.join(self.save_root, "test")
        os.makedirs(test_dir, exist_ok=True)
        result = self.stream_buffer.get_nearest_frame(datetime.now())
        if result:
            frame, ts = result
            test_path = os.path.join(test_dir, f"test_{ts.strftime('%Y%m%d_%H%M%S')}.jpg")
            cv2.imwrite(test_path, frame)
            print(f"  ✅ 測試照片已儲存: {test_path}")
        else:
            print("  ⚠️ 測試擷取失敗，但仍繼續啟動服務")

        # 3. 啟動 MQTT
        print("\n[3/3] 啟動 MQTT 監聽...")
        mqtt_thread = Thread(target=self._run_mqtt, daemon=True)
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
