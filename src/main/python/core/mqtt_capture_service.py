"""
Bosch Camera MQTT 觸發串流影像擷取系統

當收到 MQTT 訊息 (MotionAlarm State:true) 時，
擷取當下時間往前 600ms 與往後 600ms 的串流影像，
存放至 C:\Bosch\ 並按照年月日小時區分資料夾。
"""

import time
import json
import os
from datetime import datetime
from threading import Thread, Event
import paho.mqtt.client as mqtt
from src.main.python.core.stream_buffer import StreamBuffer
from src.main.python.utils.logger import Logger


class MQTTCaptureService:
    """MQTT 觸發影像擷取服務"""

    def __init__(self, mqtt_ip, mqtt_port, mqtt_timeout,
                 rtsp_url, save_root=r"C:\Bosch",
                 buffer_duration=0.6):
        """
        Args:
            mqtt_ip: MQTT Broker IP
            mqtt_port: MQTT Broker Port
            mqtt_timeout: MQTT 連線逾時 (秒)
            rtsp_url: Bosch 攝影機 RTSP 串流 URL
            save_root: 影像儲存根目錄
            buffer_duration: 前後緩衝時間 (秒), 預設 0.6 = 600ms
        """
        self.mqtt_ip = mqtt_ip
        self.mqtt_port = mqtt_port
        self.mqtt_timeout = mqtt_timeout
        self.rtsp_url = rtsp_url
        self.save_root = save_root
        self.buffer_duration = buffer_duration

        self.logger = Logger()
        self.stream_buffer = None
        self.mqtt_client = None
        self.stop_event = Event()

    def on_connect(self, client, userdata, flags, rc):
        """MQTT 連線回呼"""
        if rc == 0:
            self.logger.info("✅ 成功連線到 MQTT Broker")
            # 訂閱 MotionAlarm 主題
            client.subscribe("LPR/Response/#")
            self.logger.info("📡 已訂閱 LPR/Response/#")
        else:
            self.logger.error(f"❌ 連線失敗，錯誤代碼: {rc}")

    def on_message(self, client, userdata, msg):
        """MQTT 訊息回呼 - 判斷是否觸發擷取"""
        try:
            topic = msg.topic
            payload_str = msg.payload.decode('utf-8')

            self.logger.info(f"[MQTT] 主題: {topic} | 內容: {payload_str}")

            # 判斷觸發條件
            if self._should_capture(topic, payload_str):
                self.logger.info("🎯 觸發條件成立，開始擷取影像...")
                self._trigger_capture()

        except Exception as e:
            self.logger.error(f"❌ 訊息處理錯誤: {str(e)}")

    def _should_capture(self, topic, payload_str):
        """
        判斷是否符合擷取條件:
        - 主題包含: LPR/Response/onvif-ej/VideoSource/MotionAlarm/&1
        - 內容包含: "Source":{"Source":1},"Data":{"State":true}
        """
        # 條件 1: 檢查主題
        if "onvif-ej/VideoSource/MotionAlarm" not in topic:
            return False

        # 條件 2: 檢查 payload 內容
        try:
            data = json.loads(payload_str)
            source = data.get("Source", {})
            data_field = data.get("Data", {})

            if source.get("Source") == 1 and data_field.get("State") is True:
                return True
        except json.JSONDecodeError:
            # 如果不是 JSON，用字串比對
            if '"Source":{"Source":1}' in payload_str and '"State":true' in payload_str:
                return True

        return False

    def _trigger_capture(self):
        """觸發擷取: 取得前後 600ms 的影像"""
        if self.stream_buffer is None:
            self.logger.error("❌ 串流緩衝區尚未初始化")
            return

        trigger_time = datetime.now()

        # 等待後 600ms 的影像進入緩衝區
        time.sleep(self.buffer_duration)

        # 從緩衝區取得前後 600ms 的影格
        frames = self.stream_buffer.get_frames(
            trigger_time, self.buffer_duration
        )

        if not frames:
            self.logger.warning("⚠️ 未取得任何影格")
            return

        # 儲存影像
        save_dir = self._get_save_directory(trigger_time)
        saved_count = self._save_frames(frames, save_dir, trigger_time)

        self.logger.info(
            f"✅ 已儲存 {saved_count} 張影像至: {save_dir}"
        )

    def _get_save_directory(self, dt):
        """
        依照年月日小時建立資料夾
        例如: C:\Bosch\2026\05\26\14\
        """
        dir_path = os.path.join(
            self.save_root,
            dt.strftime("%Y"),
            dt.strftime("%m"),
            dt.strftime("%d"),
            dt.strftime("%H")
        )
        os.makedirs(dir_path, exist_ok=True)
        return dir_path

    def _save_frames(self, frames, save_dir, trigger_time):
        """儲存影格為圖片檔案"""
        import cv2

        saved = 0
        timestamp_prefix = trigger_time.strftime("%Y%m%d_%H%M%S_%f")

        for i, (frame, frame_time) in enumerate(frames):
            # 計算相對觸發時間的偏移 (ms)
            offset_ms = int(
                (frame_time - trigger_time).total_seconds() * 1000
            )
            sign = "+" if offset_ms >= 0 else ""

            filename = f"{timestamp_prefix}_{sign}{offset_ms}ms_{i:03d}.jpg"
            filepath = os.path.join(save_dir, filename)

            try:
                cv2.imwrite(filepath, frame)
                saved += 1
            except Exception as e:
                self.logger.error(f"❌ 儲存失敗 {filepath}: {e}")

        return saved

    def start(self):
        """啟動服務: 串流緩衝 + MQTT 監聽"""
        self.logger.info("🚀 啟動 Bosch Camera MQTT 擷取服務...")

        # 1. 啟動串流緩衝區 (持續擷取影像到記憶體環形緩衝)
        self.stream_buffer = StreamBuffer(
            rtsp_url=self.rtsp_url,
            buffer_seconds=self.buffer_duration + 1.0  # 多緩衝 1 秒
        )
        self.stream_buffer.start()
        self.logger.info(f"📹 串流緩衝啟動: {self.rtsp_url}")

        # 2. 啟動 MQTT 監聽
        mqtt_thread = Thread(target=self._run_mqtt, daemon=True)
        mqtt_thread.start()
        self.logger.info(f"📡 MQTT 監聽啟動: {self.mqtt_ip}:{self.mqtt_port}")

    def _run_mqtt(self):
        """MQTT 背景執行"""
        try:
            self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
            self.mqtt_client.on_connect = self.on_connect
            self.mqtt_client.on_message = self.on_message

            self.mqtt_client.connect(
                self.mqtt_ip, self.mqtt_port, self.mqtt_timeout
            )
            self.mqtt_client.loop_forever()
        except Exception as e:
            self.logger.error(f"❌ MQTT 執行異常: {e}")

    def stop(self):
        """停止服務"""
        self.stop_event.set()
        if self.stream_buffer:
            self.stream_buffer.stop()
        if self.mqtt_client:
            self.mqtt_client.disconnect()
        self.logger.info("🛑 服務已停止")


if __name__ == "__main__":
    # ===== 請修改以下設定 =====
    MQTT_IP = "127.0.0.1"
    MQTT_PORT = 1883
    MQTT_TIMEOUT = 60
    RTSP_URL = "rtsp://username:password@camera_ip:554/rtsp_stream"
    SAVE_ROOT = r"C:\Bosch"
    # ==========================

    service = MQTTCaptureService(
        mqtt_ip=MQTT_IP,
        mqtt_port=MQTT_PORT,
        mqtt_timeout=MQTT_TIMEOUT,
        rtsp_url=RTSP_URL,
        save_root=SAVE_ROOT,
        buffer_duration=0.6
    )
    service.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        service.stop()
        print("\n🛑 程式已手動停止")
