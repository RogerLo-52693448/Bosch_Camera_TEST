"""
RTSP 串流環形緩衝區

持續從 RTSP 串流擷取影格並儲存在記憶體環形緩衝區中，
當觸發事件發生時，可取得指定時間範圍內的影格。
"""

import cv2
import time
from datetime import datetime, timedelta
from threading import Thread, Lock, Event
from collections import deque


class StreamBuffer:
    """RTSP 串流環形緩衝區"""

    def __init__(self, rtsp_url, buffer_seconds=2.0):
        """
        Args:
            rtsp_url: RTSP 串流 URL
            buffer_seconds: 緩衝區保留時間 (秒)
        """
        self.rtsp_url = rtsp_url
        self.buffer_seconds = buffer_seconds

        # 環形緩衝區: 儲存 (frame, timestamp) 元組
        self.buffer = deque(maxlen=1000)
        self.lock = Lock()

        self.cap = None
        self.running = False
        self.stop_event = Event()
        self.thread = None
        self.fps = 30  # 預設 FPS，連線後會更新

    def start(self):
        """啟動串流擷取執行緒"""
        self.running = True
        self.stop_event.clear()
        self.thread = Thread(target=self._capture_loop, daemon=True)
        self.thread.start()

    def stop(self):
        """停止串流擷取"""
        self.running = False
        self.stop_event.set()
        if self.cap and self.cap.isOpened():
            self.cap.release()

    def _capture_loop(self):
        """持續擷取影格的背景迴圈"""
        retry_count = 0
        max_retries = 10

        while self.running and not self.stop_event.is_set():
            try:
                # 建立連線
                self.cap = cv2.VideoCapture(self.rtsp_url)

                if not self.cap.isOpened():
                    retry_count += 1
                    print(f"⚠️ RTSP 連線失敗 (重試 {retry_count}/{max_retries}): {self.rtsp_url}")
                    if retry_count >= max_retries:
                        print("❌ RTSP 連線重試次數已達上限")
                        break
                    time.sleep(2)
                    continue

                # 取得實際 FPS
                fps = self.cap.get(cv2.CAP_PROP_FPS)
                if fps > 0:
                    self.fps = fps

                # 更新 buffer maxlen 以容納足夠影格
                max_frames = int(self.buffer_seconds * self.fps * 2)
                self.buffer = deque(maxlen=max(max_frames, 100))

                print(f"📹 RTSP 連線成功 | FPS: {self.fps:.1f} | 緩衝: {self.buffer_seconds}s")
                retry_count = 0

                # 持續擷取
                while self.running and not self.stop_event.is_set():
                    ret, frame = self.cap.read()
                    if not ret:
                        print("⚠️ 串流讀取失敗，嘗試重新連線...")
                        break

                    timestamp = datetime.now()

                    with self.lock:
                        self.buffer.append((frame, timestamp))

                    # 控制擷取速率
                    time.sleep(1.0 / self.fps * 0.8)

            except Exception as e:
                print(f"❌ 串流擷取異常: {e}")
                time.sleep(2)

            finally:
                if self.cap and self.cap.isOpened():
                    self.cap.release()

    def get_frames(self, trigger_time, duration_seconds=0.6):
        """
        取得觸發時間前後指定秒數的影格

        Args:
            trigger_time: 觸發時間 (datetime)
            duration_seconds: 前後取得的秒數 (預設 0.6 = 600ms)

        Returns:
            list of (frame, timestamp) 元組
        """
        start_time = trigger_time - timedelta(seconds=duration_seconds)
        end_time = trigger_time + timedelta(seconds=duration_seconds)

        result = []
        with self.lock:
            for frame, ts in self.buffer:
                if start_time <= ts <= end_time:
                    result.append((frame.copy(), ts))

        return result

    @property
    def is_running(self):
        return self.running and self.thread and self.thread.is_alive()

    @property
    def buffer_size(self):
        with self.lock:
            return len(self.buffer)
