"""
日誌工具模組
"""

import os
import logging
from datetime import datetime


class Logger:
    """簡易日誌工具"""

    def __init__(self, log_dir="logs", name="bosch_camera"):
        os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(
            log_dir,
            f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
        )

        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        # 避免重複加入 handler
        if not self.logger.handlers:
            # 檔案 handler
            fh = logging.FileHandler(log_file, encoding='utf-8')
            fh.setLevel(logging.DEBUG)

            # 控制台 handler
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)

            formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)

            self.logger.addHandler(fh)
            self.logger.addHandler(ch)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def debug(self, msg):
        self.logger.debug(msg)
