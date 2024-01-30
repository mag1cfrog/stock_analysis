# src/common/log_config.py

import logging
import os

def setup_logging():
    # 创建 log 目录，如果它不存在
    log_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # 配置日志记录器
    logging.basicConfig(
        level=logging.ERROR,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(log_dir, "data_extraction_load.log")),
            logging.StreamHandler()
        ]
    )

    
