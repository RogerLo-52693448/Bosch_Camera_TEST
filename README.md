# Bosch_Camera_TEST

## 專案描述
串流影像擷取 - Bosch 攝影機串流影像擷取與處理工具

## 快速開始 (Quick Start)

1. **首先閱讀 AIAgent_init.md** - 包含 AI Agent 的基本規則
2. 在 `src/main/python/` 下使用正確的模組結構
3. 在每個已完成的功能之後提交

## 專案結構

```
Bosch_Camera_TEST/
├── AIAgent_init.md        # AI Agent 規則
├── README.md              # 專案文件
├── .gitignore             # Git 忽略模式
├── requirements.txt       # Python 依賴
├── src/
│   ├── main/
│   │   ├── python/
│   │   │   ├── __init__.py
│   │   │   ├── core/      # 核心業務邏輯（串流擷取）
│   │   │   ├── utils/     # 工具函數
│   │   │   ├── models/    # 資料模型
│   │   │   ├── services/  # 服務層
│   │   │   └── api/       # API 端點
│   │   └── resources/
│   │       ├── config/    # 配置檔
│   │       └── assets/    # 靜態資源
│   └── test/
│       ├── unit/          # 單元測試
│       └── integration/   # 整合測試
├── docs/                  # 文件
├── tools/                 # 開發工具
├── examples/              # 使用範例
└── output/                # 產生的輸出文件
```

## 安裝與使用

```bash
# 建立虛擬環境
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate  # Windows

# 安裝依賴
pip install -r requirements.txt

# 執行主程式
python -m src.main.python.core.stream_capture
```

## 開發指南

- **永遠先搜索** 再創建新文件
- **擴展現有** 功能而不是重複
- **單一事實來源** 適用於所有功能
- 在每個已完成的任務後提交並推送到 GitHub

## License

MIT
