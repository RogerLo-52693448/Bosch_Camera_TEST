# AIAgent_init.md - Bosch_Camera_TEST

> **Documentation Version**: 1.0
> **Last Updated**: 2026-05-26
> **Project**: Bosch_Camera_TEST
> **Description**: 串流影像擷取
> **Features**: GitHub auto-backup, Task agents, technical debt prevention

## CRITICAL RULES

### RULE ACKNOWLEDGMENT REQUIRED
> Before starting ANY task, AIAgent must respond with:
> "✅ 已確認關鍵規則 - 我將遵守 AIAgent_init.md 中列出的所有禁令和要求"

### ABSOLUTE PROHIBITIONS
- **NEVER** create new files in root directory → use proper module structure
- **NEVER** write output files directly to root directory → use output/
- **NEVER** create duplicate files → ALWAYS extend existing
- **NEVER** create multiple implementations of same concept → single source of truth
- **NEVER** hardcode values that should be configurable → use config files
- **NEVER** use naming like enhanced_, improved_, new_, v2_ → extend original files

### MANDATORY REQUIREMENTS
- **COMMIT**: 在每個已完成的任務/階段後進行提交
- **GITHUB BACKUP**: 在每次提交後推送: `git push origin main`
- **READ FILES FIRST**: 在編輯文件之前，必須先閱讀該文件
- **DEBT PREVENTION**: 在創建新文件之前，檢查是否存在現有相似功能
- **SINGLE SOURCE OF TRUTH**: 每個功能/概念只有一個權威的實作

## PROJECT STRUCTURE
```
src/main/python/
├── core/          # 核心業務邏輯（串流擷取）
├── utils/         # 工具函數
├── models/        # 資料模型
├── services/      # 服務層
└── api/           # API 端點
```

## COMMON COMMANDS
```bash
# 執行主程式
python -m src.main.python.core.stream_capture

# 執行測試
python -m pytest src/test/

# Git 備份
git add . && git commit -m "描述" && git push origin main
```
