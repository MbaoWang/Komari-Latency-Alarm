# Komari 延遲波動告警插件（Komari-Latency-Alarm）

偵測 [Komari](https://github.com/komari-monitor/komari) 延遲監測任務的**突發延遲波動**、**緩慢爬升**與**持續高丟包（疑似被牆／阻斷）**，異常與恢復時自動透過 Komari 內建通知或自訂 Telegram Bot 推播。

## 主要功能

- **中位數基準防尖峰**：短期基準（預設前 30 分鐘）採有效延遲**中位數**，抵抗單次尖峰干擾；同時檢查相對幅度（%）與絕對毫秒（ms）雙重門檻，並需連續多筆達標才告警。
- **長期基準抓緩慢爬升**：每 30 分鐘背景更新一次長期中位數（預設 24 小時），偵測數小時內緩慢墊高的「溫水煮青蛙」型延遲變化。
- **持續高丟包告警（疑似被牆／阻斷）**：區分「節點離線」與「節點在線但線路阻斷」；節點持續回報但連續指定分鐘（預設 15 分鐘）丟包率達門檻（預設 80%）才告警，恢復後自動推播。
- **遲滯恢復與防反向誤報**：
  - 恢復時套用 70% 遲滯區間（Hysteresis），避免在門檻邊緣反覆抖動洗版。
  - 恢復後自動記錄異常時段並在計算基準時排除，防止恢復後 30 秒立刻誤報反向波動。
  - 支援告警冷卻時間（預設 15 分鐘）與逾時重定基準（Rebase）通知。
- **突發彙整防 429 限流**：單輪同類型通知達 4 筆以上自動分批彙整摘要推播，避免大量節點同時波動觸發 Telegram API 限流。
- **V4 / V6 分組監控**：支援 V4 與 V6 兩組獨立「節點 × 監測任務」聯集過濾；未選任務時自動匹配名稱含 `V4`／`V6` 的任務，全留空則監控全部。

## 安裝方式

1. 從 [Releases](https://github.com/MbaoWang/Komari-Latency-Alarm/releases) 下載最新的 `komari-latency-alert.zip`（或將本儲存庫根目錄的 `komari-plugin.json` 與 `script.js` 打包為 zip）。
2. 在 Komari 後台的「插件管理」上傳並啟用。
3. 依需求調整推播方式、門檻與監控範圍；Bot Token 與 Chat ID 留空時會直接使用 Komari 內建通知管道。

## 設定說明

| 分類 | 設定項 | 預設值 | 說明 |
| :--- | :--- | :--- | :--- |
| 推播設定 | `bot_token` / `chat_id` | 留空 | 留空時走 Komari 內建通知；兩者皆填時改走自帶 Telegram Bot |
| 推播設定 | `thread_id` / `api_base` | 留空 / 官方位址 | 僅自帶 Bot 有效，支援群組 Topics 與自架反代 |
| 延遲偵測 | `threshold_percent` / `threshold_ms` | `50`% / `30` ms | 須同時超過百分比與毫秒門檻 |
| 延遲偵測 | `confirm_samples` | `3` 筆 | 連續確認筆數，過濾單次尖峰 |
| 延遲偵測 | `baseline_minutes` | `30` 分鐘 | 短期中位數基準視窗 |
| 延遲偵測 | `long_baseline_hours` | `24` 小時 | 長期中位數基準視窗，設為 `0` 關閉 |
| 延遲偵測 | `direction` | `both` | `both`（上升與下降皆報）或 `up`（僅報上升） |
| 高丟包偵測 | `loss_minutes` / `loss_percent` | `15` 分鐘 / `80`% | 持續高丟包偵測，`loss_minutes` 設為 `0` 關閉 |
| 告警行為 | `notify_recovery` | 開啟 | 延遲或高丟包恢復時通知 |
| 告警行為 | `rebase_minutes` / `notify_rebase` | `60` 分鐘 / 開啟 | 逾時未恢復時改以新延遲作為基準並通知 |
| 告警行為 | `cooldown_minutes` | `15` 分鐘 | 恢復或重定基準後的冷卻時間，設為 `0` 關閉 |

## 內建 RPC 指令

- `latencyAlert:check`：立即觸發一次檢查
- `latencyAlert:test`：發送一次測試訊息
- `latencyAlert:status`：查看上次檢查結果、目前告警與高丟包狀態

## 本機測試

```bash
node tests/test_plugin.js
```

## License

[GPL-3.0](LICENSE)
