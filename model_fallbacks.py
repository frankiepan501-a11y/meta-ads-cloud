"""Deterministic zero-model fallbacks used when Meta Ads AI is disabled."""


def s1_fallback(reason: str) -> str:
    note = f"零模型模板：模型未启用或被安全闸拦截（{reason}）。以下只保留确定性数据检查，不生成 AI 判断。"
    return f"""===整体表现总结===
{note}

===Campaign排行===
请按报表中的 Spend、ROAS、CTR 原始数据排序核对。

===问题诊断===
仅标记原始数据缺口；不自动推断原因。

===Winning Ads素材分析===
未启用模型分析，请由运营只查看报表中的 Top 素材原始指标。

===本周假设验证===
待模型恢复或人工填写。

===下周内容制作指引===
沿用已验证素材，不基于本模板新增未经验证的创意结论。

===下周行动项===
1. 核对数据完整性；2. 检查异常 Campaign；3. 模型恢复后单条回放。
"""


def s2_fallback(reason: str) -> str:
    note = f"零模型模板：模型未启用或被安全闸拦截（{reason}）。竞品原始广告仍保留，不生成 AI 推断。"
    return f"""===整体策略===
{note}

===周变化点评===
只展示新增、持续和停投数量，不解释竞品意图。

===威胁评估===
待模型恢复或人工核对后填写。

===行动建议===
1. 抽查新增广告；2. 保存高频素材证据；3. 模型恢复后单条回放。
"""


def s2_threat_level(analysis_text: str, model_mode: str):
    """Return a business threat level only when it came from a model result."""
    if model_mode != "deepseek":
        return None
    if "高" in analysis_text:
        return "高"
    if "中" in analysis_text:
        return "中"
    return "低"


def s4_fallback(product: str, features: str, reason: str) -> str:
    return f"""## 零模型模板（安全降级）

模型未启用或被安全闸拦截：`{reason}`。以下是确定性拍摄框架，不包含 AI 市场判断。

### 角度 1：产品功能演示
- 产品：{product}
- 已知卖点：{features}
- Hook：先展示使用前后的明确差异。
- Pain：只描述该卖点直接解决的问题，不扩写未知用户反馈。
- Solution：近景拍摄功能操作和结果。
- Trust：仅使用已有可验证参数或真实评价。
- CTA：引导查看产品详情，不虚构折扣。

### 角度 2：真实使用场景
- 产品：{product}
- 已知卖点：{features}
- Hook：从真实桌面或游戏场景开始。
- Solution：用连续镜头展示安装、使用和收纳。
- Trust：展示实物与真实操作过程。
- CTA：邀请用户查看兼容信息。

### 角度 3：操作教学
- 产品：{product}
- 已知卖点：{features}
- Hook：直接说明本视频会解决的操作问题。
- Solution：按步骤演示，每一步只呈现已验证功能。
- Trust：用完整操作过程作为证据。
- CTA：保存教程或打开产品页面。
"""
