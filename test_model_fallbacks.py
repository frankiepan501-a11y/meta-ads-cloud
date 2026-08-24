import unittest

from model_fallbacks import s1_fallback, s2_fallback, s2_threat_level, s4_fallback


class ModelFallbackTests(unittest.TestCase):
    def test_s1_fallback_keeps_required_report_sections(self):
        text = s1_fallback("model_disabled")
        self.assertIn("===整体表现总结===", text)
        self.assertIn("===下周行动项===", text)
        self.assertIn("零模型模板", text)

    def test_s2_fallback_keeps_required_report_sections(self):
        text = s2_fallback("model_disabled")
        self.assertIn("===整体策略===", text)
        self.assertIn("===行动建议===", text)
        self.assertIn("零模型模板", text)

    def test_s2_template_does_not_invent_a_low_threat_rating(self):
        self.assertIsNone(s2_threat_level("待模型恢复或人工核对后填写。", "template"))

    def test_s2_model_result_maps_threat_rating(self):
        self.assertEqual(s2_threat_level("威胁等级：高", "deepseek"), "高")
        self.assertEqual(s2_threat_level("威胁等级：中", "deepseek"), "中")
        self.assertEqual(s2_threat_level("未发现明显风险", "deepseek"), "低")

    def test_s4_fallback_is_actionable_and_never_claims_ai_analysis(self):
        text = s4_fallback("Controller", "hall sticks", "model_disabled")
        self.assertIn("Controller", text)
        self.assertIn("hall sticks", text)
        self.assertIn("零模型模板", text)
        self.assertNotIn("AI原创", text)


if __name__ == "__main__":
    unittest.main()
