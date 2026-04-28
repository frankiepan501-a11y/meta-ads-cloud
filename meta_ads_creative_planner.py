"""Unified Creative Planner — generates S3+S4 suggestions in one shot.
Input: product from control console
Output: Feishu doc with image+video plans per angle → auto-create S3 and S4 task records
"""
import json, urllib.request, sys, time, re, datetime, uuid, os, io
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

from meta_ads_s4_vsl_generator import (
    feishu, deepseek, gt, get_token, log,
)
from cloud_config import (
    PROXY, IM_APP_ID, IM_APP_SECRET, BOSS_OPEN_ID,
    WIKI_SPACE_ID, META_ADS_CONSOLE_APP,
)

CONSOLE_APP = META_ADS_CONSOLE_APP
CONSOLE_TABLE = 'tblO0W2CpqSL8dse'
S2_WEEKLY_TABLE = 'tbl8DPF9Z1jqdSpF'
WIKI_PARENT = os.environ.get('WIKI_PARENT_NODE_S4', 'MDQewVLuAiDyPmkBmf3cHz45nGh')
LOG_FILE = os.environ.get('PLANNER_LOG_FILE', '/tmp/planner_log.txt')

def plog(msg):
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f'[{ts}] {msg}'
    print(line)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + '\n')

# ============================================================
# Collect S1/S2 context
# ============================================================
def collect_context():
    """Gather S2 competitor insights for creative planning."""
    insights = []
    r = feishu('POST', f'/bitable/v1/apps/{CONSOLE_APP}/tables/{S2_WEEKLY_TABLE}/records/search?page_size=5',
        {'field_names': ['竞品名称', '策略洞察', '值得借鉴的角度']})
    for item in r.get('data', {}).get('items', []):
        fd = item['fields']
        comp = gt(fd.get('竞品名称', ''))
        strategy = gt(fd.get('策略洞察', ''))[:300]
        borrow = gt(fd.get('值得借鉴的角度', ''))[:300]
        if comp:
            insights.append(f'[{comp}] 策略:{strategy}\n借鉴:{borrow}')
    return insights

# ============================================================
# Generate unified creative plan
# ============================================================
def generate_plan(product, features, s2_insights):
    """Call DeepSeek to generate unified S3+S4 creative plan."""

    kb_path = os.environ.get('META_ADS_KB_PATH', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'meta_ads_knowledge.md'))
    with open(kb_path, 'r', encoding='utf-8') as f:
        knowledge = f.read()

    system_prompt = f"""{knowledge}

---

## 你的任务
你是META广告创意策划总监。基于仙女座算法原则，为产品同时策划图片广告(S3)和视频广告(S4)方案。

核心原则（来自知识库）：
1. 图片和视频必须同时投放在同一个Ad Set中，让算法自动测试
2. 每周需验证"视频vs图片哪个表现更好"
3. 素材需覆盖三层漏斗（TOFU/MOFU/BOFU）
4. 每个角度同时有图片版和视频版"""

    prompt = f"""===== 产品信息 =====
产品：{product}
核心卖点：{features}
品牌：Powkong
目标市场：美国
投放平台：Facebook / Instagram

===== 竞品洞察（S2系统）=====
{chr(10).join(s2_insights[:3])}

===== 任务 =====
请生成 **12 个创意角度**（参 Meta SMB 实验：3-10 creatives 比 1 个 CPA 低 46% / 99.99% 置信度，Andromeda 时代官方推荐 10-20 个独特创意/campaign），每个角度同时包含图片版和视频版方案。

严格按以下JSON格式输出（只输出JSON，不要其他文字）：

```json
[
  {{
    "angle_name": "角度名称",
    "target_audience": "目标受众",
    "funnel_level": "TOFU/MOFU/BOFU",
    "visual_type": "UGC/产品Demo/客户证言/创作者合拍/Lifestyle场景/对比测试/开箱/教学讲解",
    "strategy_reason": "为什么这个角度有效（参考竞品哪些策略）+ 跟其他 11 个角度的视觉/文案/格式三维差异点",

    "s3_image": {{
      "scene_style": "电竞桌面/简约白底/礼品场景/生活方式/户外运动/自定义",
      "image_style": "专业产品摄影/社交媒体风/电商详情页/节日促销风",
      "aspect_ratio": "1:1/4:5/9:16",
      "add_text": true,
      "ad_copy": "图片上的英文广告标题",
      "cta_button": "Shop Now/Buy Now/Learn More/Get Offer/不添加",
      "scene_description": "具体的图片场景描述"
    }},

    "s4_video": {{
      "framework": "经典痛点型/效果前置型/对比碾压型/好奇悬念型/社交证明型/真实体验型/开箱种草型/纯产品展示型",
      "duration": "15秒/30秒/60秒",
      "hook_en": "前3秒英文口播",
      "pain_en": "痛点英文口播",
      "solution_en": "卖点英文口播",
      "trust_en": "信任英文口播",
      "cta_en": "结尾英文CTA",
      "hook_scene": "停的画面描述",
      "pain_scene": "病的画面描述",
      "solution_scene": "药的画面描述"
    }}
  }}
]
```

强制要求（按 Meta 官方 Creative Diversification + Andromeda 创意分组机制）：
1. **12 个角度，每层至少 4 个**：TOFU 4+ / MOFU 4+ / BOFU 4+
2. **三维必须差异化**（视觉 + 营销文案 + 格式），不要同视觉风格只换文字 — 算法会合并成"同一创意"，学习不重启
3. `visual_type` 字段在 12 个角度里**至少覆盖 6 种不同类型**（UGC / 产品Demo / 客户证言 / 创作者合拍 / Lifestyle场景 / 对比测试 / 开箱 / 教学讲解）
4. 图片+视频成对（同一 angle_name 下 s3_image 和 s4_video 都要有），让算法在同一 Ad Set 自动分配预算
5. 至少 6 个图片带文案+CTA，6 个纯产品图（覆盖完整漏斗）
6. 参考竞品有效策略做差异化，**禁止**抄袭原文案"""

    result = deepseek(system_prompt, prompt)

    # Parse JSON
    json_match = re.search(r'\[[\s\S]*\]', result)
    if json_match:
        return json.loads(json_match.group())
    return []

# ============================================================
# Build result document
# ============================================================
def build_plan_markdown(product, angles, now):
    """Build markdown content for creative plan document."""
    date_str = now.strftime('%Y-%m-%d')
    md = []
    md.append(f'# ({date_str})META广告素材创意建议-{product}')
    md.append(f'\n产品: {product} | 生成时间: {now.strftime("%Y-%m-%d %H:%M")} | 角度: {len(angles)}个')
    md.append('\n每个角度同时有图片版(S3)和视频版(S4)，投放时放在同一个Ad Set中让算法自动优化。')
    md.append('\n---')

    for i, angle in enumerate(angles):
        md.append(f'\n## 角度 #{i+1}: {angle.get("angle_name","")}')
        md.append(f'\n**目标受众:** {angle.get("target_audience", "")}')
        md.append(f'**漏斗层级:** {angle.get("funnel_level", "")}')
        md.append(f'**策略依据:** {angle.get("strategy_reason", "")}')

        # S3 Image plan
        s3 = angle.get('s3_image', {})
        md.append(f'\n### 📷 S3 图片版')
        md.append(f'- 场景风格: {s3.get("scene_style","")} | 图片风格: {s3.get("image_style","")} | 比例: {s3.get("aspect_ratio","")}')
        if s3.get('add_text'):
            md.append(f'- 广告文案: {s3.get("ad_copy","")}')
            md.append(f'- CTA按钮: {s3.get("cta_button","")}')
        else:
            md.append('- 仅产品图（无文字）')
        md.append(f'- 场景描述: {s3.get("scene_description","")}')

        # S4 Video plan
        s4 = angle.get('s4_video', {})
        md.append(f'\n### 🎬 S4 视频版')
        md.append(f'- 结构公式: {s4.get("framework","")} | 时长: {s4.get("duration","")}')

        for label, field in [('停(Hook)', 'hook'), ('病(Pain)', 'pain'),
                             ('药(Solution)', 'solution'), ('信(Trust)', 'trust'), ('买(CTA)', 'cta')]:
            text_en = s4.get(f'{field}_en', '')
            scene = s4.get(f'{field}_scene', '')
            if text_en:
                md.append(f'- **{label}:** "{text_en}"')
            if scene:
                md.append(f'  - 画面: {scene}')

        md.append('\n---')

    return '\n'.join(md)

# ============================================================
# Create S3 and S4 task records in control console
# ============================================================
def create_subtasks(product, features, angles, source_record_id, now):
    """Create individual S3 and S4 task records for each approved angle."""
    created = 0

    for i, angle in enumerate(angles):
        s3 = angle.get('s3_image', {})
        s4 = angle.get('s4_video', {})
        angle_name = angle.get('angle_name', f'角度{i+1}')

        # Create S3 task
        s3_fields = {
            '产品名称': f'{product} - {angle_name}(图片)',
            '核心卖点': features,
            '生成类型': '图片素材(S3)',
            '目标平台': 'FB广告',
            '目标语种': '英语',
            '品牌': 'Powkong',
            '优先级': '🟡普通',
            '任务状态': '待审核',
            '场景风格': s3.get('scene_style', '简约白底'),
            '图片风格': s3.get('image_style', '专业产品摄影'),
            '图片比例': f'{s3.get("aspect_ratio","1:1")} (Facebook正方形)' if '1:1' in s3.get('aspect_ratio','') else s3.get('aspect_ratio','1:1'),
            '图片分辨率': '2k',
            '生成数量': '3张',
            '是否添加文字': '添加文案+CTA' if s3.get('add_text') else '仅产品图(无文字)',
            '备注': f'来自创意策划角度#{i+1} [{angle.get("funnel_level","")}]',
        }
        if s3.get('add_text') and s3.get('ad_copy'):
            s3_fields['广告文案'] = s3['ad_copy']
        if s3.get('cta_button') and s3.get('cta_button') != '不添加':
            s3_fields['CTA按钮'] = s3['cta_button']
        if s3.get('scene_description'):
            s3_fields['自定义场景描述'] = s3['scene_description']

        r = feishu('POST', f'/bitable/v1/apps/{CONSOLE_APP}/tables/{CONSOLE_TABLE}/records',
                   {'fields': s3_fields}, app='im')
        if r.get('code') == 0:
            created += 1

        # Create S4 task
        s4_fields = {
            '产品名称': f'{product} - {angle_name}(视频)',
            '核心卖点': features,
            '生成类型': '视频脚本(S4)',
            '目标平台': 'FB广告',
            '目标语种': '英语',
            '品牌': 'Powkong',
            '优先级': '🟡普通',
            '任务状态': '待审核',
            '备注': f'来自创意策划角度#{i+1} [{angle.get("funnel_level","")}] 框架:{s4.get("framework","")}',
        }

        r2 = feishu('POST', f'/bitable/v1/apps/{CONSOLE_APP}/tables/{CONSOLE_TABLE}/records',
                    {'fields': s4_fields}, app='im')
        if r2.get('code') == 0:
            created += 1

    return created

# ============================================================
# Import doc to wiki
# ============================================================
def import_markdown_to_wiki(md_content, title):
    """Upload markdown → import as docx → move to wiki. Returns URL."""
    import urllib.request as urlreq

    token = get_token(IM_APP_ID, IM_APP_SECRET)
    md_bytes = md_content.encode('utf-8')

    # Upload markdown
    boundary = uuid.uuid4().hex
    extra = json.dumps({'obj_type': 'docx', 'file_extension': 'md'})
    parts = []
    for n, v in [('file_name', f'{title}.md'), ('parent_type', 'ccm_import_open'),
                 ('size', str(len(md_bytes))), ('extra', extra)]:
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{n}"\r\n\r\n{v}\r\n'.encode())
    parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{title}.md"\r\nContent-Type: text/markdown\r\n\r\n'.encode())
    parts.append(md_bytes)
    parts.append(f'\r\n--{boundary}--\r\n'.encode())

    req = urlreq.Request('https://open.feishu.cn/open-apis/drive/v1/medias/upload_all',
        data=b''.join(parts),
        headers={'Content-Type': f'multipart/form-data; boundary={boundary}', 'Authorization': f'Bearer {token}'})
    try:
        resp = json.loads(urlreq.build_opener(PROXY).open(req, timeout=60).read())
        ft = resp['data']['file_token']
    except Exception as e:
        plog(f'  MD upload failed: {e}')
        return None

    # Import
    r = feishu('POST', '/drive/v1/import_tasks', {
        'file_extension': 'md', 'file_token': ft,
        'type': 'docx', 'point': {'mount_type': 1, 'mount_key': ''},
    })
    if r.get('code') != 0:
        plog(f'  Import failed: {r}')
        return None
    ticket = r['data']['ticket']

    doc_token = None
    for _ in range(15):
        time.sleep(2)
        r2 = feishu('GET', f'/drive/v1/import_tasks/{ticket}')
        if r2.get('data', {}).get('result', {}).get('job_status') == 0:
            doc_token = r2['data']['result']['token']
            break
    if not doc_token:
        return None

    # Grant permission
    feishu('POST', f'/drive/v1/permissions/{doc_token}/members?type=docx&need_notification=false',
           {'member_type': 'openid', 'member_id': BOSS_OPEN_ID, 'perm': 'full_access'})

    # Move to wiki
    r3 = feishu('POST', f'/wiki/v2/spaces/{WIKI_SPACE_ID}/nodes/move_docs_to_wiki', {
        'parent_wiki_token': WIKI_PARENT,
        'obj_type': 'docx', 'obj_token': doc_token,
    })
    if r3.get('code') != 0:
        return f'https://u1wpma3xuhr.feishu.cn/docx/{doc_token}'
    wt = r3.get('data', {}).get('wiki_token')
    if not wt:
        for _ in range(10):
            time.sleep(2)
            rn = feishu('GET', f'/wiki/v2/spaces/get_node?obj_type=docx&token={doc_token}')
            wt = rn.get('data', {}).get('node', {}).get('node_token')
            if wt:
                break
    return f'https://u1wpma3xuhr.feishu.cn/wiki/{wt}' if wt else f'https://u1wpma3xuhr.feishu.cn/docx/{doc_token}'

# ============================================================
# Main entry (called from console_poll)
# ============================================================
def run_creative_planning(record_id, fields):
    """Run unified creative planning for a console record."""
    product = gt(fields.get('产品名称', ''))
    features = gt(fields.get('核心卖点', ''))
    now = datetime.datetime.now()

    plog(f'Creative Planning: {product}')

    # Step 1: Collect context
    plog('  Collecting S2 data...')
    s2_insights = collect_context()
    plog(f'  S2 insights: {len(s2_insights)}')

    # Step 2: Generate plan
    plog('  Generating creative plan...')
    try:
        angles = generate_plan(product, features, s2_insights)
        plog(f'  Generated {len(angles)} angles')
    except Exception as e:
        plog(f'  Generation failed: {e}')
        return None, 0, str(e)

    if not angles:
        return None, 0, 'No angles generated'

    # Step 3: Build markdown document
    plog('  Building markdown...')
    date_str = now.strftime('%Y-%m-%d')
    title = f'({date_str})META广告素材创意建议-{product}'
    md_content = build_plan_markdown(product, angles, now)
    plog(f'  Markdown: {len(md_content)} chars')

    # Step 4: Import to wiki
    plog('  Importing to Feishu...')
    wiki_url = import_markdown_to_wiki(md_content, title)
    plog(f'  Doc: {wiki_url}')

    # Step 5: Fill angle names back to console (no subtask creation yet)
    plog('  Filling angle names to console...')
    update_fields = {}
    for i, angle in enumerate(angles[:6]):
        update_fields[f'角度{i+1}名称'] = angle.get('angle_name', '')[:50]
        update_fields[f'角度{i+1}审批'] = '待审核'       # video review
        update_fields[f'角度{i+1}图片审批'] = '待审核'    # image review

    feishu('PUT', f'/bitable/v1/apps/{CONSOLE_APP}/tables/{CONSOLE_TABLE}/records/{record_id}',
           {'fields': update_fields}, app='im')
    plog(f'  Filled {len(angles)} angle names')

    # Store angles JSON for later use when operator confirms
    # Save to the record's 备注 field as compressed reference
    angles_summary = json.dumps(angles, ensure_ascii=False)
    # Save to a local file for retrieval during 入库 phase
    angles_file = f'/tmp/plan_angles_{record_id}.json'
    with open(angles_file, 'w', encoding='utf-8') as f:
        json.dump(angles, f, ensure_ascii=False)
    plog(f'  Angles saved to {angles_file}')

    return wiki_url, len(angles), None


S3_TASK_TABLE = 'tbl5wV4nSOxKoCtc'

def process_approved_images(record_id, fields, console_app=CONSOLE_APP):
    """When operator confirms, create S3 task records for approved image angles."""
    product = gt(fields.get('产品名称', ''))
    features = gt(fields.get('核心卖点', ''))

    # Load saved angles
    angles_file = f'/tmp/plan_angles_{record_id}.json'
    try:
        with open(angles_file, 'r', encoding='utf-8') as f:
            angles = json.load(f)
    except FileNotFoundError:
        plog(f'  No angles file for {record_id}')
        return 0

    created = 0
    for i, angle in enumerate(angles[:6]):
        # Check image approval
        img_status = gt(fields.get(f'角度{i+1}图片审批', ''))
        if '通过' not in img_status or '不通过' in img_status:
            continue

        s3 = angle.get('s3_image', {})
        task_fields = {
            '产品名称': product,
            '创意角度': angle.get('angle_name', ''),
            '场景风格': s3.get('scene_style', '简约白底'),
            '图片风格': s3.get('image_style', '专业产品摄影'),
            '图片比例': s3.get('aspect_ratio', '1:1').split(' ')[0],
            '分辨率': '2k',
            '生成数量': '3',
            '是否添加文字': '添加文案+CTA' if s3.get('add_text') else '仅产品图',
            '广告文案': s3.get('ad_copy', '') if s3.get('add_text') else '',
            'CTA按钮': s3.get('cta_button', '不添加') if s3.get('add_text') else '不添加',
            '场景描述': s3.get('scene_description', ''),
            '漏斗层级': angle.get('funnel_level', ''),
            '任务状态': '待生成',
            '来源记录': record_id,
        }

        # Copy product image attachment from console to S3 task
        # (operator needs to upload to S3 task table separately)

        r = feishu('POST', f'/bitable/v1/apps/{console_app}/tables/{S3_TASK_TABLE}/records',
                   {'fields': task_fields}, app='im')
        if r.get('code') == 0:
            created += 1
            plog(f'    S3 task created: {angle.get("angle_name","")}')

    return created

if __name__ == '__main__':
    plog('Creative Planner - standalone test')
