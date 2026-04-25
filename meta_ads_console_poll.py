"""META ADS 控制台轮询 — 检测多维表格触发指令.
1. 检测「任务状态=待生成」→ 调 S4/创意策划 → 回填控制台
2. 检测「确认入库=true + 任务状态=已生成/部分通过」→ 拆分入库 → 回填控制台
Triggered via FastAPI POST /run/console-poll (cron every 30 min by n8n).
"""
import json, urllib.request, sys, time, re, datetime, os, traceback, io, contextlib
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

from cloud_config import (
    PROXY, IM_APP_ID, IM_APP_SECRET, BT_APP_ID, BT_APP_SECRET,
    BOSS_OPEN_ID, META_ADS_CONSOLE_APP,
)

CONSOLE_APP = META_ADS_CONSOLE_APP
CONSOLE_TABLE = 'tblO0W2CpqSL8dse'
LOG_TABLE = 'tblk2ABjcQMnnxUh'
LOG_FILE = os.environ.get('CONSOLE_POLL_LOG_FILE', '/tmp/console_poll_log.txt')

def log(msg):
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f'[{ts}] {msg}'
    print(line)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + '\n')

_token_cache = {}
def get_token(app_id, secret):
    key = app_id
    if key in _token_cache and time.time() - _token_cache[key][1] < 600:
        return _token_cache[key][0]
    body = json.dumps({'app_id': app_id, 'app_secret': secret}).encode()
    req = urllib.request.Request('https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal',
        data=body, headers={'Content-Type': 'application/json'})
    token = json.loads(urllib.request.build_opener(PROXY).open(req, timeout=15).read())['tenant_access_token']
    _token_cache[key] = (token, time.time())
    return token

def feishu(method, path, body=None, app='im'):
    tok = get_token(BT_APP_ID, BT_APP_SECRET) if app == 'bt' else get_token(IM_APP_ID, IM_APP_SECRET)
    url = f'https://open.feishu.cn/open-apis{path}'
    data = json.dumps(body).encode('utf-8') if body else None
    req = urllib.request.Request(url, data=data,
        headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {tok}'}, method=method)
    try:
        return json.loads(urllib.request.build_opener(PROXY).open(req, timeout=30).read())
    except urllib.error.HTTPError as e:
        return {'code': e.code, 'body': e.read().decode('utf-8','replace')[:300]}

def gt(v):
    if not v: return ''
    if isinstance(v, str): return v
    if isinstance(v, list) and v and isinstance(v[0], dict): return v[0].get('text', '')
    return str(v)

def update_console(record_id, fields):
    """Update a console record. Try with im app, fallback to bt."""
    r = feishu('PUT', f'/bitable/v1/apps/{CONSOLE_APP}/tables/{CONSOLE_TABLE}/records/{record_id}',
               {'fields': fields}, app='im')
    if r.get('code') != 0:
        r = feishu('PUT', f'/bitable/v1/apps/{CONSOLE_APP}/tables/{CONSOLE_TABLE}/records/{record_id}',
                   {'fields': fields}, app='bt')
    return r

def write_log(system, status, report_url='', summary='', duration=0):
    """Write to system log table."""
    feishu('POST', f'/bitable/v1/apps/{CONSOLE_APP}/tables/{LOG_TABLE}/records', {
        'fields': {
            '系统': system,
            '执行时间': int(time.time() * 1000),
            '状态': status,
            '报告链接': {'link': report_url, 'text': report_url.split('/')[-1]} if report_url else None,
            '摘要': summary[:2000],
            '耗时(秒)': duration,
        }
    }, app='im')

# ============================================================
# Task 1: Detect "待生成" → Run S4
# ============================================================
def handle_pending_generation():
    """Find records with 任务状态=待生成, run S4 for each."""
    r = feishu('POST', f'/bitable/v1/apps/{CONSOLE_APP}/tables/{CONSOLE_TABLE}/records/search?page_size=10', {
        'filter': {'conjunction': 'and', 'conditions': [
            {'field_name': '任务状态', 'operator': 'is', 'value': ['待生成']}
        ]}
    }, app='im')

    items = r.get('data', {}).get('items', [])
    if not items:
        return 0

    log(f'Found {len(items)} pending generation tasks')

    for item in items:
        rid = item['record_id']
        fd = item['fields']
        product = gt(fd.get('产品名称', ''))
        features = gt(fd.get('核心卖点', ''))
        gen_type = gt(fd.get('生成类型', ''))

        if not product:
            log(f'  Skip {rid}: no product name')
            continue

        log(f'  Processing: {product} (type={gen_type})')

        # Route: Creative Planning (S3+S4 unified)
        if gen_type and '创意策划' in gen_type:
            update_console(rid, {'任务状态': '生成中'})
            start = time.time()
            try:
                from meta_ads_creative_planner import run_creative_planning
                wiki_url, created, error = run_creative_planning(rid, fd)
                duration = int(time.time() - start)

                if wiki_url:
                    update_fields = {
                        '任务状态': '已生成',
                        '生成时间': int(time.time() * 1000),
                        '入库记录数': created,
                        '备注': f'{created}条子任务已创建（{created//2}个角度×图片+视频）',
                    }
                    if wiki_url:
                        update_fields['广告素材建议文档'] = {'link': wiki_url, 'text': f'META广告素材创意建议-{product}'}
                    update_console(rid, update_fields)
                    write_log('S3图片素材', '✅成功', wiki_url, f'创意策划 {product}: {created}条子任务', duration)
                    log(f'  Creative Plan Done: {created} subtasks ({duration}s)')
                else:
                    update_console(rid, {'任务状态': '待生成', '备注': f'策划失败: {error}'})
                    write_log('S3图片素材', '❌失败', '', f'{product}: {error}', duration)
            except Exception as e:
                update_console(rid, {'任务状态': '待生成', '备注': f'异常: {str(e)[:200]}'})
                log(f'  Creative Plan Error: {e}')
            continue

        # Route to S3 or S4 based on generation type
        if gen_type and 'S3' in gen_type and 'S4' not in gen_type:
            # S3: Image generation via RunningHub
            update_console(rid, {'任务状态': '生成中'})
            start = time.time()
            try:
                from meta_ads_s3_image_generator import generate_ad_images, build_result_doc, import_docx_to_wiki, log as s3_log
                image_urls, error = generate_ad_images(rid, fd)
                duration = int(time.time() - start)

                if image_urls:
                    now = datetime.datetime.now()
                    docx_path = build_result_doc(product, image_urls, now)
                    wiki_url = import_docx_to_wiki(docx_path, f'({now.strftime("%Y-%m-%d")})广告图片-{product}')
                    try:
                        os.remove(docx_path)
                    except:
                        pass

                    update_fields = {
                        '任务状态': '已生成',
                        '生成时间': int(time.time() * 1000),
                        '入库记录数': len(image_urls),
                    }
                    if wiki_url:
                        update_fields['生成图片链接'] = {'link': wiki_url, 'text': f'广告图片-{product}'}
                    update_console(rid, update_fields)
                    write_log('S3图片素材', '✅成功', wiki_url or '', f'{product}: {len(image_urls)}张图片', duration)
                    log(f'  S3 Done: {len(image_urls)} images ({duration}s)')
                else:
                    update_console(rid, {'任务状态': '待生成', '备注': f'生成失败: {error}'})
                    write_log('S3图片素材', '❌失败', '', f'{product}: {error}', duration)
                    log(f'  S3 Failed: {error}')
            except Exception as e:
                update_console(rid, {'任务状态': '待生成', '备注': f'异常: {str(e)[:200]}'})
                log(f'  S3 Error: {e}')
            continue

        # Update status to 生成中
        update_console(rid, {'任务状态': '生成中'})

        # Run S4 in-process (capturing stdout for URL/angle extraction)
        start = time.time()
        captured = io.StringIO()
        try:
            from meta_ads_s4_vsl_generator import main as s4_main
            with contextlib.redirect_stdout(captured), contextlib.redirect_stderr(captured):
                s4_main(product=product, features=features)
            duration = int(time.time() - start)
            output = captured.getvalue()

            url_match = re.search(r'https://u1wpma3xuhr\.feishu\.cn/(?:wiki|docx)/\S+', output)
            wiki_url = url_match.group() if url_match else ''

            angle_matches = re.findall(r'Angle #(\d+): (.+)', output)

            if wiki_url:
                update_fields = {
                    '任务状态': '已生成',
                    '广告素材建议文档': {'link': wiki_url, 'text': f'VSL视频脚本-{product}'},
                    '生成时间': int(time.time() * 1000),
                }
                for num, name in angle_matches[:6]:
                    update_fields[f'角度{num}名称'] = name[:50]
                    update_fields[f'角度{num}审批'] = '待审核'

                update_console(rid, update_fields)
                write_log('S4视频脚本', '✅成功', wiki_url, f'{product}: {len(angle_matches)}个角度', duration)
                log(f'  Done: {wiki_url} ({duration}s)')
            else:
                tail = output[-200:]
                update_console(rid, {'任务状态': '待生成', '备注': f'生成失败: {tail}'})
                write_log('S4视频脚本', '❌失败', '', f'{product}: {tail[-100:]}', duration)
                log(f'  Failed: {tail}')

        except Exception as e:
            tb = traceback.format_exc()[-400:]
            update_console(rid, {'任务状态': '待生成', '备注': f'异常: {e}'})
            log(f'  Error: {e}\n{tb}')

    return len(items)

# ============================================================
# Task 2: Detect "确认入库=true" → Split into material libraries
# ============================================================
def handle_pending_storage():
    """Find records with 确认入库=true and status=已生成/部分通过, run split."""
    r = feishu('POST', f'/bitable/v1/apps/{CONSOLE_APP}/tables/{CONSOLE_TABLE}/records/search?page_size=10', {
        'filter': {'conjunction': 'and', 'conditions': [
            {'field_name': '确认入库', 'operator': 'is', 'value': ['true']},
        ]}
    }, app='im')

    items = r.get('data', {}).get('items', [])
    # Filter to only process items not yet stored
    pending = [i for i in items if gt(i['fields'].get('任务状态', '')) in ('已生成', '部分通过', '审核中')]

    if not pending:
        return 0

    log(f'Found {len(pending)} pending storage tasks')

    for item in pending:
        rid = item['record_id']
        fd = item['fields']
        product = gt(fd.get('产品名称', ''))
        gen_type = gt(fd.get('生成类型', ''))

        # For creative planning: handle both video and image approvals
        if '创意策划' in gen_type:
            log(f'  Creative Plan storage: {product}')

            # Count video approvals (角度N审批)
            video_approved = []
            for i in range(1, 7):
                status = gt(fd.get(f'角度{i}审批', ''))
                if '通过' in status and '不通过' not in status:
                    video_approved.append(i)

            # Count image approvals (角度N图片审批)
            image_approved = []
            for i in range(1, 7):
                status = gt(fd.get(f'角度{i}图片审批', ''))
                if '通过' in status and '不通过' not in status:
                    image_approved.append(i)

            total_tasks = 0

            # Handle approved images → create S3 task records
            if image_approved:
                try:
                    from meta_ads_creative_planner import process_approved_images
                    img_created = process_approved_images(rid, fd)
                    total_tasks += img_created
                    log(f'    S3 image tasks: {img_created}')
                except Exception as e:
                    log(f'    S3 image error: {e}')

            # Handle approved videos → S4 split to material library
            if video_approved:
                # Get document content for S4 video scripts
                doc_url = ''
                link_val = fd.get('广告素材建议文档', '')
                if isinstance(link_val, dict):
                    doc_url = link_val.get('link', '')
                elif isinstance(link_val, str):
                    doc_url = link_val

                if doc_url:
                    doc_match = re.search(r'/(?:wiki|docx)/(\w+)', doc_url)
                    if doc_match:
                        doc_id = doc_match.group(1)
                        try:
                            r3 = feishu('GET', f'/docx/v1/documents/{doc_id}/blocks?page_size=300', app='im')
                            blocks = r3.get('data', {}).get('items', [])
                            script_text = '\n'.join(
                                ''.join(e.get('text_run',{}).get('content','') for e in b.get(bk,{}).get('elements',[]))
                                for b in blocks
                                for bk in ['text','heading2','bullet']
                                if b.get('block_type') in (2,4,12)
                            )
                            if script_text:
                                sys.path.insert(0, 'C:/Users/Administrator/scripts')
                                from meta_ads_s4_vsl_generator import split_scripts_to_bitable
                                now = datetime.datetime.now()
                                vid_created = split_scripts_to_bitable(script_text, product, now)
                                total_tasks += vid_created
                                log(f'    S4 video tasks: {vid_created}')
                        except Exception as e:
                            log(f'    S4 video error: {e}')

            if total_tasks > 0 or (not video_approved and not image_approved):
                status_text = '已入库' if total_tasks > 0 else '已拒绝'
                update_console(rid, {
                    '任务状态': status_text,
                    '入库记录数': total_tasks,
                    '入库时间': int(time.time() * 1000),
                    '备注': f'视频{len(video_approved)}条 + 图片{len(image_approved)}条 = {total_tasks}条任务',
                })
                write_log('S4视频脚本', '✅成功', '', f'创意策划入库 {product}: {total_tasks}条', 0)
            else:
                update_console(rid, {'任务状态': '已拒绝', '备注': '无通过的角度'})
            continue

        # Regular S4: Count approved angles
        approved = []
        for i in range(1, 7):
            status = gt(fd.get(f'角度{i}审批', ''))
            if '通过' in status and '不通过' not in status:
                approved.append(i)

        if not approved:
            log(f'  Skip {rid}: no approved angles')
            update_console(rid, {'任务状态': '已拒绝', '备注': '无通过的角度'})
            continue

        log(f'  Storing {product}: {len(approved)} approved angles ({approved})')

        # Get the script document URL to retrieve content
        doc_url = ''
        link_val = fd.get('广告素材建议文档', '')
        if isinstance(link_val, dict):
            doc_url = link_val.get('link', '')
        elif isinstance(link_val, str):
            doc_url = link_val

        if not doc_url:
            log(f'  Skip {rid}: no document link')
            continue

        # Extract wiki token from URL
        wiki_match = re.search(r'/wiki/(\w+)', doc_url)
        if not wiki_match:
            log(f'  Skip {rid}: invalid URL')
            continue

        wiki_token = wiki_match.group(1)

        # Read the document content
        r2 = feishu('GET', f'/wiki/v2/spaces/get_node?token={wiki_token}', app='im')
        doc_id = r2.get('data', {}).get('node', {}).get('obj_token', '')
        if not doc_id:
            log(f'  Skip {rid}: cannot resolve doc')
            continue

        r3 = feishu('GET', f'/docx/v1/documents/{doc_id}/blocks?page_size=300', app='im')
        blocks = r3.get('data', {}).get('items', [])

        # Extract full text
        full_text = []
        for block in blocks:
            bt = block.get('block_type')
            def get_t(bk):
                return ''.join(e.get('text_run', {}).get('content', '') for e in block.get(bk, {}).get('elements', []))
            if bt == 2: full_text.append(get_t('text'))
            elif bt == 4: full_text.append(f'## {get_t("heading2")}')
            elif bt == 12: full_text.append(f'- {get_t("bullet")}')

        script_text = '\n'.join(full_text)

        # Import the split function from S4
        sys.path.insert(0, 'C:/Users/Administrator/scripts')
        from meta_ads_s4_vsl_generator import split_scripts_to_bitable

        start = time.time()
        now = datetime.datetime.now()
        total_records = split_scripts_to_bitable(script_text, product, now)
        duration = int(time.time() - start)

        # Update console
        update_console(rid, {
            '任务状态': '已入库',
            '入库记录数': total_records,
            '入库时间': int(time.time() * 1000),
        })
        write_log('S4视频脚本', '✅成功', doc_url,
                  f'{product}: {total_records}条素材入库 ({len(approved)}个角度)', duration)
        log(f'  Stored: {total_records} records ({duration}s)')

    return len(pending)

# ============================================================
# Task 3: Detect S3图片生成任务表 "待生成" → RunningHub
# ============================================================
S3_TASK_TABLE = 'tbl5wV4nSOxKoCtc'

def handle_s3_image_tasks():
    """Find S3 image tasks with 任务状态=待生成, run RunningHub generation."""
    r = feishu('POST', f'/bitable/v1/apps/{CONSOLE_APP}/tables/{S3_TASK_TABLE}/records/search?page_size=5', {
        'filter': {'conjunction': 'and', 'conditions': [
            {'field_name': '任务状态', 'operator': 'is', 'value': ['待生成']}
        ]}
    }, app='im')

    items = r.get('data', {}).get('items', [])
    if not items:
        return 0

    log(f'Found {len(items)} pending S3 image tasks')

    for item in items:
        rid = item['record_id']
        fd = item['fields']
        product = gt(fd.get('产品名称', ''))
        angle = gt(fd.get('创意角度', ''))

        # Check if product image attached
        attachments = fd.get('产品参考图', [])
        if not (isinstance(attachments, list) and attachments):
            log(f'  Skip {angle}: no product image uploaded')
            continue

        log(f'  S3 Image: {product} - {angle}')

        # Update status
        feishu('PUT', f'/bitable/v1/apps/{CONSOLE_APP}/tables/{S3_TASK_TABLE}/records/{rid}',
               {'fields': {'任务状态': '生成中'}}, app='im')

        start = time.time()
        try:
            from meta_ads_s3_image_generator import generate_ad_images, build_result_doc, import_docx_to_wiki
            image_urls, error = generate_ad_images(rid, fd)
            duration = int(time.time() - start)

            if image_urls:
                now = datetime.datetime.now()
                docx_path = build_result_doc(product, image_urls, now)
                wiki_url = import_docx_to_wiki(docx_path, f'({now.strftime("%Y-%m-%d")})广告图片-{product}-{angle}')
                try:
                    os.remove(docx_path)
                except:
                    pass

                update_fields = {'任务状态': '已完成'}
                if wiki_url:
                    update_fields['生成结果'] = {'link': wiki_url, 'text': f'{len(image_urls)}张图片'}
                feishu('PUT', f'/bitable/v1/apps/{CONSOLE_APP}/tables/{S3_TASK_TABLE}/records/{rid}',
                       {'fields': update_fields}, app='im')
                write_log('S3图片素材', '✅成功', wiki_url or '', f'{product}-{angle}: {len(image_urls)}张', duration)
                log(f'  Done: {len(image_urls)} images ({duration}s)')
            else:
                feishu('PUT', f'/bitable/v1/apps/{CONSOLE_APP}/tables/{S3_TASK_TABLE}/records/{rid}',
                       {'fields': {'任务状态': '失败'}}, app='im')
                write_log('S3图片素材', '❌失败', '', f'{product}-{angle}: {error}', duration)
                log(f'  Failed: {error}')
        except Exception as e:
            feishu('PUT', f'/bitable/v1/apps/{CONSOLE_APP}/tables/{S3_TASK_TABLE}/records/{rid}',
                   {'fields': {'任务状态': '失败'}}, app='im')
            log(f'  S3 Error: {e}')

    return len(items)

# ============================================================
# Main
# ============================================================
def main():
    log('--- Console Poll ---')
    n1 = handle_pending_generation()
    n2 = handle_pending_storage()
    # S3 image tasks are handled by n8n workflow (K6yoxCiItXQcsxb1), not here
    if n1 == 0 and n2 == 0:
        log('  No pending tasks')

if __name__ == '__main__':
    main()
