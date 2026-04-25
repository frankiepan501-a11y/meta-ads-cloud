"""Facebook Ad Library scraper via Playwright.
Cloud-friendly: PLAYWRIGHT_PROXY env optional, no hardcoded local proxy.
v1: still used by S2 for competitor text/IDs (image embedding skipped on cloud).
v2: replace with Graph API /ads_archive once Ad Library API approval lands.
"""
import os, sys, json, time, re
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

from playwright.sync_api import sync_playwright

PLAYWRIGHT_PROXY = (os.environ.get('PLAYWRIGHT_PROXY') or '').strip() or None


def scrape_ad_library(brand_name, country='ALL', max_ads=20):
    """Scrape Facebook Ad Library for a brand's active ads."""
    url = (f'https://www.facebook.com/ads/library/?active_status=active&ad_type=all'
           f'&country={country}&q={brand_name}&search_type=keyword_exact_phrase')

    launch_kwargs = {
        'headless': True,
        'args': [
            '--no-sandbox',
            '--disable-blink-features=AutomationControlled',
            '--autoplay-policy=no-user-gesture-required',
        ],
    }
    if PLAYWRIGHT_PROXY:
        launch_kwargs['proxy'] = {'server': PLAYWRIGHT_PROXY}

    with sync_playwright() as p:
        browser = p.chromium.launch(**launch_kwargs)
        ctx = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            locale='zh-CN',
        )
        page = ctx.new_page()
        try:
            page.goto(url, wait_until='networkidle', timeout=45000)
        except Exception:
            pass
        time.sleep(5)

        for _ in range(min(max_ads // 5 + 1, 5)):
            page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            time.sleep(2)

        ads = page.evaluate("""(maxAds) => {
            const results = [];
            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, {
                acceptNode: (node) => node.textContent.includes('资料库编号')
                    ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT
            });
            const adNodes = [];
            while (walker.nextNode()) {
                let container = walker.currentNode.parentElement;
                for (let i = 0; i < 8; i++) {
                    if (container && container.parentElement) container = container.parentElement;
                }
                if (!adNodes.includes(container)) adNodes.push(container);
            }
            for (const container of adNodes.slice(0, maxAds)) {
                const text = container.innerText || '';
                const ad = {};
                const idMatch = text.match(/资料库编号[：:]\\s*(\\d+)/);
                ad.id = idMatch ? idMatch[1] : '';
                const dateMatch = text.match(/(\\d{4}年\\d{1,2}月\\d{1,2}日)开始投放/);
                ad.startDate = dateMatch ? dateMatch[1] : '';
                ad.platforms = [];
                if (text.includes('Facebook') || text.includes('facebook')) ad.platforms.push('Facebook');
                if (text.includes('Instagram') || text.includes('instagram')) ad.platforms.push('Instagram');
                const lines = text.split('\\n').map(l => l.trim()).filter(l => l);
                let advertiser = '';
                let bodyStart = -1;
                for (let i = 0; i < lines.length; i++) {
                    if (lines[i] === '赞助内容' || lines[i] === 'Sponsored') {
                        for (let j = i - 1; j >= Math.max(i - 5, 0); j--) {
                            const c = lines[j];
                            if (c && c !== '​' && !c.includes('查看') && !c.includes('打开') &&
                                !c.includes('平台') && !c.includes('资料库') && !c.includes('投放') &&
                                !c.includes('条广告') && c.length > 1 && c.length < 100) {
                                advertiser = c; break;
                            }
                        }
                        bodyStart = i + 1; break;
                    }
                }
                ad.pageName = advertiser;
                const ctaKeywords = ['Shop now','Shop Now','Learn more','Learn More','Install now',
                    'Sign up','Get offer','Contact us','Book now','Download','Apply now',
                    'Use app','Subscribe','Send message','去逛逛','了解详情','立即安装','发消息',
                    '查看广告详情','查看摘要详情','投放中','已停止'];
                ad.body = ''; ad.cta = '';
                if (bodyStart > 0) {
                    const bodyLines = [];
                    for (let i = bodyStart; i < Math.min(bodyStart + 15, lines.length); i++) {
                        const line = lines[i];
                        if (!line || line === '​') continue;
                        if (ctaKeywords.includes(line)) { ad.cta = line; continue; }
                        if (line.includes('资料库编号')) break;
                        bodyLines.push(line);
                    }
                    ad.body = bodyLines.join('\\n');
                }
                const containerImgs = container.querySelectorAll('img');
                ad.images = [];
                for (const img of containerImgs) {
                    if (img.src && img.src.includes('scontent') && img.naturalWidth > 80) {
                        ad.images.push(img.src);
                    }
                }
                const containerVids = container.querySelectorAll('video');
                ad.hasVideo = containerVids.length > 0;
                if (containerVids.length > 0) {
                    for (const vid of containerVids) {
                        if (vid.poster && vid.poster.includes('scontent')) ad.images.push(vid.poster);
                        const parent = vid.parentElement;
                        if (parent) {
                            const nearImgs = parent.querySelectorAll('img');
                            for (const img of nearImgs) {
                                if (img.src && img.src.includes('scontent') && !ad.images.includes(img.src)) {
                                    ad.images.push(img.src);
                                }
                            }
                        }
                    }
                }
                ad.mediaType = containerVids.length > 0 ? 'video' : (ad.images.length > 0 ? 'image' : 'unknown');
                if (ad.pageName || ad.body) results.push(ad);
            }
            return results;
        }""", max_ads)

        browser.close()

    return ads[:max_ads]


if __name__ == '__main__':
    brand = sys.argv[1] if len(sys.argv) > 1 else 'GameSir'
    country = sys.argv[2] if len(sys.argv) > 2 else 'ALL'
    max_ads = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    print(json.dumps(scrape_ad_library(brand, country, max_ads), ensure_ascii=False, indent=2))
