// Optional real-browser identity regression. All HTTP traffic is local fixtures.
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');
let chromium;
try { ({chromium} = require(process.env.SPARK_PLAYWRIGHT_MODULE || 'playwright')); }
catch { console.log('SKIP: set SPARK_PLAYWRIGHT_MODULE to an installed Playwright package'); process.exit(77); }

(async () => {
  const browser = await chromium.launch({headless: true, executablePath: process.env.SPARK_CHROMIUM_EXECUTABLE});
  try {
    const page = await browser.newPage({viewport: {width: 1150, height: 900}});
    const assets = path.resolve(__dirname, '../web_demo/static');
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.route('**/*', async route => {
      const url = new URL(route.request().url());
      const name = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
      if (url.origin === 'http://kernel.test' && /^[a-z_]+\.(html|js|css)$/.test(name)) {
        try {
          await route.fulfill({body: await fs.readFile(path.join(assets, name)),
            contentType: name.endsWith('.js') ? 'text/javascript' : name.endsWith('.css') ? 'text/css' : 'text/html'});
          return;
        } catch {}
      }
      await route.fulfill({status: 200, json: {code: 0, data: {messages: []}}});
    });
    await page.goto('http://kernel.test/');
    assert.equal((await page.locator('#start-hermes-btn').innerText()).replace(/\s+/g, ' ').trim(),
      '✨ 与 Kernel Agent 对话（ID 900000000001）');
    assert.equal(await page.evaluate(() => agentName(900000000001)), 'Kernel Agent');
    assert.equal(await page.evaluate(() => agentName(900000000101)), 'Hermes · technical');
    await page.evaluate(() => {
      currentUserId = 7;
      showChatWindow(hermesBotUserId, 's_7_' + hermesBotUserId);
    });
    assert.equal(await page.locator('#chat-title').innerText(), '与 Kernel Agent 对话中');
    assert.equal(await page.evaluate(() => currentSessionId), 's_7_900000000001');
    assert.deepEqual(errors, []);
    if (process.env.KERNEL_AGENT_SCREENSHOT)
      await page.screenshot({path: process.env.KERNEL_AGENT_SCREENSHOT, fullPage: true});
    console.log('Kernel Agent browser identity: 6 checks passed');
  } finally { await browser.close(); }
})().catch(error => {console.error(error); process.exitCode = 1;});
