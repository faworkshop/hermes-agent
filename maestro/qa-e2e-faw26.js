const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  const results = [];
  
  // Collect console errors
  const consoleErrors = [];
  page.on('console', msg => {
    if (msg.type() === 'error') consoleErrors.push(msg.text());
  });

  try {
    // LOGIN FIRST before testing protected pages
    console.log('=== LOGGING IN ===');
    await page.goto('http://localhost:3000/login', { waitUntil: 'networkidle', timeout: 15000 });
    await page.fill('input[type="email"], input[name="email"]', 'tr001@yopmail.com');
    await page.fill('input[type="password"], input[name="password"]', 'Test1234');
    await page.click('button[type="submit"], button:has-text("Sign in"), button:has-text("Login"), button:has-text("Log in")');
    await page.waitForLoadState('networkidle', { timeout: 10000 });
    console.log('Post-login URL:', page.url());

    // T1: Search bar visible on /products (authenticated)
    console.log('\n--- T1: Testing search bar visibility on /products ---');
    await page.goto('http://localhost:3000/products', { waitUntil: 'networkidle', timeout: 15000 });
    const searchBar = await page.$('input[placeholder*="Search"], input[type="search"], input[aria-label*="Search"]');
    const isSearchBarVisible = searchBar ? await searchBar.isVisible() : false;
    console.log('T1:', isSearchBarVisible ? 'PASS' : 'FAIL', '- Search bar visible:', isSearchBarVisible);
    results.push({ test: 'T1 - Search bar visible on /products', pass: isSearchBarVisible });

    // T2: Products page loads content
    console.log('\n--- T2: Testing products page loads content ---');
    const pageContent = await page.content();
    console.log('T2 PASS: Products page loaded, content length =', pageContent.length);
    results.push({ test: 'T2 - Products page loads content', pass: pageContent.length > 1000 });

    // T7: Search param pre-fills search bar
    console.log('\n--- T7: Testing ?search=eco pre-fills search bar ---');
    await page.goto('http://localhost:3000/products?search=eco', { waitUntil: 'networkidle', timeout: 15000 });
    const searchInput = await page.$('input[placeholder*="Search"], input[type="search"]');
    if (searchInput) {
      const inputValue = await searchInput.inputValue();
      const t7Pass = inputValue === 'eco';
      console.log('T7:', t7Pass ? 'PASS' : 'FAIL', '- Input value:', JSON.stringify(inputValue));
      results.push({ test: 'T7 - ?search=eco pre-fills search bar', pass: t7Pass });
    } else {
      console.log('T7: FAIL - search input not found');
      results.push({ test: 'T7 - ?search=eco pre-fills search bar', pass: false });
    }

    // T3: API search - single word
    console.log('\n--- T3: Testing GET /api/products?search=eco ---');
    const res3 = await page.evaluate(async () => {
      const r = await fetch('http://localhost:8080/api/products?search=eco');
      return { status: r.status, data: await r.json() };
    });
    const t3Pass = res3.status === 200 && Array.isArray(res3.data.data);
    console.log('T3:', t3Pass ? 'PASS' : 'FAIL', '- Status:', res3.status, '- Is array:', Array.isArray(res3.data.data), '- Total:', res3.data.total);
    results.push({ test: 'T3 - GET /api/products?search=eco → 200 + array', pass: t3Pass });

    // T4: API search - multi-word
    console.log('\n--- T4: Testing GET /api/products?search=eco+fan (multi-word) ---');
    const res4 = await page.evaluate(async () => {
      const r = await fetch('http://localhost:8080/api/products?search=eco+fan');
      return { status: r.status, data: await r.json() };
    });
    const t4Pass = res4.status === 200 && Array.isArray(res4.data.data);
    console.log('T4:', t4Pass ? 'PASS' : 'FAIL', '- Status:', res4.status);
    results.push({ test: 'T4 - GET /api/products?search=eco+fan multi-word', pass: t4Pass });

    // T5: API search - non-existent query
    console.log('\n--- T5: Testing GET /api/products?search=xyznonexistent123 ---');
    const res5 = await page.evaluate(async () => {
      const r = await fetch('http://localhost:8080/api/products?search=xyznonexistent123');
      return { status: r.status, data: await r.json() };
    });
    const t5Pass = Array.isArray(res5.data.data) && res5.data.data.length === 0;
    console.log('T5:', t5Pass ? 'PASS' : 'FAIL', '- Empty array:', t5Pass, '- Total:', res5.data.total);
    results.push({ test: 'T5 - Non-existent search returns empty array', pass: t5Pass });

    // T6: API search - paginated
    console.log('\n--- T6: Testing paginated search ?search=eco&page=0&size=5 ---');
    const res6 = await page.evaluate(async () => {
      const r = await fetch('http://localhost:8080/api/products?search=eco&page=0&size=5');
      return { status: r.status, data: await r.json() };
    });
    const t6Pass = res6.status === 200 && Array.isArray(res6.data.data) && res6.data.data.length <= 5;
    console.log('T6:', t6Pass ? 'PASS' : 'FAIL', '- Items:', res6.data.data?.length);
    results.push({ test: 'T6 - Paginated search returns ≤5 items', pass: t6Pass });

    // T8: API response structure - has id, name, brand
    console.log('\n--- T8: Testing API response structure (id, name, brand) ---');
    const res8 = await page.evaluate(async () => {
      const r = await fetch('http://localhost:8080/api/products?search=eco');
      const json = await r.json();
      return json.data[0] || null;
    });
    const t8Pass = res8 && res8.id && res8.name !== undefined && res8.brand !== undefined;
    console.log('T8:', t8Pass ? 'PASS' : 'FAIL', '- Fields:', res8 ? Object.keys(res8).slice(0,5) : 'null');
    results.push({ test: 'T8 - API response has id, name, brand', pass: !!t8Pass });

    // T9: Backend API accessible
    console.log('\n--- T9: Testing backend API accessibility ---');
    const res9 = await page.evaluate(async () => {
      try {
        const r = await fetch('http://localhost:8080/api/products');
        return r.status === 200;
      } catch { return false; }
    });
    console.log('T9:', res9 ? 'PASS' : 'FAIL');
    results.push({ test: 'T9 - Backend API accessible', pass: res9 });

  } catch (err) {
    console.error('TEST ERROR:', err.message);
    results.push({ test: 'Test execution error', pass: false, error: err.message });
  }

  // Console errors check
  console.log('\n--- Console Error Check ---');
  const noConsoleErrors = consoleErrors.length === 0;
  console.log('Console errors:', noConsoleErrors ? 'NONE (PASS)' : consoleErrors.slice(0,5).join('; '));
  results.push({ test: 'Console errors', pass: noConsoleErrors, errors: consoleErrors });

  // Summary
  console.log('\n========== SUMMARY ==========');
  const passed = results.filter(r => r.pass).length;
  const failed = results.filter(r => !r.pass).length;
  results.forEach(r => console.log((r.pass ? '✓' : '✗'), r.test));
  console.log(`\nTotal: ${passed} passed, ${failed} failed`);

  await browser.close();
  process.exit(failed > 0 ? 1 : 0);
})();
