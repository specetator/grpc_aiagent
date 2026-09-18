/* ==========================================================================
   Spark Push Theme Manager
   - 主题：原版暖阳（默认，空值）/ kimi / claude / codex
   - localStorage 持久化，多标签页通过 storage 事件同步
   - 在 <head> 中同步加载，解析阶段即应用主题，避免闪烁
   ========================================================================== */
(function () {
  'use strict';

  var STORAGE_KEY = 'spark-theme';

  var THEMES = [
    { id: '',       name: '原版暖阳',       desc: 'Claude Minimal Warm（默认）', colors: ['#c96442', '#faf9f5', '#f4f3ec'] },
    { id: 'kimi',   name: 'Kimi 风格',      desc: '清爽蓝 · 简洁现代',           colors: ['#4d6bfe', '#f7f8fa', '#ffffff'] },
    { id: 'claude', name: 'Claude Desktop', desc: '暖阳陶土 · 深邃米棕',         colors: ['#d97757', '#f0eee6', '#e9e7dc'] },
    { id: 'codex',  name: 'Codex 风格',     desc: '极客暗夜 · 荧光绿终端',       colors: ['#10a37f', '#0e0e10', '#1a1a1e'] }
  ];

  function isValidTheme(id) {
    for (var i = 0; i < THEMES.length; i++) {
      if (THEMES[i].id === id) return true;
    }
    return false;
  }

  function getSavedTheme() {
    try {
      var saved = localStorage.getItem(STORAGE_KEY) || '';
      return isValidTheme(saved) ? saved : '';
    } catch (e) {
      return '';
    }
  }

  var currentTheme = getSavedTheme();

  function applyTheme(id, animate) {
    currentTheme = isValidTheme(id) ? id : '';
    var root = document.documentElement;
    if (animate) {
      root.classList.add('spark-theme-anim');
      setTimeout(function () {
        root.classList.remove('spark-theme-anim');
      }, 420);
    }
    if (currentTheme) {
      root.setAttribute('data-theme', currentTheme);
    } else {
      root.removeAttribute('data-theme');
    }
    syncMenuState();
  }

  function saveAndApply(id) {
    try {
      if (id) {
        localStorage.setItem(STORAGE_KEY, id);
      } else {
        localStorage.removeItem(STORAGE_KEY);
      }
    } catch (e) { /* 隐私模式等场景下静默降级 */ }
    applyTheme(id, true);
    if (typeof window.showToast === 'function') {
      var label = id === '' ? '原版暖阳' : id;
      for (var i = 0; i < THEMES.length; i++) {
        if (THEMES[i].id === id) label = THEMES[i].name;
      }
      window.showToast('已切换到主题：' + label);
    }
  }

  /* 解析阶段立即应用，防止首屏闪烁 */
  applyTheme(currentTheme, false);

  /* 跨标签页同步 */
  window.addEventListener('storage', function (e) {
    if (e.key === STORAGE_KEY) applyTheme(getSavedTheme(), true);
  });

  /* ---------------- 悬浮切换器 UI ---------------- */
  var fab = null;
  var menu = null;

  var PALETTE_ICON =
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" ' +
    'stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
    '<circle cx="12" cy="12" r="9"/>' +
    '<circle cx="8.2" cy="9.5" r="1.1" fill="currentColor" stroke="none"/>' +
    '<circle cx="12" cy="7.6" r="1.1" fill="currentColor" stroke="none"/>' +
    '<circle cx="15.8" cy="9.5" r="1.1" fill="currentColor" stroke="none"/>' +
    '<path d="M12 21c-4.97 0-9-3.8-9-8.5C3 7.4 7.03 3 12 3s9 3.58 9 8c0 2.49-1.79 3.5-3.5 3.5h-2.05a2.45 2.45 0 0 0-1.9 4c-.6 1.55-1.9 2.5-3.55 2.5z"/>' +
    '</svg>';

  function syncMenuState() {
    if (!menu) return;
    var options = menu.querySelectorAll('.spark-theme-option');
    for (var i = 0; i < options.length; i++) {
      var active = options[i].getAttribute('data-theme-id') === currentTheme;
      options[i].classList.toggle('active', active);
      options[i].setAttribute('aria-checked', active ? 'true' : 'false');
    }
  }

  function closeMenu() {
    if (menu) menu.classList.add('hidden');
  }

  function toggleMenu() {
    if (!menu) return;
    menu.classList.toggle('hidden');
  }

  function onDocumentClick(e) {
    if (!menu || menu.classList.contains('hidden')) return;
    if (menu.contains(e.target) || (fab && fab.contains(e.target))) return;
    closeMenu();
  }

  function onDocumentKeydown(e) {
    if (e.key === 'Escape') closeMenu();
  }

  function buildUI() {
    if (fab) return;

    fab = document.createElement('button');
    fab.type = 'button';
    fab.className = 'spark-theme-fab';
    fab.title = '切换主题';
    fab.setAttribute('aria-label', '切换主题');
    fab.setAttribute('aria-haspopup', 'menu');
    fab.innerHTML = PALETTE_ICON;

    menu = document.createElement('div');
    menu.className = 'spark-theme-menu hidden';
    menu.setAttribute('role', 'menu');
    menu.setAttribute('aria-label', '主题选择');

    var title = document.createElement('div');
    title.className = 'spark-theme-menu-title';
    title.textContent = '界面主题';
    menu.appendChild(title);

    THEMES.forEach(function (t) {
      var opt = document.createElement('button');
      opt.type = 'button';
      opt.className = 'spark-theme-option';
      opt.setAttribute('data-theme-id', t.id);
      opt.setAttribute('role', 'menuitemradio');

      var swatches = document.createElement('span');
      swatches.className = 'spark-theme-swatches';
      t.colors.forEach(function (c) {
        var dot = document.createElement('span');
        dot.style.background = c;
        swatches.appendChild(dot);
      });

      var meta = document.createElement('span');
      meta.className = 'spark-theme-option-meta';
      var name = document.createElement('div');
      name.className = 'spark-theme-option-name';
      name.textContent = t.name;
      var desc = document.createElement('div');
      desc.className = 'spark-theme-option-desc';
      desc.textContent = t.desc;
      meta.appendChild(name);
      meta.appendChild(desc);

      var check = document.createElement('span');
      check.className = 'spark-theme-check';
      check.textContent = '✓';

      opt.appendChild(swatches);
      opt.appendChild(meta);
      opt.appendChild(check);

      opt.addEventListener('click', function () {
        saveAndApply(t.id);
        closeMenu();
      });
      menu.appendChild(opt);
    });

    fab.addEventListener('click', function (e) {
      e.stopPropagation();
      toggleMenu();
    });
    document.addEventListener('click', onDocumentClick);
    document.addEventListener('keydown', onDocumentKeydown);

    document.body.appendChild(fab);
    document.body.appendChild(menu);
    syncMenuState();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', buildUI);
  } else {
    buildUI();
  }

  /* 暴露给页面脚本（如设置面板想复用） */
  window.SparkTheme = {
    list: THEMES.slice(),
    current: function () { return currentTheme; },
    set: saveAndApply
  };
})();
