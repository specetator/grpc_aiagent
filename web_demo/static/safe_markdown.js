/* SparkSafeMarkdown 1.0.0 — local, dependency-free safe Markdown subset.
 * It never writes innerHTML. All output is constructed with DOM APIs and all
 * links pass an explicit scheme/shape allowlist. */
(function (global) {
  'use strict';
  const VERSION = '1.0.0';
  const CANN_URI = /^cannkb:\/\/document\/(doc_[0-9a-f]{24})\?chunk=(chk_[0-9a-f]{24})$/;

  function safeHref(raw) {
    const cann = String(raw || '').match(CANN_URI);
    if (cann) {
      return 'knowledge.html?doc_id=' + encodeURIComponent(cann[1]) +
        '&chunk_id=' + encodeURIComponent(cann[2]);
    }
    try {
      const url = new URL(String(raw || ''), window.location.href);
      if (url.protocol === 'http:' || url.protocol === 'https:') return url.href;
    } catch (_) {}
    return null;
  }

  function appendInline(parent, source) {
    const pattern = /(\[[^\]\n]+\]\([^\s)]+\)|`[^`\n]+`|\*\*[^*\n]+\*\*)/g;
    let cursor = 0;
    for (const match of String(source || '').matchAll(pattern)) {
      parent.appendChild(document.createTextNode(source.slice(cursor, match.index)));
      const token = match[0];
      if (token[0] === '[') {
        const split = token.lastIndexOf('](');
        const label = token.slice(1, split);
        const href = safeHref(token.slice(split + 2, -1));
        if (href) {
          const link = document.createElement('a');
          link.textContent = label;
          link.href = href;
          if (/^https?:/i.test(href)) {
            link.target = '_blank';
            link.rel = 'noopener noreferrer';
          }
          parent.appendChild(link);
        } else {
          parent.appendChild(document.createTextNode(label + ' [危险链接已移除]'));
        }
      } else if (token[0] === '`') {
        const code = document.createElement('code');
        code.textContent = token.slice(1, -1);
        parent.appendChild(code);
      } else {
        const strong = document.createElement('strong');
        strong.textContent = token.slice(2, -2);
        parent.appendChild(strong);
      }
      cursor = match.index + token.length;
    }
    parent.appendChild(document.createTextNode(source.slice(cursor)));
  }

  function render(target, markdown) {
    const fragment = document.createDocumentFragment();
    const lines = String(markdown || '').replace(/\r\n?/g, '\n').split('\n');
    let index = 0;
    while (index < lines.length) {
      const line = lines[index];
      if (!line.trim()) { index += 1; continue; }
      if (/^```/.test(line)) {
        const pre = document.createElement('pre');
        const code = document.createElement('code');
        const parts = [];
        index += 1;
        while (index < lines.length && !/^```/.test(lines[index])) parts.push(lines[index++]);
        if (index < lines.length) index += 1;
        code.textContent = parts.join('\n');
        pre.appendChild(code);
        fragment.appendChild(pre);
        continue;
      }
      const heading = line.match(/^(#{1,4})\s+(.+)$/);
      if (heading) {
        const node = document.createElement('h' + heading[1].length);
        appendInline(node, heading[2]);
        fragment.appendChild(node);
        index += 1;
        continue;
      }
      if (/^[-*]\s+/.test(line)) {
        const list = document.createElement('ul');
        while (index < lines.length && /^[-*]\s+/.test(lines[index])) {
          const item = document.createElement('li');
          appendInline(item, lines[index].replace(/^[-*]\s+/, ''));
          list.appendChild(item);
          index += 1;
        }
        fragment.appendChild(list);
        continue;
      }
      const paragraph = [];
      while (index < lines.length && lines[index].trim() &&
             !/^(#{1,4})\s+/.test(lines[index]) &&
             !/^[-*]\s+/.test(lines[index]) && !/^```/.test(lines[index])) {
        paragraph.push(lines[index++]);
      }
      const p = document.createElement('p');
      paragraph.forEach(function (part, partIndex) {
        if (partIndex) p.appendChild(document.createElement('br'));
        appendInline(p, part);
      });
      fragment.appendChild(p);
    }
    target.replaceChildren(fragment);
    target.dataset.safeMarkdownVersion = VERSION;
  }

  global.SparkSafeMarkdown = { version: VERSION, render: render, safeHref: safeHref };
})(window);
