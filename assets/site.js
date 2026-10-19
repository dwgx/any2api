(function () {
  var root = document.documentElement;
  var toggle = document.getElementById('themeToggle');
  var meta = document.querySelector('meta[name="theme-color"]');

  function applyTheme(theme) {
    root.dataset.theme = theme;
    if (meta) meta.setAttribute('content', theme === 'dark' ? '#151515' : '#f9f9f7');
    try { localStorage.setItem('any2api-theme', theme); } catch (e) {}
  }

  if (toggle) {
    toggle.addEventListener('click', function () {
      applyTheme(root.dataset.theme === 'dark' ? 'light' : 'dark');
    });
  }

  var snippets = {
    openai: 'POST /v1/chat/completions\n{\n  "model": "example-model",\n  "messages": [{"role": "user", "content": "Hello"}],\n  "stream": true\n}',
    anthropic: 'POST /v1/messages\n{\n  "model": "example-model",\n  "max_tokens": 256,\n  "messages": [{"role": "user", "content": "Hello"}],\n  "stream": true\n}',
    gemini: 'POST /v1beta/models/example-model:generateContent\n{\n  "contents": [\n    {"role": "user", "parts": [{"text": "Hello"}]}\n  ]\n}',
    responses: 'POST /v1/responses\n{\n  "model": "example-model",\n  "input": "Hello",\n  "stream": true\n}'
  };
  var code = document.getElementById('protocolCode');
  var tabs = Array.prototype.slice.call(document.querySelectorAll('.tab[data-protocol]'));

  tabs.forEach(function (tab) {
    tab.addEventListener('click', function () {
      var key = tab.getAttribute('data-protocol');
      if (code && snippets[key]) code.textContent = snippets[key];
      tabs.forEach(function (t) {
        var active = t === tab;
        t.classList.toggle('is-active', active);
        t.setAttribute('aria-selected', active ? 'true' : 'false');
      });
    });
  });

  Array.prototype.slice.call(document.querySelectorAll('.copy-btn[data-copy-target]')).forEach(function (button) {
    button.addEventListener('click', function () {
      var target = document.getElementById(button.getAttribute('data-copy-target'));
      if (!target) return;
      var text = target.textContent || '';
      var done = function () {
        var previous = button.textContent;
        button.textContent = root.lang.indexOf('zh') === 0 ? '已複製' : 'Copied';
        setTimeout(function () { button.textContent = previous; }, 1200);
      };
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(done, function () {});
      } else {
        var area = document.createElement('textarea');
        area.value = text;
        area.setAttribute('readonly', '');
        area.style.position = 'fixed';
        area.style.opacity = '0';
        document.body.appendChild(area);
        area.select();
        try { document.execCommand('copy'); done(); } catch (e) {}
        document.body.removeChild(area);
      }
    });
  });

  if (window.matchMedia) {
    var media = window.matchMedia('(prefers-color-scheme: dark)');
    var onChange = function (event) {
      var saved = null;
      try { saved = localStorage.getItem('any2api-theme'); } catch (e) {}
      if (!saved) applyTheme(event.matches ? 'dark' : 'light');
    };
    if (media.addEventListener) media.addEventListener('change', onChange);
    else if (media.addListener) media.addListener(onChange);
  }
})();
