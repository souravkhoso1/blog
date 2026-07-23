---
layout: default
title: About
---
<div class="container">
  <div class="about-page">
    <h1>About me</h1>
    <p>
      Hey, I'm Sourav Khoso. I build software and occasionally write about what I learn along the way.
    </p>
    <p>
      This blog is a place for me to think out loud — about code, tools, ideas, and whatever else I find worth putting into words.
    </p>
    <p>
      You can reach me at <a href="mailto:souravkhoso1@gmail.com">souravkhoso1@gmail.com</a>.
    </p>

    <div class="post-body">
      <h2>What I'm building</h2>
      <p>
        I like building small, single-purpose web tools: no sign-up, no install, nothing to configure. Open the page and it does the one thing it promises. Here's what I'm working on.
      </p>

      <h3><a href="https://richtext.online" target="_blank" rel="noopener">RichText.online</a></h3>
      <p>
        A free, browser-based rich text editor for formatting text, drafting HTML snippets, or converting between Markdown and rich text without opening a full word processor.
      </p>
      <p>
        The editor runs entirely client-side. Your document is saved to <code>localStorage</code> as you type, so nothing is sent to a server and there's no account to lose access to.
      </p>

      <h4>Features</h4>
      <ul>
        <li>Full formatting toolbar — bold, italic, underline, strikethrough, headings, alignment, lists, indentation, sub/superscript</li>
        <li>Tables, blockquotes, code blocks, and horizontal rules</li>
        <li>Font family and size controls, plus foreground/background colour pickers</li>
        <li>Links, images (from a URL or uploaded), and embedded YouTube/Vimeo videos</li>
        <li>Find &amp; replace with step-through highlighting</li>
        <li>A Markdown mode with a live split-pane preview, and a raw HTML source view</li>
        <li>Document sharing via a compressed URL or a GitHub Gist, with output sanitised through DOMPurify</li>
        <li>Export to standalone HTML or a print-ready PDF view, plus dark mode and fullscreen</li>
        <li>Works offline once the page has loaded</li>
      </ul>

      <h4>Built with</h4>
      <div class="tag-list">
        <span class="tag">JavaScript</span>
        <span class="tag">HTML5</span>
        <span class="tag">CSS3</span>
        <span class="tag">Bootstrap 5</span>
        <span class="tag">DOMPurify</span>
        <span class="tag">LZ-String</span>
        <span class="tag">Marked.js</span>
      </div>

      <p>
        It's a deliberately simple stack — vanilla JavaScript, no build step, no framework — because the whole point is a page that loads fast and just works. Source: <a href="https://github.com/souravkhoso1/wysiwyg-editor" target="_blank" rel="noopener">souravkhoso1/wysiwyg-editor</a>.
      </p>

      <h3><a href="https://quickdataformat.online/" target="_blank" rel="noopener">QuickDataFormat.online</a></h3>
      <p>
        A tiny, static JSON / YAML / CSV formatter, validator, and converter. Paste in structured data and get it pretty-printed, validated, minified, or converted to a different format — all in the browser, with no backend.
      </p>

      <h4>Features</h4>
      <ul>
        <li><strong>Format</strong> — pretty-print JSON or YAML with 2/4-space or tab indentation</li>
        <li><strong>Validate</strong> — parse errors reported with line/column, for JSON, YAML, and CSV</li>
        <li><strong>Convert</strong> — between JSON, YAML, and CSV, flattening nested objects to dot-notation columns and rebuilding them on the way back</li>
        <li><strong>Minify</strong> — compact single-line JSON output</li>
        <li>Auto-detects the input format, supports drag-and-drop or file upload, and lets you download or copy the result</li>
        <li>Dark mode, and works fully offline once loaded</li>
      </ul>

      <h4>Built with</h4>
      <div class="tag-list">
        <span class="tag">JavaScript</span>
        <span class="tag">HTML5</span>
        <span class="tag">CSS3</span>
        <span class="tag">js-yaml</span>
        <span class="tag">PapaParse</span>
      </div>

      <p>
        Same philosophy as RichText.online: plain HTML/CSS/JS, no framework or bundler. The two parsing libraries are vendored directly in the repo rather than pulled from a CDN, so the site has no external runtime dependencies. Source: <a href="https://github.com/souravkhoso1/data-formatter" target="_blank" rel="noopener">souravkhoso1/data-formatter</a>.
      </p>
    </div>
  </div>
</div>
