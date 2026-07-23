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
        My main side project right now is <a href="https://richtext.online" target="_blank" rel="noopener">RichText.online</a> — a free, browser-based rich text editor. No sign-up, no install, nothing to configure: open the page and start writing. It's aimed at anyone who needs a quick WYSIWYG editor for formatting text, drafting HTML snippets, or converting between Markdown and rich text without opening a full word processor.
      </p>
      <p>
        The editor runs entirely client-side. Your document is saved to <code>localStorage</code> as you type, so nothing is sent to a server and there's no account to lose access to.
      </p>

      <h3>Features</h3>
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

      <h3>Built with</h3>
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
        It's a deliberately simple stack — vanilla JavaScript, no build step, no framework — because the whole point is a page that loads fast and just works. The source is on GitHub at <a href="https://github.com/souravkhoso1/wysiwyg-editor" target="_blank" rel="noopener">souravkhoso1/wysiwyg-editor</a>, and I keep iterating on it as I find gaps in the editing experience.
      </p>
    </div>
  </div>
</div>
