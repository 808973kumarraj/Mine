/* @ds-bundle: {"format":4,"namespace":"SparkInternals","components":[{"name":"Episode"},{"name":"Frame"},{"name":"Header"},{"name":"Deck"},{"name":"DeckButton"},{"name":"Scrubber"},{"name":"SceneTitle"},{"name":"StippleText"},{"name":"Pill"},{"name":"Card"},{"name":"Stat"},{"name":"MetricRows"},{"name":"ProgressBar"},{"name":"StepList"},{"name":"TaskBars"},{"name":"DataGrid"},{"name":"Terminal"},{"name":"CodeCompare"},{"name":"Record"},{"name":"Machine"},{"name":"Slab"},{"name":"RackGrid"},{"name":"Disk"},{"name":"Hourglass"},{"name":"LED"},{"name":"BitRibbon"},{"name":"Pipe"},{"name":"ClosingCard"}]} */
/* Spark Internals — components and the canvas drawing kit, ported from "Spark Internals / EP. 07 — The Shuffle".
   Needs React 18 on the page (window.React, window.ReactDOM) and tokens.css for colors. */
(function () {
  'use strict';
  var R = window.React;
  var h = R.createElement, useRef = R.useRef, useEffect = R.useEffect, useState = R.useState, useMemo = R.useMemo;

  /* ---------- math and easing (the episode's own) ---------- */
  function clamp(v, a, b) { a = a == null ? 0 : a; b = b == null ? 1 : b; return Math.max(a, Math.min(b, v)); }
  function lerp(a, b, t) { return a + (b - a) * t; }
  function prog(t, a, b) { return clamp((t - a) / (b - a)); }
  function hash(i) { var s = Math.sin(i * 12.9898 + 78.233) * 43758.5453; return s - Math.floor(s); }
  var ease = {
    outCubic: function (t) { return 1 - Math.pow(1 - t, 3); },
    inCubic: function (t) { return t * t * t; },
    inOutCubic: function (t) { return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2; },
    outBack: function (t) { var c1 = 1.70158, c3 = c1 + 1; return 1 + c3 * Math.pow(t - 1, 3) + c1 * Math.pow(t - 1, 2); }
  };
  function pad2(n) { return String(n).padStart(2, '0'); }
  function fmt(s) { s = Math.max(0, Math.floor(s)); return pad2(Math.floor(s / 60)) + ':' + pad2(s % 60); }

  /* ---------- colors: read from tokens.css, fall back to the episode's values ---------- */
  var FALLBACK = {
    ink: '#1b1b1b', muted: '#726e64', blue: '#1c56d4', green: '#2c7a3b', red: '#b6362a', amber: '#d47926',
    cream: '#f2efe8', paper: '#faf8f3', node: '#ebe6da', white: '#ffffff',
    'key-1': '#1c56d4', 'key-2': '#2c7a3b', 'key-3': '#d47926', 'key-4': '#3d3a34',
    tile: '#efeae0', 'tile-shade': '#cfc8b9', slab: '#d9d3c5', platform: '#e2ddd1', fact: '#bdb6a7', block: '#e6e0d4',
    'tint-kept': '#dcefdf', 'tint-dropped': '#f2ddd6', 'tint-queued': '#e9c4bb',
    'hatch-grey': 'rgba(114,110,100,0.55)', 'hatch-white': 'rgba(255,255,255,0.38)',
    string: '#9fd3a8', 'led-soft': '#bff0c8'
  };
  function tokens(el) {
    var out = {}, cs = null;
    try { cs = window.getComputedStyle(el || document.documentElement); } catch (e) { cs = null; }
    for (var key in FALLBACK) {
      var v = cs ? cs.getPropertyValue('--' + key).trim() : '';
      out[key] = v || FALLBACK[key];
    }
    return out;
  }
  function parse(c) {
    c = String(c).trim();
    var m;
    if (c.charAt(0) === '#') {
      var x = c.slice(1);
      if (x.length <= 4) x = x.split('').map(function (ch) { return ch + ch; }).join('');
      return [parseInt(x.slice(0, 2), 16), parseInt(x.slice(2, 4), 16), parseInt(x.slice(4, 6), 16), x.length >= 8 ? parseInt(x.slice(6, 8), 16) / 255 : 1];
    }
    if ((m = /rgba?\(([^)]+)\)/.exec(c))) {
      var p = m[1].split(/[\s,\/]+/).filter(Boolean).map(parseFloat);
      return [p[0], p[1], p[2], p.length > 3 ? p[3] : 1];
    }
    return [0, 0, 0, 1];
  }
  function mix(a, b, t) { var A = parse(a), B = parse(b); return 'rgb(' + [0, 1, 2].map(function (i) { return Math.round(A[i] + (B[i] - A[i]) * t); }).join(',') + ')'; }
  function shade(c, t) { return t < 0 ? mix(c, '#000000', -t) : mix(c, '#ffffff', t); }
  function rgba(c, a) { var p = parse(c); return 'rgba(' + p[0] + ',' + p[1] + ',' + p[2] + ',' + a + ')'; }
  function lum(c) { var p = parse(c); return [0, 1, 2].reduce(function (s, i, j) { var v = p[i] / 255; v = v <= 0.04045 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); return s + v * [0.2126, 0.7152, 0.0722][j]; }, 0); }
  function contrast(a, b) { var x = lum(a), y = lum(b); return (Math.max(x, y) + 0.05) / (Math.min(x, y) + 0.05); }

  /* ---------- the drawing kit: every primitive the episode draws, bound to one 2D context ---------- */
  function kit(ctx, C) {
    C = C || tokens(ctx && ctx.canvas);
    var MONO = '"JetBrains Mono", ui-monospace, monospace', SERIFF = '"Newsreader", Georgia, serif';
    var C30 = 0.866, S30 = 0.5, PAT = null;
    function F(w, s) { return w + ' ' + s + 'px ' + MONO; }
    function SERIF(w, s, it) { return (it ? 'italic ' : '') + w + ' ' + s + 'px ' + SERIFF; }
    function textOn(bg) { return contrast(C.cream, bg) >= 4.5 ? C.cream : C.ink; }
    function labelOn(bg) { return contrast(C.white, bg) >= 4.5 ? C.white : C.ink; }
    function color(name) { return C[name] || name; }

    function poly(p) { ctx.beginPath(); ctx.moveTo(p[0][0], p[0][1]); for (var i = 1; i < p.length; i++) ctx.lineTo(p[i][0], p[i][1]); ctx.closePath(); }
    function rr(x, y, w, hh, r) { r = Math.min(r, hh / 2, w / 2); ctx.beginPath(); ctx.moveTo(x + r, y); ctx.arcTo(x + w, y, x + w, y + hh, r); ctx.arcTo(x + w, y + hh, x, y + hh, r); ctx.arcTo(x, y + hh, x, y, r); ctx.arcTo(x, y, x + w, y, r); ctx.closePath(); }
    function txt(s, x, y, font, col, align, alpha, base) { ctx.save(); ctx.globalAlpha *= (alpha == null ? 1 : alpha); ctx.font = font; ctx.fillStyle = col; ctx.textAlign = align || 'left'; ctx.textBaseline = base || 'alphabetic'; ctx.fillText(s, x, y); ctx.restore(); }
    function led(x, y, c, r, g) {
      r = r == null ? 1.7 : r; g = g == null ? 1 : g;
      if (g > 0) { var gr = ctx.createRadialGradient(x, y, 0, x, y, r * 4.5); gr.addColorStop(0, rgba(c, 0.55 * g)); gr.addColorStop(1, rgba(c, 0)); ctx.fillStyle = gr; ctx.beginPath(); ctx.arc(x, y, r * 4.5, 0, 7); ctx.fill(); }
      ctx.fillStyle = c; ctx.beginPath(); ctx.arc(x, y, r, 0, 7); ctx.fill();
      ctx.fillStyle = 'rgba(255,255,255,.6)'; ctx.beginPath(); ctx.arc(x - r * 0.35, y - r * 0.35, r * 0.35, 0, 7); ctx.fill();
    }
    function card(x, y, w, hh, o) {
      o = o || {}; ctx.save(); ctx.shadowColor = 'rgba(27,27,27,.18)'; ctx.shadowBlur = 14; ctx.shadowOffsetY = 5;
      rr(x, y, w, hh, o.r == null ? 8 : o.r); ctx.fillStyle = o.bg || C.paper; ctx.fill(); ctx.shadowColor = 'transparent';
      ctx.lineWidth = 1.2; ctx.strokeStyle = o.border || C.ink; ctx.stroke(); ctx.restore();
    }

    /* isometric diamond tile (30°) */
    function iso(gx, gy, gz, ox, oy, s) { return [ox + (gx - gy) * C30 * s, oy + (gx + gy) * S30 * s - gz * s]; }
    function isoTile(gx, gy, gz, w, d, hh, ox, oy, scale, o) {
      o = o || {};
      var P = function (x, y, z) { return iso(x, y, z, ox, oy, scale); }, zt = gz + hh, base = o.color || C.tile;
      ctx.save(); ctx.globalAlpha *= (o.alpha == null ? 1 : o.alpha); ctx.lineJoin = 'round';
      if (o.ghost > 0) {
        var gz2 = zt + o.ghost; ctx.save(); ctx.globalAlpha *= (o.ghostAlpha == null ? 0.6 : o.ghostAlpha);
        ctx.strokeStyle = o.ghostColor || C.blue; ctx.lineWidth = 0.7; ctx.setLineDash([2, 2]);
        poly([P(gx, gy, gz2), P(gx + w, gy, gz2), P(gx + w, gy + d, gz2), P(gx, gy + d, gz2)]); ctx.stroke();
        ctx.beginPath();
        [[gx, gy + d], [gx + w, gy + d], [gx + w, gy]].forEach(function (ab) { var p1 = P(ab[0], ab[1], gz2), p2 = P(ab[0], ab[1], gz2 - hh); ctx.moveTo(p1[0], p1[1]); ctx.lineTo(p2[0], p2[1]); });
        ctx.stroke(); ctx.restore();
      }
      var top = [P(gx, gy, zt), P(gx + w, gy, zt), P(gx + w, gy + d, zt), P(gx, gy + d, zt)];
      var left = [P(gx, gy + d, zt), P(gx + w, gy + d, zt), P(gx + w, gy + d, gz), P(gx, gy + d, gz)];
      var right = [P(gx + w, gy + d, zt), P(gx + w, gy, zt), P(gx + w, gy, gz), P(gx + w, gy + d, gz)];
      ctx.lineWidth = o.lw || 0.8; ctx.strokeStyle = o.stroke || C.ink;
      poly(left); ctx.fillStyle = o.left || shade(base, -0.16); ctx.fill(); ctx.stroke();
      poly(right); ctx.fillStyle = o.right || shade(base, -0.3); ctx.fill(); ctx.stroke();
      poly(top); ctx.fillStyle = o.top || base; ctx.fill(); ctx.stroke();
      if (o.vents) {
        ctx.strokeStyle = 'rgba(27,27,27,.45)'; ctx.lineWidth = 0.55; ctx.beginPath();
        for (var i = 0; i < 3; i++) { var u = 0.18 + i * 0.13, a = P(gx + w * u, gy + d, gz + hh * 0.25), b = P(gx + w * u, gy + d, gz + hh * 0.75); ctx.moveTo(a[0], a[1]); ctx.lineTo(b[0], b[1]); }
        ctx.stroke();
      }
      if (o.led) { var p = P(gx + w, gy + d * 0.45, gz + hh * 0.5); led(p[0], p[1], o.led, o.ledR || 1.5, o.glow == null ? 1 : o.glow); }
      ctx.restore();
    }

    /* oblique axonometric box: front face skews up 8.5% of its width, depth recedes up-left */
    function boxGeo(x, y, w, hh, depth) {
      var t = -w * 0.085, dx = -depth * 0.56, dy = -depth * 0.48;
      return { x: x, y: y, w: w, h: hh, depth: depth, t: t, dx: dx, dy: dy, topPt: function (u, v) { return [x + u * w + v * dx, y + u * t + v * dy]; } };
    }
    function box(x, y, w, hh, depth, o) {
      o = o || {};
      var g = boxGeo(x, y, w, hh, depth), t = g.t, dx = g.dx, dy = g.dy, base = o.color || C.node;
      var front = [[x, y], [x + w, y + t], [x + w, y + hh + t], [x, y + hh]];
      var top = [[x, y], [x + w, y + t], [x + w + dx, y + t + dy], [x + dx, y + dy]];
      var side = [[x, y], [x + dx, y + dy], [x + dx, y + hh + dy], [x, y + hh]];
      ctx.save(); ctx.globalAlpha *= (o.alpha == null ? 1 : o.alpha); ctx.lineJoin = 'round'; ctx.lineWidth = o.lw || 1.1; ctx.strokeStyle = o.stroke || C.ink;
      if (o.wire) { ctx.setLineDash(o.dash || [3, 3]); ctx.lineDashOffset = o.dashOffset || 0; [side, top, front].forEach(function (f) { poly(f); ctx.stroke(); }); ctx.restore(); return g; }
      if (o.shadow !== false) {
        ctx.save(); var sx = Math.max(3, depth * 0.2), sy = 3;
        ctx.fillStyle = 'rgba(27,27,27,' + (o.shadowA == null ? 0.16 : o.shadowA) + ')'; ctx.shadowColor = 'rgba(27,27,27,.3)'; ctx.shadowBlur = o.shadowBlur == null ? 8 : o.shadowBlur;
        poly([[x + sx, y + hh + sy], [x + w + sx, y + hh + t + sy], [x + w + sx + dx, y + hh + t + dy + sy], [x + sx + dx, y + hh + dy + sy]]); ctx.fill(); ctx.restore();
      }
      var P = o.pattern ? (typeof o.pattern === 'string' ? pat()[o.pattern] : o.pattern) : null;
      poly(side); ctx.fillStyle = o.sideColor || shade(base, -0.22); ctx.fill(); if (P) { ctx.fillStyle = P; ctx.fill(); } ctx.stroke();
      poly(top); ctx.fillStyle = o.topColor || shade(base, 0.25); ctx.fill(); if (P) { ctx.fillStyle = P; ctx.fill(); } ctx.stroke();
      poly(front); ctx.fillStyle = base; ctx.fill(); if (P) { ctx.fillStyle = P; ctx.fill(); } ctx.stroke();
      var fy = function (px) { return y + t * (px - x) / w; };
      if (o.dividers) { ctx.save(); ctx.strokeStyle = 'rgba(27,27,27,.4)'; ctx.lineWidth = 0.8; for (var i = 1; i <= o.dividers; i++) { var f = i / (o.dividers + 1); ctx.beginPath(); ctx.moveTo(x, y + hh * f); ctx.lineTo(x + w, y + hh * f + t); ctx.stroke(); } ctx.restore(); }
      if (o.vents) { ctx.save(); ctx.strokeStyle = 'rgba(27,27,27,.5)'; ctx.lineWidth = 0.9; ctx.beginPath(); for (var j = 0; j < o.vents; j++) { var px = x + 7 + j * 3.6, yb = fy(px) + hh - 5; ctx.moveTo(px, yb - 8); ctx.lineTo(px, yb); } ctx.stroke(); ctx.restore(); }
      if (o.leds) { o.leds.forEach(function (c, i) { var px = x + w - 9 - i * 7.5; led(px, fy(px) + 8, c, 1.7, o.glow == null ? 1 : o.glow); }); }
      if (o.label) { ctx.save(); ctx.translate(x, y); ctx.transform(1, t / w, 0, 1, 0, 0); ctx.font = o.labelFont || F(700, 8); ctx.fillStyle = o.labelColor || C.ink; ctx.fillText(o.label, o.labelX == null ? 7 : o.labelX, o.labelY == null ? 13 : o.labelY); ctx.restore(); }
      if (o.topLabel) { ctx.save(); ctx.translate(x + dx, y + dy); ctx.transform(1, t / w, -dx / depth, -dy / depth, 0, 0); ctx.font = o.topLabelFont || F(800, 7); ctx.fillStyle = o.topLabelColor || C.ink; ctx.textAlign = 'center'; ctx.textBaseline = 'middle'; ctx.fillText(o.topLabel, w / 2, depth / 2 + 0.5); ctx.restore(); }
      ctx.restore(); return g;
    }
    var SLAB = { w: 26, h: 5, d: 14 };
    function slab(x, y, col, o) { o = o || {}; return box(x, y - SLAB.h, SLAB.w, SLAB.h, SLAB.d, Object.assign({ color: col, shadowA: 0.12, shadowBlur: 4, lw: 0.8, topLabelColor: labelOn(col), topLabelFont: F(800, 6.5) }, o)); }

    /* diagonal hatch patterns */
    function hatch(col, step, width, bg) {
      step = step || 6; width = width || 1.2;
      var s = step * 2, c = document.createElement('canvas'); c.width = c.height = s; var g = c.getContext('2d');
      if (bg) { g.fillStyle = bg; g.fillRect(0, 0, s, s); }
      g.strokeStyle = col; g.lineWidth = width * 2; g.beginPath(); g.moveTo(-1, s + 1); g.lineTo(s + 1, -1); g.moveTo(-2, 2); g.lineTo(2, -2); g.moveTo(s - 2, s + 2); g.lineTo(s + 2, s - 2); g.stroke();
      var p = ctx.createPattern(c, 'repeat'); if (p && p.setTransform && window.DOMMatrix) p.setTransform(new DOMMatrix().scale(0.5)); return p;
    }
    function pat() {
      if (!PAT) PAT = { green: hatch(C.green, 5, 1.1, rgba(C.green, 0.22)), red: hatch(C.red, 4, 1.2, rgba(C.red, 0.3)), grey: hatch(C['hatch-grey'], 5, 0.9), white: hatch(C['hatch-white'], 5, 1.4) };
      return PAT;
    }

    /* labels */
    function pill(x, y, text, o) {
      o = o || {}; ctx.save(); var font = o.font || F(700, 8.5); ctx.font = font;
      var pd = o.pad == null ? 7 : o.pad, hh = o.h == null ? 17 : o.h, w = ctx.measureText(text).width + pd * 2 + (o.dot ? 6 : 0);
      var X = x; if (o.align === 'center') X = x - w / 2; else if (o.align === 'right') X = x - w;
      ctx.globalAlpha *= (o.alpha == null ? 1 : o.alpha);
      if (o.shadow !== false) { ctx.shadowColor = 'rgba(27,27,27,.22)'; ctx.shadowBlur = 6; ctx.shadowOffsetY = 2; }
      var bg = o.bg || C.ink; rr(X, y - hh / 2, w, hh, hh / 2); ctx.fillStyle = bg; ctx.fill(); ctx.shadowColor = 'transparent';
      if (o.border) { ctx.strokeStyle = o.border; ctx.lineWidth = 1; ctx.stroke(); }
      if (o.dot) led(X + pd + 1, y, o.dot, 2, 0);
      ctx.font = font; ctx.fillStyle = o.fg || textOn(bg); ctx.textAlign = 'center'; ctx.textBaseline = 'middle'; ctx.fillText(text, X + w / 2 + (o.dot ? 3 : 0), y + 0.5);
      ctx.restore(); return w;
    }
    function pillWidth(text, font, pd) { ctx.save(); ctx.font = font || F(700, 8.5); var w = ctx.measureText(text).width + (pd == null ? 7 : pd) * 2; ctx.restore(); return w; }
    function stipple(text, x, y, p, o) {
      o = o || {}; ctx.save(); var font = o.font || F(700, 11), sz = o.size || 11; ctx.font = font; ctx.textBaseline = 'alphabetic'; ctx.textAlign = 'left';
      var n = text.length, rev = p * (n + 3), col = o.color || C.ink, A = ctx.globalAlpha * (o.alpha == null ? 1 : o.alpha);
      var cx = x; if (o.align === 'center') cx = x - ctx.measureText(text).width / 2;
      for (var i = 0; i < n; i++) {
        var ch = text[i], cw = ctx.measureText(ch).width, d = rev - i;
        if (d >= 3) { ctx.globalAlpha = A; ctx.fillStyle = col; ctx.fillText(ch, cx, y); }
        else if (d > 0 && ch !== ' ') {
          var q = d / 3; ctx.fillStyle = col; ctx.globalAlpha = A * q * q; ctx.fillText(ch, cx, y);
          ctx.globalAlpha = A * (1 - q) * 0.9; var ds = Math.max(1, sz / 9);
          for (var a = 0; a < 3; a++) for (var b = 0; b < 4; b++) { if (hash(i * 31 + a * 7 + b * 3 + Math.floor(d * 9)) > 0.3 + q * 0.5) continue; ctx.fillRect(cx + (a + 0.5) * cw / 3 - ds / 2, y - sz * 0.74 + (b + 0.5) * sz * 0.78 / 4 - ds / 2, ds, ds); }
        }
        cx += cw;
      }
      ctx.restore();
    }
    function title(n, name, t) {
      stipple(pad2(n) + ' — ' + name, 20, 68, prog(t, 0, 1.1), { font: F(700, 10.5), size: 10.5, color: C.muted });
      ctx.save(); ctx.globalAlpha *= prog(t, 0, 0.4); ctx.strokeStyle = C.muted; ctx.lineWidth = 0.8; ctx.setLineDash([2, 3]); ctx.beginPath(); ctx.moveTo(20, 76); ctx.lineTo(20 + 180 * ease.outCubic(prog(t, 0.2, 1.2)), 76); ctx.stroke(); ctx.restore();
    }
    function terminal(x, y, w, hh, cmd, p, t, alpha) {
      alpha = alpha == null ? 1 : alpha; if (alpha <= 0) return;
      ctx.save(); ctx.globalAlpha *= alpha; ctx.shadowColor = 'rgba(0,0,0,.28)'; ctx.shadowBlur = 12; ctx.shadowOffsetY = 4; rr(x, y, w, hh, 7); ctx.fillStyle = C.ink; ctx.fill(); ctx.shadowColor = 'transparent';
      var s = cmd.slice(0, Math.floor(cmd.length * p)); ctx.font = F(500, 10); ctx.textBaseline = 'middle'; ctx.fillStyle = C.amber; ctx.fillText('›', x + 11, y + hh / 2);
      var cx = x + 23; s.split(/("[^"]*"?)/).forEach(function (part) { ctx.fillStyle = part.charAt(0) === '"' ? C.string : C.cream; ctx.fillText(part, cx, y + hh / 2); cx += ctx.measureText(part).width; });
      if (p < 1 || Math.floor(t * 2.2) % 2 === 0) { ctx.fillStyle = C.amber; ctx.fillRect(cx + 1, y + hh / 2 - 6, 6, 12); }
      ctx.restore();
    }

    /* objects */
    function cylinder(cx, cy, r, hh, rot, o) {
      o = o || {}; var ry = r * 0.36; ctx.save(); ctx.lineWidth = 1.1; ctx.strokeStyle = C.ink;
      ctx.save(); ctx.fillStyle = 'rgba(27,27,27,.15)'; ctx.shadowColor = 'rgba(27,27,27,.3)'; ctx.shadowBlur = 8; ctx.beginPath(); ctx.ellipse(cx + 5, cy + hh + 3, r, ry, 0, 0, 7); ctx.fill(); ctx.restore();
      ctx.beginPath(); ctx.moveTo(cx - r, cy); ctx.lineTo(cx - r, cy + hh); ctx.ellipse(cx, cy + hh, r, ry, 0, Math.PI, 0, true); ctx.lineTo(cx + r, cy); ctx.closePath();
      var g = ctx.createLinearGradient(cx - r, 0, cx + r, 0); g.addColorStop(0, C['tile-shade']); g.addColorStop(0.45, C.tile); g.addColorStop(1, '#bfb8a8'); ctx.fillStyle = g; ctx.fill(); ctx.stroke();
      ctx.save(); ctx.strokeStyle = 'rgba(27,27,27,.35)'; ctx.lineWidth = 0.8;
      for (var i = 0; i < 8; i++) { var a = rot + i * Math.PI / 4; if (Math.sin(a) <= 0) continue; var px = cx + r * Math.cos(a), py = ry * Math.sin(a); ctx.beginPath(); ctx.moveTo(px, cy + py); ctx.lineTo(px, cy + hh + py); ctx.stroke(); }
      ctx.restore();
      ctx.strokeStyle = C.ink; ctx.beginPath(); ctx.ellipse(cx, cy, r, ry, 0, 0, 7); ctx.fillStyle = '#f7f4ec'; ctx.fill(); ctx.stroke();
      ctx.beginPath(); ctx.ellipse(cx, cy, r * 0.62, ry * 0.62, 0, 0, 7); ctx.strokeStyle = 'rgba(27,27,27,.35)'; ctx.stroke();
      ctx.beginPath(); ctx.ellipse(cx, cy, r * 0.16, ry * 0.16, 0, 0, 7); ctx.fillStyle = C.ink; ctx.fill();
      var mx = cx + Math.cos(rot) * r * 0.8, my = cy + Math.sin(rot) * ry * 0.8; ctx.strokeStyle = C.amber; ctx.lineWidth = 1.6; ctx.beginPath(); ctx.moveTo(cx, cy); ctx.lineTo(mx, my); ctx.stroke(); led(mx, my, C.amber, 1.6, o.glow == null ? 0.6 : o.glow);
      ctx.restore();
    }
    function hourglass(cx, cy, Rr, H, rot, f) {
      var profile = function (y) { return Rr * (0.1 + 0.9 * Math.pow(Math.abs(y) / H, 1.3)); }; ctx.save();
      var sil = function (y0, y1) { ctx.beginPath(); for (var i = 0; i <= 12; i++) { var y = lerp(y0, y1, i / 12); ctx.lineTo(cx - profile(y), cy + y); } for (var j = 12; j >= 0; j--) { var yy = lerp(y0, y1, j / 12); ctx.lineTo(cx + profile(yy), cy + yy); } ctx.closePath(); };
      ctx.fillStyle = rgba(C.amber, 0.55); var top = H * (1 - f) * 0.92; if (top > 1) { sil(-top, -2); ctx.fill(); } var bot = H * f * 0.92; if (bot > 1) { sil(H - bot, H); ctx.fill(); }
      if (f < 1) { ctx.strokeStyle = C.amber; ctx.lineWidth = 1; ctx.beginPath(); ctx.moveTo(cx, cy); ctx.lineTo(cx, cy + H - bot); ctx.stroke(); }
      for (var m = 0; m < 10; m++) {
        var a = rot + m * Math.PI / 5, front = Math.sin(a) > 0; ctx.strokeStyle = front ? 'rgba(27,27,27,.85)' : 'rgba(27,27,27,.25)'; ctx.lineWidth = front ? 1 : 0.7; ctx.beginPath();
        for (var i = 0; i <= 16; i++) { var y = lerp(-H, H, i / 16), r = profile(y), px = cx + r * Math.cos(a), py = cy + y + r * Math.sin(a) * 0.3; if (i) ctx.lineTo(px, py); else ctx.moveTo(px, py); }
        ctx.stroke();
      }
      ctx.strokeStyle = C.ink; ctx.lineWidth = 1.2; [-H, H].forEach(function (y) { ctx.beginPath(); ctx.ellipse(cx, cy + y, Rr, Rr * 0.3, 0, 0, 7); ctx.stroke(); });
      ctx.lineWidth = 2.2; [-H - 3, H + 3].forEach(function (y) { ctx.beginPath(); ctx.ellipse(cx, cy + y, Rr * 1.12, Rr * 0.34, 0, 0, 7); ctx.stroke(); });
      ctx.restore();
    }
    function ribbon(cx, cy, len, alpha, t, dense) {
      if (alpha <= 0) return; ctx.save(); ctx.globalAlpha *= alpha;
      var step = dense ? 4.6 : 6.4, n = Math.max(2, Math.floor(len / step)), pts = [];
      for (var i = 0; i <= n; i++) { var x = cx - len / 2 + i * len / n; pts.push([x, cy + Math.sin(x * 0.09 + t * 5) * 3]); }
      ctx.lineCap = 'round'; ctx.lineJoin = 'round';
      ctx.strokeStyle = shade(C.blue, -0.35); ctx.lineWidth = 15; ctx.beginPath(); pts.forEach(function (p, j) { if (j) ctx.lineTo(p[0], p[1] + 1); else ctx.moveTo(p[0], p[1] + 1); }); ctx.stroke();
      ctx.strokeStyle = C.blue; ctx.lineWidth = 13; ctx.beginPath(); pts.forEach(function (p, j) { if (j) ctx.lineTo(p[0], p[1]); else ctx.moveTo(p[0], p[1]); }); ctx.stroke();
      ctx.font = F(700, dense ? 6.5 : 8); ctx.fillStyle = C.cream; ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
      for (var k = 0; k < n; k++) { var p = pts[k], q = pts[k + 1]; ctx.fillText(hash(k * 3 + (dense ? 7 : 1)) > 0.5 ? '1' : '0', (p[0] + q[0]) / 2, (p[1] + q[1]) / 2 + 0.5); }
      ctx.restore();
    }
    function record(cx, cy, alpha, s) {
      alpha = alpha == null ? 1 : alpha; if (alpha <= 0) return; ctx.save(); ctx.globalAlpha *= alpha; ctx.translate(cx, cy); ctx.scale(s || 1, s || 1);
      ctx.shadowColor = 'rgba(27,27,27,.2)'; ctx.shadowBlur = 6; ctx.shadowOffsetY = 2; rr(-46, -17, 92, 34, 5); ctx.fillStyle = C.white; ctx.fill(); ctx.shadowColor = 'transparent'; ctx.lineWidth = 1.1; ctx.strokeStyle = C.ink; ctx.stroke();
      ctx.font = F(700, 7.5); ctx.fillStyle = C.ink; ctx.fillText('ROW', -39, -6); ctx.font = F(500, 7.5); ctx.fillStyle = C.blue; ctx.fillText('user_id: 42', -39, 4); ctx.fillStyle = C.muted; ctx.fillText('ts, country, …', -39, 13);
      ctx.restore();
    }

    /* the network pipe: a Gaussian throat at x = 235 */
    function pipeHW(x) { return 34 - 23 * Math.exp(-Math.pow((x - 235) / 58, 2)); }
    function pipePath(cy) { ctx.beginPath(); for (var x = 34; x <= 436; x += 4) ctx.lineTo(x, cy - pipeHW(x)); for (var x2 = 436; x2 >= 34; x2 -= 4) ctx.lineTo(x2, cy + pipeHW(x2)); ctx.closePath(); }
    function pipeBody(cy) {
      ctx.save(); ctx.shadowColor = 'rgba(27,27,27,.2)'; ctx.shadowBlur = 10; ctx.shadowOffsetY = 6; pipePath(cy);
      var g = ctx.createLinearGradient(0, cy - 34, 0, cy + 34); g.addColorStop(0, '#fbfaf6'); g.addColorStop(0.55, '#ece7dc'); g.addColorStop(1, '#cfc8b8'); ctx.fillStyle = g; ctx.fill(); ctx.restore();
      ctx.save(); ctx.beginPath(); ctx.ellipse(34, cy, 7, pipeHW(34), 0, 0, 7); ctx.fillStyle = '#d6cfc0'; ctx.fill(); ctx.strokeStyle = C.ink; ctx.lineWidth = 1.1; ctx.stroke(); ctx.restore();
    }
    function pipeOutline(cy) {
      ctx.save(); ctx.lineWidth = 1.3; ctx.strokeStyle = C.ink;
      ctx.beginPath(); for (var x = 34; x <= 436; x += 4) ctx.lineTo(x, cy - pipeHW(x)); ctx.stroke();
      ctx.beginPath(); for (var x2 = 34; x2 <= 436; x2 += 4) ctx.lineTo(x2, cy + pipeHW(x2)); ctx.stroke();
      ctx.strokeStyle = 'rgba(255,255,255,.8)'; ctx.lineWidth = 2; ctx.beginPath(); for (var x3 = 44; x3 <= 426; x3 += 4) ctx.lineTo(x3, cy - pipeHW(x3) * 0.62); ctx.stroke();
      ctx.beginPath(); ctx.ellipse(436, cy, 7, pipeHW(436), 0, -Math.PI / 2, Math.PI / 2); ctx.strokeStyle = C.ink; ctx.lineWidth = 1.1; ctx.stroke(); ctx.restore();
    }

    return {
      ctx: ctx, C: C, F: F, SERIF: SERIF, color: color, textOn: textOn, labelOn: labelOn,
      poly: poly, rr: rr, txt: txt, led: led, card: card, iso: iso, isoTile: isoTile, boxGeo: boxGeo, box: box, slab: slab, SLAB: SLAB,
      hatch: hatch, pat: pat, pill: pill, pillWidth: pillWidth, stipple: stipple, title: title, terminal: terminal,
      cylinder: cylinder, hourglass: hourglass, ribbon: ribbon, record: record,
      pipeHW: pipeHW, pipePath: pipePath, pipeBody: pipeBody, pipeOutline: pipeOutline,
      mix: mix, shade: shade, rgba: rgba, clamp: clamp, lerp: lerp, prog: prog, hash: hash, ease: ease
    };
  }

  /* ---------- canvas plumbing for the figure components ---------- */
  function reduceMotion() { try { return window.matchMedia('(prefers-reduced-motion: reduce)').matches; } catch (e) { return false; } }
  var fontsReady = null;
  function loadFonts() {
    if (fontsReady) return fontsReady;
    if (!document.fonts || !document.fonts.load) { fontsReady = Promise.resolve(); return fontsReady; }
    fontsReady = Promise.all([
      document.fonts.load('500 12px "JetBrains Mono"'), document.fonts.load('800 12px "JetBrains Mono"'),
      document.fonts.load('500 12px "Newsreader"'), document.fonts.load('italic 400 12px "Newsreader"')
    ]).catch(function () {});
    return fontsReady;
  }
  function useCanvas(w, hh, draw, opts) {
    opts = opts || {};
    var ref = useRef(null), drawRef = useRef(draw);
    drawRef.current = draw;
    var animate = !!opts.animate, deps = opts.deps;
    useEffect(function () {
      var cv = ref.current; if (!cv) return undefined;
      var dpr = Math.min(2, window.devicePixelRatio || 1);
      cv.width = Math.round(w * dpr); cv.height = Math.round(hh * dpr);
      var ctx = cv.getContext('2d'), alive = true, raf = 0, start = performance.now();
      var K = kit(ctx, tokens(cv)), still = !animate || reduceMotion();
      function paint(now) {
        if (!alive) return;
        var t = still ? 0 : (now - start) / 1000;
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0); ctx.clearRect(0, 0, w, hh);
        ctx.save(); drawRef.current(K, t, still); ctx.restore();
        if (!still) raf = requestAnimationFrame(paint);
      }
      paint(performance.now());
      loadFonts().then(function () { if (alive && still) paint(performance.now()); });
      return function () { alive = false; cancelAnimationFrame(raf); };
    }, [w, hh, animate, deps]);
    return ref;
  }
  function figure(ref, w, hh, label, className) {
    return h('canvas', { ref: ref, className: 'si-figure' + (className ? ' ' + className : ''), width: w, height: hh, style: { width: w + 'px', height: hh + 'px' }, role: 'img', 'aria-label': label });
  }
  function cx() { return Array.prototype.filter.call(arguments, Boolean).join(' '); }

  /* ---------- frame chrome ---------- */
  function useFrameK(ref) {
    var st = useState(1), k = st[0], setK = st[1];
    useEffect(function () {
      var el = ref.current; if (!el || !window.ResizeObserver) return undefined;
      var ro = new ResizeObserver(function () { var r = el.getBoundingClientRect(); if (r.width) setK(r.width / 470); });
      ro.observe(el); return function () { ro.disconnect(); };
    }, []);
    return k;
  }
  function Frame(p) {
    var ref = useRef(null), k = useFrameK(ref);
    var style = Object.assign({ '--k': k.toFixed(4) }, p.width != null ? { width: typeof p.width === 'number' ? p.width + 'px' : p.width } : null);
    return h('div', { ref: ref, className: cx('si-frame', p.className), style: style }, p.children);
  }
  function Header(p) {
    var series = p.series || 'Spark Internals';
    var left = '> ' + series + (p.episode != null ? ' / EP. ' + pad2(p.episode) : '');
    var right = p.scene != null ? pad2(p.scene) + ' / ' + pad2(p.total || 9) : null;
    return h('header', { className: cx('si-header', p.docked && 'is-docked') },
      h('span', null, left), h('span', { className: 'si-cursor', 'aria-hidden': 'true' }),
      right && h('span', { className: 'si-header-r' }, right));
  }
  function tokenize(text) { var out = []; String(text || '').replace(/`([^`]+)`(\S*)|(\S+)/g, function (m, c, tail, w) { out.push(c ? { code: c, tail: tail || '' } : { w: w }); return m; }); return out; }
  function Deck(p) {
    var toks = tokenize(p.text), cur = p.current == null ? toks.length : p.current;
    return h('section', { className: cx('si-deck', p.beat && 'is-beat', p.docked && 'is-docked'), style: p.style, 'aria-label': 'Narration' },
      h('div', { className: 'si-deck-top' }, h('span', { className: 'si-deck-label' }, p.label || ''), h('span', { className: 'si-deck-clock', ref: p.clockRef }, p.clock || '')),
      h('p', { className: 'si-script' }, toks.map(function (t, j) {
        var state = j < cur ? ' is-spoken' : j === cur ? ' is-current' : '';
        return h('span', { key: j, className: 'si-w' + state }, t.code ? [h('span', { key: 'c', className: 'si-code' }, t.code), t.tail || null] : t.w);
      })),
      p.children ? h('div', { className: 'si-deck-controls' }, p.children) : null);
  }
  function DeckButton(p) {
    var rest = Object.assign({}, p); delete rest.on; delete rest.className; delete rest.children;
    return h('button', Object.assign({ type: 'button' }, rest, { className: cx('si-btn', p.on && 'is-on', p.className), 'aria-pressed': p.on == null ? undefined : !!p.on }), p.children);
  }
  function Scrubber(p) {
    var max = p.max || 78, ticks = p.ticks || [];
    return h('div', { className: 'si-scrub' },
      h('div', { className: 'si-ticks', 'aria-hidden': 'true' }, ticks.map(function (tk, i) {
        var at = typeof tk === 'number' ? tk : tk.at, beat = typeof tk === 'object' && tk.beat;
        return h('i', { key: i, className: beat ? 'is-beat' : null, style: { left: (at / max * 100) + '%' } });
      })),
      h('input', { type: 'range', id: p.id, min: 0, max: max, step: 0.01, defaultValue: p.value || 0, ref: p.inputRef, 'aria-label': p.label || 'Seek',
        onInput: function (e) { if (p.onChange) p.onChange(+e.target.value); }, onPointerDown: p.onPointerDown }));
  }

  /* ---------- labels ---------- */
  function SceneTitle(p) {
    var w = p.width || 240, hh = 30, text = pad2(p.n == null ? 1 : p.n) + ' — ' + String(p.name || '').toUpperCase();
    var ref = useCanvas(w, hh, function (K, t, still) {
      var tt = still ? 2 : t % 3.2;
      K.stipple(text, 1, 15, prog(tt, 0, 1.1), { font: K.F(700, 10.5), size: 10.5, color: K.C.muted });
      var c = K.ctx; c.save(); c.globalAlpha *= prog(tt, 0, 0.4); c.strokeStyle = K.C.muted; c.lineWidth = 0.8; c.setLineDash([2, 3]);
      c.beginPath(); c.moveTo(1, 23); c.lineTo(1 + Math.min(180, w - 2) * ease.outCubic(prog(tt, 0.2, 1.2)), 23); c.stroke(); c.restore();
    }, { animate: p.animate, deps: text + w });
    return figure(ref, w, hh, text);
  }
  function StippleText(p) {
    var serif = p.family === 'serif', size = p.size || (serif ? 50 : 11), weight = p.weight || (serif ? 500 : 700), text = String(p.text || '');
    var w = p.width || Math.ceil(text.length * size * (serif ? 0.5 : 0.62)) + 8, hh = Math.ceil(size * 1.45);
    var ref = useCanvas(w, hh, function (K, t, still) {
      var dur = p.duration || 1.1, tt = still ? dur + 1 : t % (dur + 1.6);
      var font = serif ? K.SERIF(weight, size, p.italic) : K.F(weight, size);
      K.stipple(text, p.align === 'center' ? w / 2 : 2, Math.round(size * 1.05), prog(tt, 0, dur), { font: font, size: size, align: p.align, color: K.color(p.color || 'ink') });
    }, { animate: p.animate, deps: JSON.stringify([text, size, weight, serif, p.italic, p.color, p.align, w]) });
    return figure(ref, w, hh, text);
  }
  var PILL_TONES = { ink: 1, blue: 1, green: 1, red: 1, amber: 1, outline: 1, k1: 1, k2: 1, k3: 1, k4: 1 };
  function Pill(p) {
    var tone = PILL_TONES[p.tone] ? p.tone : 'ink';
    return h('span', { className: cx('si-pill', 'si-pill--' + tone, p.size && 'si-pill--' + p.size, p.dot && 'has-dot') },
      p.dot ? h('i', { className: 'si-pill-dot', 'aria-hidden': 'true' }) : null, h('span', null, p.children));
  }

  /* ---------- cards and data ---------- */
  function Card(p) {
    return h('div', { className: cx('si-card', p.className), style: p.style },
      p.eyebrow ? h('div', { className: cx('si-eyebrow', p.tone && 'si-tone-' + p.tone) }, p.eyebrow) : null, p.children);
  }
  function Stat(p) {
    return h('div', { className: cx('si-stat', 'si-stat--' + (p.size || 'lg')) },
      p.label ? h('div', { className: 'si-eyebrow' }, p.label) : null,
      h('div', { className: cx('si-stat-value', p.tone && 'si-tone-' + p.tone) }, p.value),
      p.note ? h('div', { className: 'si-stat-note' }, p.note) : null,
      p.detail ? h('div', { className: 'si-stat-detail' }, p.detail) : null);
  }
  function MetricRows(p) {
    return h('dl', { className: 'si-rows' }, (p.rows || []).map(function (r, i) {
      return h('div', { key: i, className: 'si-row' }, h('dt', null, r.label), h('dd', { className: r.tone ? 'si-tone-' + r.tone : null }, r.value));
    }));
  }
  function ProgressBar(p) {
    var v = clamp(p.value || 0);
    return h('div', { className: 'si-progress' },
      h('div', { className: 'si-progress-track', role: 'progressbar', 'aria-valuemin': 0, 'aria-valuemax': 100, 'aria-valuenow': Math.round(v * 100), 'aria-label': p.label || 'Progress' },
        h('div', { className: 'si-progress-fill', style: { width: (v * 100) + '%' } }, p.stalled ? h('i', { className: 'si-progress-tip' }) : null)),
      h('div', { className: 'si-progress-meta' }, h('span', null, p.caption || ''), h('b', null, Math.round(v * 100) + '%')));
  }
  function StepList(p) {
    var cur = p.current == null ? -1 : p.current, sp = clamp(p.progress == null ? 0.5 : p.progress);
    return h('ol', { className: 'si-steps' }, (p.steps || []).map(function (s, i) {
      var st = i < cur ? 'done' : i === cur ? 'current' : 'todo';
      return h('li', { key: i, className: 'si-step is-' + st },
        h('i', { className: 'si-check', 'aria-hidden': 'true' }, st === 'current' ? h('b', { style: { width: (sp * 75) + '%' } }) : null),
        h('span', { className: 'si-step-label' }, s.label, h('span', { className: 'si-sr' }, st === 'done' ? ' (done)' : st === 'current' ? ' (in progress)' : '')),
        s.cost != null ? h('span', { className: 'si-step-cost' }, s.cost) : null);
    }));
  }
  function TaskBars(p) {
    var bar = clamp(p.barrier == null ? 0.5 : p.barrier, 0.1, 0.9), tasks = p.tasks || [];
    return h('div', { className: 'si-tasks', style: { '--b': bar } },
      h('div', { className: 'si-tasks-head' }, p.label ? h(Pill, { tone: 'red' }, p.label) : null),
      h('div', { className: 'si-tasks-body' },
        h('i', { className: 'si-tasks-barrier', 'aria-hidden': 'true' }),
        tasks.map(function (tk, i) {
          return h('div', { key: i, className: 'si-task' },
            h('span', { className: 'si-task-label' }, tk.label || 'task ' + (i + 1)),
            h('span', { className: 'si-task-track' },
              h('i', { className: 'si-task-wait', style: { width: (clamp(tk.wait == null ? 1 : tk.wait) * bar * 100) + '%' } }),
              tk.work ? h('i', { className: 'si-task-work', style: { width: (clamp(tk.work) * (1 - bar) * 100) + '%' } }) : null));
        })),
      h('div', { className: 'si-tasks-legend' }, h('span', null, h('i', { className: 'si-swatch si-task-wait' }), 'waiting'), h('span', null, h('i', { className: 'si-swatch si-task-work' }), 'real work')));
  }
  function DataGrid(p) {
    var cols = p.columns || [], rows = p.rows || [], keepR = p.keepRows, keepC = p.keepColumns;
    var keptR = function (i) { return !keepR || keepR.indexOf(i) >= 0; }, keptC = function (j) { return !keepC || keepC.indexOf(j) >= 0; };
    var showR = rows.map(function (r, i) { return i; }).filter(function (i) { return !p.filtered || keptR(i); });
    var showC = cols.map(function (c, j) { return j; }).filter(function (j) { return !p.filtered || keptC(j); });
    return h('div', { className: cx('si-grid-wrap', p.oblique !== false && 'is-oblique') },
      h('table', { className: 'si-grid' },
        h('thead', null, h('tr', null, showC.map(function (j) { return h('th', { key: j, className: keptC(j) ? null : 'is-out' }, cols[j]); }))),
        h('tbody', null, showR.map(function (i) {
          return h('tr', { key: i, className: p.marked === false || p.filtered ? null : (keptR(i) ? 'is-kept' : 'is-dropped') }, showC.map(function (j) {
            var cell = rows[i][j], v = cell && typeof cell === 'object' ? cell.v : cell, tone = cell && typeof cell === 'object' ? cell.tone : null;
            return h('td', { key: j, className: cx(!keptC(j) && 'is-out', tone && 'si-tone-' + tone) }, v);
          }));
        }))));
  }

  /* ---------- code ---------- */
  function Terminal(p) {
    var cmd = String(p.command || ''), n = p.typed == null ? cmd.length : Math.floor(cmd.length * clamp(p.typed));
    var parts = cmd.slice(0, n).split(/("[^"]*"?)/);
    return h('div', { className: 'si-terminal' },
      h('span', { className: 'si-prompt', 'aria-hidden': 'true' }, '›'),
      h('code', null, parts.map(function (part, i) { return part ? h('span', { key: i, className: part.charAt(0) === '"' ? 'si-str' : null }, part) : null; })),
      p.cursor === false ? null : h('i', { className: 'si-term-cursor', 'aria-hidden': 'true' }));
  }
  function CodeCompare(p) {
    var bad = p.bad || {}, good = p.good || {};
    function side(s, kind) {
      return h('div', { className: 'si-card si-compare-card is-' + kind },
        h('div', { className: 'si-eyebrow si-tone-' + (kind === 'bad' ? 'red' : 'green') }, s.title || (kind === 'bad' ? 'ANTI-PATTERN' : 'DO THIS')),
        h('pre', { className: 'si-compare-code' }, (s.code || []).join('\n')),
        s.note ? h('div', { className: 'si-compare-note' }, s.note) : null,
        kind === 'bad' && p.struck !== false ? h('svg', { className: 'si-strike', viewBox: '0 0 208 100', preserveAspectRatio: 'none', 'aria-hidden': 'true' },
          h('line', { x1: 10, y1: 10, x2: 198, y2: 90, className: 'si-strike-under', vectorEffect: 'non-scaling-stroke' }),
          h('line', { x1: 10, y1: 10, x2: 198, y2: 90, className: 'si-strike-over', vectorEffect: 'non-scaling-stroke' })) : null);
    }
    return h('div', { className: 'si-compare' }, side(bad, 'bad'), side(good, 'good'));
  }
  function Record(p) {
    return h('div', { className: 'si-record' },
      h('b', null, p.title || 'ROW'), h('span', { className: 'si-record-key' }, p.field || 'user_id: 42'), h('span', { className: 'si-record-rest' }, p.rest || 'ts, country, …'));
  }

  /* ---------- machines and materials (canvas) ---------- */
  function Machine(p) {
    var w = p.w || 78, bh = p.h || 56, d = p.depth == null ? 40 : p.depth, pd = 8;
    var W = Math.ceil(w + d * 0.56 + pd * 2 + Math.max(3, d * 0.2) + 4), H = Math.ceil(bh + d * 0.48 + w * 0.085 + pd * 2 + 6);
    var ref = useCanvas(W, H, function (K, t) {
      var x = pd + d * 0.56, y = pd + d * 0.48 + w * 0.085;
      K.box(x, y, w, bh, d, {
        color: K.color(p.color || 'node'), dividers: p.dividers == null ? 1 : p.dividers, vents: p.vents == null ? 5 : p.vents,
        leds: (p.leds || ['green']).map(K.color), label: p.label, glow: p.animate ? 0.6 + 0.4 * Math.sin(t * 4) : 0.8,
        pattern: p.pattern || null, wire: !!p.wire, dash: p.wire ? [4, 3] : null, dashOffset: p.wire ? -t * 14 : 0, stroke: p.wire ? K.C.blue : null,
        topLabel: p.topLabel, labelColor: p.labelColor ? K.color(p.labelColor) : null
      });
    }, { animate: p.animate, deps: JSON.stringify(p) });
    return figure(ref, W, H, p.label || 'Machine');
  }
  var SLAB_FILL = { k1: 'key-1', k2: 'key-2', k3: 'key-3', k4: 'key-4', idle: 'slab', fact: 'fact' };
  function Slab(p) {
    var stack = p.stack || ['k1'], s = p.scale || 2, W = Math.ceil((26 + 14 * 0.56 + 8) * s) + 4, H = Math.ceil((5 * stack.length + 14 * 0.48 + 26 * 0.085 + 10) * s) + 4;
    var ref = useCanvas(W, H, function (K) {
      K.ctx.scale(s, s);
      var x = 4 + 14 * 0.56, y = H / s - 5;
      stack.forEach(function (key, i) {
        var label = key === 'idle' ? '' : key === 'fact' ? 'FACT' : key;
        K.slab(x, y - i * 5, K.color(SLAB_FILL[key] || key), { topLabel: p.labels === false ? '' : label, topLabelFont: key === 'fact' ? K.F(800, 5.5) : K.F(800, 6.5) });
      });
    }, { deps: JSON.stringify(p) });
    return figure(ref, W, H, 'Slab stack: ' + stack.join(', '));
  }
  function RackGrid(p) {
    var n = p.size || 10, s = p.scale || 12.4, tw = 0.86, th = 0.7;
    var W = Math.ceil(2 * n * 0.866 * s) + 24, H = Math.ceil(2 * n * 0.5 * s + th * s) + 24;
    var ref = useCanvas(W, H, function (K, t) {
      var ox = W / 2, oy = 12 + th * s + 4, slow = !!p.slow, strag = p.stragglers == null ? 0.13 : p.stragglers;
      for (var sum = 0; sum <= 2 * (n - 1); sum++) for (var i = 0; i < n; i++) {
        var j = sum - i; if (j < 0 || j > n - 1) continue;
        var isStrag = slow && hash(i * 10 + j) < strag, warn = slow && !isStrag && hash(j * 10 + i) < 0.25;
        var bob = p.animate && !slow ? 0.12 * Math.sin(t * 3 - sum * 0.5) : 0;
        K.isoTile(i, j, bob, tw, tw, th, ox, oy, s, {
          vents: true, color: isStrag ? K.C['tint-dropped'] : K.C.tile,
          led: isStrag ? K.C.red : warn ? K.C.amber : K.C.green,
          glow: p.animate ? (isStrag ? 0.6 + 0.4 * Math.sin(t * 8) : 0.5 + 0.5 * Math.sin(t * 4 + sum * 0.6)) : 0.7
        });
      }
    }, { animate: p.animate, deps: JSON.stringify(p) });
    return figure(ref, W, H, n * n + ' executors' + (p.slow ? ', some straggling' : ''));
  }
  function Disk(p) {
    var r = p.r || 26, ch = p.h || 30, W = r * 2 + 30, H = ch + r * 0.72 + (p.label === false ? 18 : 34);
    var ref = useCanvas(W, H, function (K, t, still) {
      var ccx = W / 2 - 3, ccy = r * 0.36 + 6;
      K.cylinder(ccx, ccy, r, ch, still ? 0.6 : t * (p.active ? 9 : 1.6), { glow: p.active ? 1 : 0.4 });
      if (p.label !== false) K.txt(p.label || 'LOCAL DISK', ccx, ccy + ch + r * 0.36 + 22, K.F(700, 7.5), p.active ? K.C.ink : K.C.muted, 'center');
    }, { animate: p.animate, deps: JSON.stringify(p) });
    return figure(ref, W, H, (p.label || 'Local disk') + (p.active ? ', writing' : ''));
  }
  function Hourglass(p) {
    var W = 56, H = 84;
    var ref = useCanvas(W, H, function (K, t, still) {
      var f = still ? (p.fill == null ? 0.5 : p.fill) : (t % 4) / 4;
      K.hourglass(W / 2, H / 2, 15, 30, still ? 0.4 : t * 1.6, f);
    }, { animate: p.animate, deps: JSON.stringify(p) });
    return figure(ref, W, H, 'Waiting');
  }
  function LED(p) {
    var r = p.size || 1.7, W = Math.ceil(r * 12), H = W;
    var ref = useCanvas(W, H, function (K, t) {
      K.led(W / 2, H / 2, K.color(p.color || 'green'), r, p.animate ? 0.5 + 0.5 * Math.sin(t * (p.color === 'red' ? 8 : 4)) : (p.glow == null ? 1 : p.glow));
    }, { animate: p.animate, deps: JSON.stringify(p) });
    return figure(ref, W, H, (p.color || 'green') + ' LED', 'si-led');
  }
  function BitRibbon(p) {
    var len = p.length || 110, W = len + 24, H = 34;
    var ref = useCanvas(W, H, function (K, t, still) { K.ribbon(W / 2, H / 2, len, 1, still ? 0.3 : t, !!p.dense); }, { animate: p.animate, deps: JSON.stringify(p) });
    return figure(ref, W, H, p.dense ? 'Compressed bytes' : 'Serialized bytes');
  }
  function Pipe(p) {
    var W = 470, H = 118, cyy = 66, mode = p.mode === 'flowing' ? 'flowing' : 'jammed';
    var ref = useCanvas(W, H, function (K, t, still) {
      var c = K.ctx;
      K.pipeBody(cyy); c.save(); K.pipePath(cyy); c.clip();
      if (mode === 'jammed') {
        var tt = still ? 6.5 : t % 9, stopX = 196, v = 110, deps = [], prev = -1e9, queued = 0;
        for (var i = 0; i < 16; i++) { var sp = 0.3 + i * 0.4; prev = Math.max(sp + (stopX - 20) / v, prev + 1.6); deps.push([sp, prev]); }
        var aheadOf = function (i2) { var a = 0; for (var j = 0; j < i2; j++) if (deps[j][1] > tt) a++; return a; };
        for (var b = 15; b >= 0; b--) {
          var s0 = deps[b][0], dp = deps[b][1]; if (tt < s0) continue;
          var x, bh = 20, isQ = false;
          if (tt < dp) { var qx = stopX - aheadOf(b) * 24; x = Math.min(20 + v * (tt - s0), qx); if (x >= qx - 0.5) { queued++; isQ = true; } }
          else { var e = tt - dp; x = stopX + 26 * e + 42 * e * e; bh = Math.min(20, K.pipeHW(x) * 1.6); }
          if (x > 450) continue;
          K.box(x - 11, cyy - bh / 2 + 2, 22, bh, 7, { color: isQ ? K.C['tint-queued'] : K.C.block, shadow: false, lw: 0.8 });
        }
        if (queued > 2) { var g = c.createRadialGradient(stopX + 10, cyy, 0, stopX + 10, cyy, 70); g.addColorStop(0, rgba(K.C.red, 0.28 * clamp((queued - 2) / 4))); g.addColorStop(1, rgba(K.C.red, 0)); c.fillStyle = g; c.fillRect(stopX - 70, cyy - 40, 140, 80); }
        c.restore(); K.pipeOutline(cyy);
        if (p.readout !== false) K.pill(436, 16, 'QUEUED ' + pad2(queued), { bg: queued > 3 ? K.C.red : K.C.ink, align: 'right' });
      } else {
        var t2 = still ? 4 : t, first = Math.max(0, Math.floor((t2 - 3.6) / 0.13)), last = Math.floor(t2 / 0.13);
        for (var n2 = last; n2 >= first; n2--) {
          var age = t2 - n2 * 0.13; if (age < 0) continue;
          var xx = 20 + xAtTime(age); if (xx > 450) continue;
          var hw = K.pipeHW(clamp(xx, 34, 436)), ly = ((n2 % 3) - 1) * hw * 0.48;
          if (hw < 20) { c.save(); c.strokeStyle = rgba(K.C.blue, 0.35 * (1 - hw / 20)); c.lineWidth = 2; c.beginPath(); c.moveTo(xx - 6, cyy + ly); c.lineTo(xx - 6 - (20 - hw) * 2.4, cyy + ly); c.stroke(); c.restore(); }
          K.box(xx - 5, cyy + ly - 4, 10, 8, 5, { color: n2 % 4 === 0 ? K.C.green : K.C.blue, shadow: false, lw: 0.7 });
        }
        c.restore(); K.pipeOutline(cyy);
        if (p.readout !== false) K.pill(436, 16, p.badge || '11× THROUGHPUT', { bg: K.C.green, align: 'right' });
      }
    }, { animate: p.animate, deps: JSON.stringify(p) });
    return figure(ref, W, H, mode === 'jammed' ? 'Network pipe, jammed with full rows' : 'Network pipe, flowing with small blocks', 'si-pipe');
  }
  var VT = (function () { var a = [], T = 0; for (var x = 0; x <= 480; x += 2) { a.push(T); var hw = 34 - 23 * Math.exp(-Math.pow((Math.max(34, x) - 235) / 58, 2)); T += 2 / (150 * 34 / hw); } return a; })();
  function xAtTime(s) { var lo = 0, hi = VT.length - 1; if (s >= VT[hi]) return 480; while (hi - lo > 1) { var m = (lo + hi) >> 1; if (VT[m] <= s) lo = m; else hi = m; } return lo * 2; }

  /* ---------- ending ---------- */
  function ClosingCard(p) {
    var takeaways = p.takeaways || [], rows = [];
    for (var r = 0; r < takeaways.length; r += 2) rows.push(takeaways.slice(r, r + 2));
    return h('div', { className: 'si-closing' },
      h('div', { className: 'si-closing-eyebrow' }, String(p.series || 'Spark Internals').toUpperCase() + (p.episode != null ? ' · EP. ' + pad2(p.episode) : '')),
      h('h2', { className: 'si-closing-title' }, p.title || 'Move less data.'),
      p.subtitle ? h('p', { className: 'si-closing-sub' }, p.subtitle) : null,
      h('i', { className: 'si-closing-rule', 'aria-hidden': 'true' }),
      rows.length ? h('div', { className: 'si-closing-pills' }, rows.map(function (row, ri) { return h('div', { key: ri }, row.map(function (tk, i) { return h(Pill, { key: i, tone: tk.tone || 'green', size: 'lg' }, tk.label); })); })) : null,
      p.footnote ? h('code', { className: 'si-closing-foot' }, p.footnote) : null);
  }

  /* ---------- the episode player: frame + canvas scenes + karaoke deck + controls ---------- */
  function buildModel(segments) {
    var acc = 0, scenes = 0;
    var segs = (segments || []).map(function (sg) {
      var o = Object.assign({}, sg); o.dur = o.dur || (o.beat ? 2.5 : 7); o.start = acc; acc += o.dur;
      if (!o.beat) o.n = ++scenes;
      var toks = tokenize(o.text), a = o.beat ? 0.15 : 0.25, b = o.dur - (o.beat ? 0.55 : 0.45);
      var wts = toks.map(function (t) { return (t.code || t.w).length + 2; }), sum = wts.reduce(function (x, y) { return x + y; }, 0) || 1, c = a;
      o.times = toks.map(function (t, j) { var at = c; c += (b - a) * wts[j] / sum; return at; }); o.endT = b;
      return o;
    });
    segs.forEach(function (o, i) { if (o.beat) for (var j = i - 1; j >= 0; j--) if (!segs[j].beat) { o.ref = segs[j]; break; } });
    return { segs: segs, total: acc || 1, scenes: scenes || 1 };
  }
  function Episode(p) {
    var model = useMemo(function () { return buildModel(p.segments); }, [p.segments]);
    var frameRef = useRef(null), cvRef = useRef(null), scrubRef = useRef(null), clockRef = useRef(null);
    var timeRef = useRef(0), playRef = useRef(p.autoplay !== false && !reduceMotion()), dragRef = useRef(false);
    var vs = useState({ seg: 0, word: -1 }), view = vs[0], setView = vs[1];
    var ps = useState(playRef.current), playing = ps[0], setPlaying = ps[1];
    var es = useState(true), autoExpand = es[0], setAutoExpand = es[1];
    var ts = useState(false), tts = ts[0], setTts = ts[1];
    var k = useFrameK(frameRef);

    useEffect(function () {
      if (p.storageKey) { try { var v = parseFloat(window.localStorage.getItem(p.storageKey)); if (v >= 0 && v < model.total) timeRef.current = v; } catch (e) { /* storage unavailable */ } }
      var frame = frameRef.current, cv = cvRef.current; if (!frame || !cv) return undefined;
      var ctx = cv.getContext('2d'), dpr = 1, kk = 1, LH = 835, alive = true, raf = 0, last = performance.now(), lastSave = 0, K = kit(ctx, tokens(frame));
      function resize() { var r = frame.getBoundingClientRect(); if (!r.width) return; dpr = Math.min(2, window.devicePixelRatio || 1); cv.width = Math.round(r.width * dpr); cv.height = Math.round(r.height * dpr); kk = r.width / 470; LH = r.height / kk; }
      var ro = window.ResizeObserver ? new ResizeObserver(resize) : null; if (ro) ro.observe(frame); resize();
      function segAt(time) { for (var i = model.segs.length - 1; i >= 0; i--) if (time >= model.segs[i].start) return i; return 0; }
      function loop(now) {
        if (!alive) return;
        var dt = Math.min(0.05, (now - last) / 1000); last = now;
        if (playRef.current && !dragRef.current) { timeRef.current += dt; if (timeRef.current >= model.total) { timeRef.current = model.total - 0.001; playRef.current = false; setPlaying(false); } }
        var time = timeRef.current, i = segAt(time), s = model.segs[i], lt = time - s.start;
        ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.clearRect(0, 0, cv.width, cv.height);
        var sc = Math.min(1, (LH * 0.63 - 6) / 516); ctx.setTransform(dpr * kk * sc, 0, 0, dpr * kk * sc, dpr * kk * (470 - 470 * sc) / 2, 0);
        var scn = s.beat ? s.ref : s, st = s.beat ? (scn ? scn.dur - 0.001 : 0) : lt, nx = model.segs[i + 1];
        if (scn && scn.draw) {
          ctx.save(); ctx.globalAlpha = s.beat ? 1 : prog(lt, 0, 0.35) * (nx && !nx.beat ? 1 - prog(lt, s.dur - 0.3, s.dur) : 1);
          try { scn.draw(K, st, scn.dur, scn.name, scn.n); } catch (err) { if (window.console) console.error(err); }
          ctx.restore();
        }
        var cur = -1; for (var j = 0; j < s.times.length; j++) if (lt >= s.times[j]) cur = j; if (lt > s.endT) cur = s.times.length;
        setView(function (v) { return v.seg === i && v.word === cur ? v : { seg: i, word: cur }; });
        if (clockRef.current) { var c = fmt(time) + ' / ' + fmt(model.total); if (clockRef.current.textContent !== c) clockRef.current.textContent = c; }
        if (scrubRef.current && !dragRef.current) scrubRef.current.value = time;
        if (p.storageKey && now - lastSave > 1000) { lastSave = now; try { window.localStorage.setItem(p.storageKey, time.toFixed(2)); } catch (e2) { /* storage unavailable */ } }
        raf = requestAnimationFrame(loop);
      }
      raf = requestAnimationFrame(loop);
      loadFonts();
      var up = function () { dragRef.current = false; };
      window.addEventListener('pointerup', up);
      return function () { alive = false; cancelAnimationFrame(raf); if (ro) ro.disconnect(); window.removeEventListener('pointerup', up); };
    }, [model]);

    useEffect(function () {
      if (!tts || !playing || !('speechSynthesis' in window)) return;
      var s = model.segs[view.seg]; if (!s) return;
      window.speechSynthesis.cancel(); var u = new window.SpeechSynthesisUtterance(String(s.text || '').replace(/`/g, '')); u.rate = s.beat ? 0.95 : 1.12; window.speechSynthesis.speak(u);
    }, [view.seg, tts, playing]);

    function stopSpeech() { if ('speechSynthesis' in window) window.speechSynthesis.cancel(); }
    function seek(time) { timeRef.current = clamp(time, 0, model.total - 0.001); stopSpeech(); }
    function step(d) {
      var t = timeRef.current, i = 0; for (var n = model.segs.length - 1; n >= 0; n--) if (t >= model.segs[n].start) { i = n; break; }
      var lt = t - model.segs[i].start, j = d < 0 ? (lt > 0.6 ? i : i - 1) : i + 1; seek(model.segs[clamp(j, 0, model.segs.length - 1)].start);
    }
    function togglePlay() { if (!playRef.current && timeRef.current >= model.total - 0.01) seek(0); playRef.current = !playRef.current; setPlaying(playRef.current); if (!playRef.current) stopSpeech(); }
    function onKey(e) {
      if (e.target && e.target.tagName === 'INPUT' && e.code !== 'Space') return;
      if (e.target && e.target.tagName === 'BUTTON' && (e.code === 'Space' || e.code === 'Enter')) return;
      if (e.code === 'Space') { e.preventDefault(); togglePlay(); } else if (e.code === 'ArrowRight') step(1); else if (e.code === 'ArrowLeft') step(-1);
    }

    var s = model.segs[view.seg] || model.segs[0] || { text: '', times: [] }, scn = s.beat ? s.ref : s;
    var label = s.beat ? 'BEAT · ' + pad2(scn ? scn.n : 0) + ' → ' + pad2((scn ? scn.n : 0) + 1) : 'SCENE ' + pad2(s.n || 1) + ' / ' + pad2(model.scenes) + ' · ' + String(s.name || '').toUpperCase();
    var ticks = model.segs.map(function (sg) { return { at: sg.start, beat: !!sg.beat }; });
    return h('div', { ref: frameRef, className: 'si-frame si-episode', style: Object.assign({ '--k': k.toFixed(4) }, p.width != null ? { width: typeof p.width === 'number' ? p.width + 'px' : p.width } : null), tabIndex: 0, onKeyDown: onKey, role: 'region', 'aria-label': (p.series || 'Spark Internals') + (p.episode != null ? ', episode ' + p.episode : '') },
      h('canvas', { ref: cvRef, className: 'si-episode-canvas', 'aria-hidden': 'true' }),
      h(Header, { series: p.series, episode: p.episode, scene: scn ? scn.n : 1, total: model.scenes, docked: true }),
      h(Deck, { docked: true, beat: autoExpand && !!s.beat, label: label, clockRef: clockRef, clock: fmt(0) + ' / ' + fmt(model.total), text: s.text, current: view.word },
        h(Scrubber, { max: model.total, ticks: ticks, inputRef: scrubRef, label: 'Seek', onChange: seek, onPointerDown: function () { dragRef.current = true; } }),
        h('div', { className: 'si-btns' },
          h(DeckButton, { onClick: function () { step(-1); } }, '← Step'),
          h(DeckButton, { onClick: togglePlay }, playing ? 'Pause' : 'Play'),
          h(DeckButton, { onClick: function () { step(1); } }, 'Step →'),
          h(DeckButton, { onClick: function () { setAutoExpand(!autoExpand); } }, 'Expand: ' + (autoExpand ? 'Auto' : 'Locked')),
          h(DeckButton, { on: tts, onClick: function () { var n = !tts; setTts(n); if (!n) stopSpeech(); } }, 'TTS: ' + (tts ? 'On' : 'Off')))));
  }

  var api = {
    Episode: Episode, Frame: Frame, Header: Header, Deck: Deck, DeckButton: DeckButton, Scrubber: Scrubber,
    SceneTitle: SceneTitle, StippleText: StippleText, Pill: Pill, Card: Card, Stat: Stat, MetricRows: MetricRows,
    ProgressBar: ProgressBar, StepList: StepList, TaskBars: TaskBars, DataGrid: DataGrid, Terminal: Terminal,
    CodeCompare: CodeCompare, Record: Record, Machine: Machine, Slab: Slab, RackGrid: RackGrid, Disk: Disk,
    Hourglass: Hourglass, LED: LED, BitRibbon: BitRibbon, Pipe: Pipe, ClosingCard: ClosingCard,
    kit: kit, tokens: tokens, ease: ease, clamp: clamp, lerp: lerp, prog: prog, hash: hash, tokenize: tokenize
  };
  window.SparkInternals = Object.assign(window.SparkInternals || {}, api);
})();
