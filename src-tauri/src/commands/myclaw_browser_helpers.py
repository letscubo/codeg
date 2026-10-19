"""MyClaw 浏览器的页面助手:elements() 把当前页读成一张编号元素表,act(n, ...) 按编号操作。

browser-harness 启动时把 $BH_AGENT_WORKSPACE/agent_helpers.py 里的公开函数并进它的 helpers,
browser_exec 的代码里可以直接调用(启动脚本每次把本文件拷过去)。

为什么要它:agent 原来看页面靠截图(大、慢,而且有的线路上模型根本收不到图),点东西靠
算坐标(页面一滚就偏)。元素表是几百字的文本,每个编号绑着真实的页面元素;操作前先核对
页面没变、元素还在、没被挡住,不对就让 agent 重新看,而不是点空了还以为成功。

页面读取与执行前校验改编自 browser-use/jev-ultrafast 的 snapshot.js 与执行器
(MIT License, Copyright (c) 2026 Browser Use):https://github.com/browser-use/jev-ultrafast
"""

import json
import time

import browser_harness.helpers as _h

# 读页面:可见的可操作控件(按钮、链接、输入框、下拉、复选……)+ 屏幕上的可见文字。
# 元素引用留在页面里(window.__myclawEls),编号只在 Python 侧;校验用的指纹也留在页面里,
# 不回传(每个元素带一段上下文文字,全回传会有几百 KB)。
_SNAPSHOT_JS = r"""
((limit, textChars, optionCap) => {
  if (!document.body) return null;
  const cache = window.__myclawEls ||= {ids:new WeakMap(), nodes:new Map(), next:1};
  const identity = e => {
    if (!cache.ids.has(e)) cache.ids.set(e, cache.next++);
    const id = cache.ids.get(e); cache.nodes.set(id, e); return id;
  };
  for (const [id, e] of cache.nodes) if (!e.isConnected) cache.nodes.delete(id);
  const safe = e => !['password','file','hidden'].includes(e.type);
  const visible = e => !e.closest('[aria-hidden="true"],[inert]') &&
    e.checkVisibility({checkOpacity:true, checkVisibilityCSS:true});
  const name = (e, seen=new Set()) => {
    if (!e || seen.has(e)) return '';
    seen.add(e);
    const referenced = (e.getAttribute('aria-labelledby')||'').split(/\s+/)
      .map(id => name(document.getElementById(id), seen)).filter(Boolean).join(' ');
    return referenced || e.getAttribute('aria-label') ||
      [...(e.labels||[])].map(l => name(l, seen)).filter(Boolean).join(' ') ||
      (['button','submit','reset'].includes(e.type) ? e.value : '') || e.getAttribute('alt') ||
      (e.tagName==='INPUT' ? '' : [...e.childNodes].map(n => n.nodeType===3 ? n.textContent :
        n.nodeType===1 && n.getAttribute('aria-hidden')!=='true' ? name(n, seen) : '').join(' ').trim()) ||
      e.getAttribute('title') || e.getAttribute('placeholder') || e.getAttribute('name') || '';
  };
  const roles = ['button','link','checkbox','radio','switch','tab','menuitem','menuitemradio',
    'option','gridcell','combobox','textbox','searchbox','spinbutton'];
  const selector = 'a[href],button,input,textarea,select,summary,[contenteditable="true"],' +
    roles.map(r => '[role="'+r+'"]').join(',');
  const role = e => {
    const explicit = e.getAttribute('role');
    if (roles.includes(explicit)) return explicit;
    if (e.tagName==='BUTTON' || e.tagName==='SUMMARY') return 'button';
    if (e.tagName==='A') return 'link';
    if (e.tagName==='SELECT') return 'combobox';
    if (e.tagName==='TEXTAREA' || e.isContentEditable) return 'textbox';
    if (e.tagName==='INPUT') {
      if (['checkbox','radio'].includes(e.type)) return e.type;
      if (['button','submit','reset','image'].includes(e.type)) return 'button';
      if (e.type==='search') return 'searchbox';
      if (e.type==='number') return 'spinbutton';
      if (['text','email','url','tel','date','time','datetime-local','month','week',''].includes(e.type)) return 'textbox';
    }
    return null;
  };
  // 页面指纹:换页、滚动、窗口大小、表单值变了都算变
  cache.pageKey = () => JSON.stringify([performance.timeOrigin, location.href, scrollX, scrollY, innerWidth, innerHeight,
    [...document.querySelectorAll('input,textarea,select')].filter(safe)
      .map(e => [identity(e), e.value, e.checked, e.selectedIndex, e.disabled, e.readOnly])]);
  // 元素指纹:身份、角色、名字、状态,外加所在区块的文字(列表行 / 表单 / 对话框)
  cache.guard = e => {
    if (!e?.isConnected || !visible(e)) return null;
    const scope = e.closest('form,dialog,[role="dialog"],article,li,tr,[role="row"]') || e.parentElement;
    return JSON.stringify([identity(e), role(e), name(e), e.value??null, e.checked??null, e.selectedIndex??null,
      e.readOnly??null, e.matches(':disabled'), e.getAttribute('aria-disabled'),
      e.getAttribute('aria-expanded'), e.getAttribute('aria-checked'), e.getAttribute('aria-selected'),
      e.getAttribute('href'), scope?.innerText?.slice(0, 2000)||'']);
  };
  const actions = [];
  for (const e of document.querySelectorAll(selector)) {
    if (!safe(e) || !visible(e) || e.matches(':disabled') || e.closest('[aria-disabled="true"]')) continue;
    const r = e.getBoundingClientRect(), x = r.x + r.width/2, y = r.y + r.height/2, rname = role(e);
    if (!rname || r.width<=0 || r.height<=0 || x<0 || y<0 || x>=innerWidth || y>=innerHeight) continue;
    if (rname==='gridcell' && e.querySelector('button,[role="button"]')) continue;
    const base = {node:identity(e), role:rname, label:(name(e)||rname).replace(/\s+/g,' ').trim().slice(0,120)};
    for (const key of ['checked','selected','expanded']) {
      const v = e.getAttribute('aria-'+key);
      if (v!==null) base[key] = v;
    }
    if (['checkbox','radio'].includes(e.type)) base.checked = String(e.checked);
    if (e.tagName==='SELECT') {
      const current = [...e.selectedOptions].map(o => o.label).join(', ');
      let n = 0;
      for (const o of e.options) {
        if (o.selected || o.disabled || o.closest('optgroup[disabled]')) continue;
        if (++n > optionCap) break;
        actions.push({...base, kind:'select', value:o.value, current, option:o.label});
      }
    } else {
      const editable = !e.readOnly && e.getAttribute('aria-readonly')!=='true' &&
        (['textbox','searchbox','spinbutton'].includes(rname) ||
          (rname==='combobox' && ['INPUT','TEXTAREA'].includes(e.tagName)));
      const value = 'value' in e ? String(e.value) :
        e.isContentEditable || rname==='combobox' ? e.innerText.trim() : '';
      actions.push({...base, kind:editable ? 'fill' : 'click', value:value.slice(0,80)});
    }
  }
  const words = [], walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  const range = document.createRange(); let node, length = 0;
  while ((node = walker.nextNode()) && length < textChars) {
    const value = node.textContent.replace(/\s+/g,' ').trim(), parent = node.parentElement;
    if (!value || !parent || parent.closest('script,style,noscript,template') || !visible(parent)) continue;
    range.selectNodeContents(node); const r = range.getBoundingClientRect();
    if (r.width>0 && r.height>0 && r.bottom>0 && r.top<innerHeight && r.right>0 && r.left<innerWidth) {
      words.push(value); length += value.length + 1;
    }
  }
  const omitted = Math.max(0, actions.length - limit);
  actions.splice(limit);
  cache.snapKey = cache.pageKey();
  cache.snapGuards = {};
  for (const a of actions) if (!(a.node in cache.snapGuards)) cache.snapGuards[a.node] = cache.guard(cache.nodes.get(a.node));
  const height = document.documentElement.scrollHeight;
  const frames = document.querySelectorAll('iframe').length;
  return {url:location.href, title:document.title, w:innerWidth, h:innerHeight,
    scroll:{y:Math.round(scrollY), height}, text:words.join(' | ').slice(0, textChars),
    actions, omitted, frames};
})
"""

# 执行前现查一遍:页面没变、元素还在且可见可用、点得到(没被盖住);select 直接改值并发事件。
_RESOLVE_JS = r"""
((a, strict) => {
  const c = window.__myclawEls;
  const e = c?.nodes.get(a.node);
  if (!c || !e) return {error:'gone'};
  if (strict && c.pageKey() !== c.snapKey) return {error:'page_changed'};
  if (c.guard(e) !== c.snapGuards?.[a.node]) return {error:'element_changed'};
  if (!e.isConnected || e.matches(':disabled') || e.closest('[aria-disabled="true"],[inert]') ||
      !e.checkVisibility({checkOpacity:true, checkVisibilityCSS:true})) return {error:'gone'};
  if (a.kind==='fill' && (e.readOnly || e.getAttribute('aria-readonly')==='true')) return {error:'readonly'};
  e.scrollIntoView?.({block:'nearest', inline:'nearest'});
  const r = e.getBoundingClientRect(), x = r.x + r.width/2, y = r.y + r.height/2;
  if (!r.width || !r.height || x<0 || y<0 || x>=innerWidth || y>=innerHeight) return {error:'offscreen'};
  const hit = document.elementFromPoint(x, y);
  if (!e.contains(hit) && !hit?.contains?.(e)) return {error:'covered', by:(hit?.tagName||'')+' '+(hit?.innerText||'').slice(0,40)};
  if (a.kind==='select') {
    if (e.tagName!=='SELECT' || ![...e.options].some(o => o.value===a.value && !o.disabled)) return {error:'element_changed'};
    e.value = a.value;
    e.dispatchEvent(new Event('input', {bubbles:true}));
    e.dispatchEvent(new Event('change', {bubbles:true}));
  }
  return {x, y, combobox: e.getAttribute('role')==='combobox'};
})
"""

# 自己刚做完的那一下引起的变化(填了值、勾了框)记为已知,只更新指纹,不重新编号 ——
# 否则同一批里第二次填写会被当成「页面变了」拒掉(2026-10-07 C9:批量填表第二个框被拒)。
# 页面自己的变化(弹窗、异步加载)之后照样会被查出来。
_ACCEPT_JS = r"""
((node) => {
  const c = window.__myclawEls;
  if (!c) return;
  c.snapKey = c.pageKey();
  const e = c.nodes.get(node);
  if (e && c.snapGuards) c.snapGuards[node] = c.guard(e);
})
"""

_last = None  # 最近一次 elements() 的结果:编号 → 动作


def _fmt_action(i, a):
    label = a["label"]
    if a["kind"] == "select":
        return f'[{i}] select   {label} → "{a["option"]}"   (now: {a.get("current") or "-"})'
    state = []
    if a.get("checked") is not None:
        state.append("checked" if a["checked"] == "true" else "unchecked")
    if a.get("expanded") is not None:
        state.append("expanded" if a["expanded"] == "true" else "collapsed")
    if a.get("selected") == "true":
        state.append("selected")
    if a["kind"] == "fill":
        state.append(f'value="{a.get("value", "")}"')
    tail = f"   ({', '.join(state)})" if state else ""
    return f"[{i}] {a['role']:<9} {label}{tail}"


def elements(limit=150, text_chars=1500, option_cap=12):
    """Read the current tab as a numbered table of visible interactive elements plus the
    visible text, and print it. Then operate by number with act(n, ...).
    Cheaper and more reliable than screenshots for finding and clicking things."""
    global _last
    st = _h.js(f"{_SNAPSHOT_JS}({int(limit)}, {int(text_chars)}, {int(option_cap)})")
    if not st:
        print("The page is still loading (no document yet). Call wait_for_load() and elements() again.")
        _last = None
        return
    _last = st
    acts = st["actions"]
    lines = [f"Page: {st['title']} — {st['url']}",
             f"Scroll: {st['scroll']['y']} / {st['scroll']['height']}px, viewport {st['w']}x{st['h']}"
             + ("  (more below: act('down'))" if st["scroll"]["y"] + st["h"] < st["scroll"]["height"] - 2 else "")]
    lines.append(f"Elements ({len(acts)} visible" + (f", {st['omitted']} more not listed" if st["omitted"] else "") + "):")
    lines += [_fmt_action(i + 1, a) for i, a in enumerate(acts)]
    if st.get("frames"):
        lines.append(f"Note: {st['frames']} iframe(s) on the page are not listed; use iframe_target()/js(target_id=...) for those.")
    lines.append("Visible text: " + (st["text"] or "(none)"))
    print("\n".join(lines))


def act(n, text=None, enter=False, observe=True):
    """Operate on element [n] from the latest elements() table:
    - click it: act(n)
    - type into a text field (replaces its content): act(n, "text"); add enter=True to press Enter after
    - choose a dropdown option: act(n) on the `select … → "option"` row
    - scroll the page: act("down") / act("up")
    Before acting it checks the page and the element are unchanged and not covered; if not,
    it says so and you should call elements() again. Afterwards it prints the new table."""
    if n in ("down", "up"):
        # 让页面自己滚,不发鼠标滚轮:headless 下标签页没被合成过时,滚轮事件会一直等不到回执
        # (2026-10-07 C9 实测:Input.dispatchMouseEvent mouseWheel 卡满 5 秒超时)
        moved = _h.js(f"(() => {{ const y = scrollY; scrollBy({{top: {1 if n == 'down' else -1} * Math.round(innerHeight * 0.8)}}); "
                      "return Math.round(scrollY - y); })()")
        time.sleep(0.15)
        print(f"Scrolled {n} by {abs(moved or 0)}px." if moved else f"Already at the {'bottom' if n == 'down' else 'top'}.")
        if observe:
            elements()
        return True
    if not _last:
        print("Call elements() first.")
        return False
    acts = _last["actions"]
    try:
        a = acts[int(n) - 1]
    except (ValueError, IndexError):
        print(f"No element [{n}] in the latest table (1..{len(acts)}). Call elements() again.")
        return False
    if a["kind"] == "fill" and text is None:
        a = {**a, "kind": "click"}  # 没给文字就是点它(比如点开日期选择器)
    if text is not None and a["kind"] != "fill":
        print(f"[{n}] is a {a['role']}, not a text field; to type into it, pick a textbox row.")
        return False
    t = _h.js(f"{_RESOLVE_JS}({json.dumps(a)}, {'true' if a['kind'] == 'fill' else 'false'})")
    if not t or t.get("error"):
        why = (t or {}).get("error", "unknown")
        msg = {"page_changed": "the page changed since elements()",
               "element_changed": "that element changed since elements()",
               "gone": "that element is gone or hidden now",
               "offscreen": "that element is outside the viewport",
               "readonly": "that field is read-only",
               "covered": f"that element is covered by {(t or {}).get('by', 'another element')!r}"}.get(why, why)
        print(f"Not done: {msg}. Call elements() and pick again.")
        return False
    url_before = _last["url"]
    if a["kind"] != "select":
        _h.click_at_xy(t["x"], t["y"])
    if a["kind"] == "fill":
        _h.cdp("Input.dispatchKeyEvent", type="keyDown", key="a", code="KeyA", modifiers=2, commands=["selectAll"])
        _h.cdp("Input.dispatchKeyEvent", type="keyUp", key="a", code="KeyA", modifiers=2)
        _h.cdp("Input.insertText", text=str(text))
        if enter:
            _h.press_key("Enter")
    try:
        _h.js(f"{_ACCEPT_JS}({int(a['node'])})")
    except Exception:
        pass  # 点了之后换页了:旧页面已经没了,下次 elements() 会重建
    verb = {"click": "clicked", "fill": "typed into", "select": f'selected "{a.get("option")}" in'}[a["kind"]]
    print(f"Done: {verb} [{n}] {a['label']}" + (" and pressed Enter" if enter else ""))
    # 等页面有用的变化:换页就等加载完;下拉框输入后给联想留一点时间;其余等两帧
    time.sleep(0.25 if t.get("combobox") or a["kind"] == "fill" else 0.08)
    ready = None
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            ready = _h.js("[document.readyState, location.href]")
        except Exception:
            ready = None
        if ready and ready[0] == "complete":
            break
        time.sleep(0.1)
    if observe:
        if ready and ready[1] != url_before:
            print(f"(navigated to {ready[1]})")
        elements()
    return True
