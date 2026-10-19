#!/bin/bash
# MyClaw fork ext (letscubo) —— 把浏览器起起来就退出(编进 codeg,commands/myclaw_browser.rs 调用:
# 内置浏览器工具与浏览器悬浮窗共用)。
#
# 由 codeg 起、脚本随即退出:浏览器被 init 收养,不在任何 agent 运行时的进程树里。
# openclaw 2026.10.5 每轮结束回收 MCP 并清掉它的整棵子孙进程(ownProcessTree),浏览器若由
# 某个 MCP 拉起就每轮被杀(麻豆传媒 2026-10-19:12 次 MCP 启动对应 12 次浏览器启动)。
#
# 目录约定与浏览器应用的启动脚本(MyClaw 仓库 apps/myclaw-browser/skills/myclaw-browser/bin/browser-mcp.sh)
# 一致,老版本 codeg 的实例还在用它:
#   ~/.myclaw-browser/<key>/{port,profile,window-size,display,chrome.log}、全实例锁 .launch.lock、
#   端口 9230–9330、.no-sandbox 标记、短临时目录 /tmp/mcb-<摘要>。
# 内置浏览器一个实例一份,key 固定为 `instance`(见 myclaw_browser::BROWSER_KEY)。
#
# 用法:MYCLAW_AGENT_ID=<key> bash -c "$script" myclaw-browser-launch
# 成功:stdout 打印端口、退出 0;内存不够退出 3;其余失败退出 1,原因在 stderr 最后一行。
set -u

if [ -z "${HOME:-}" ]; then
  HOME=$(getent passwd "$(id -un)" 2>/dev/null | cut -d: -f6)
  [ -n "$HOME" ] || HOME=/home/ubuntu
fi
export HOME
export PATH="${PATH:-}:/usr/local/bin:/usr/bin:/bin"

AGENT="${MYCLAW_AGENT_ID:-}"
case "$AGENT" in ''|*[!A-Za-z0-9_-]*) echo "invalid agent id" >&2; exit 1 ;; esac
BASE="$HOME/.myclaw-browser/$AGENT"
PROF="$BASE/profile"
PORTFILE="$BASE/port"
LOG="$BASE/chrome.log"
mkdir -p "$PROF" "$BASE"

CHROME="${MYCLAW_CHROME:-}"
if [ -z "$CHROME" ]; then
  for c in "${PLAYWRIGHT_BROWSERS_PATH:-/opt/pw-browsers}"/chromium-*/chrome-linux64/chrome \
           /opt/pw-browsers/chromium-*/chrome-linux64/chrome \
           /usr/bin/google-chrome-stable /usr/bin/chromium; do
    [ -x "$c" ] && CHROME="$c" && break
  done
fi
if [ -z "$CHROME" ] || [ ! -x "$CHROME" ]; then
  echo "no chromium found on this instance" >&2
  exit 1
fi

alive() { curl -sf -m 2 "http://127.0.0.1:$1/json/version" >/dev/null 2>&1; }

# 可用内存(MB):系统视角与 cgroup 上限视角取小的
avail_mb() {
  local a c m
  a=$(awk '/^MemAvailable:/ {print int($2/1024)}' /proc/meminfo 2>/dev/null)
  m=$(cat /sys/fs/cgroup/memory.max 2>/dev/null)
  c=$(cat /sys/fs/cgroup/memory.current 2>/dev/null)
  case "$m$c" in *[!0-9]*|'') echo "${a:-0}"; return ;; esac
  c=$(( (m - c) / 1048576 ))
  [ -n "$a" ] && [ "$a" -lt "$c" ] && c=$a
  echo "$c"
}
MIN_MB="${MYCLAW_BROWSER_MIN_MEM_MB:-350}"

# 浏览器常驻,不能继承会话的临时目录(会话结束被删后它一建共享内存就崩);路径要短
# (SingletonSocket 的 Unix socket 路径上限约 108 字节)
BTMP="/tmp/mcb-$(printf %s "$AGENT" | md5sum | cut -c1-12)"
mkdir -p "$BTMP" && chmod 700 "$BTMP"

NOSANDBOX_MARK="$HOME/.myclaw-browser/.no-sandbox"
SANDBOX_FLAG=""
[ -f "$NOSANDBOX_MARK" ] && SANDBOX_FLAG="--no-sandbox"

WINSIZE_FILE="$BASE/window-size"
window_flag() {
  local ws
  ws=$(tr -d ' \n' 2>/dev/null <"$WINSIZE_FILE") || ws=""
  if [[ "$ws" =~ ^([0-9]{3,4}),([0-9]{3,4})$ ]] && [ "${BASH_REMATCH[1]}" -le 1400 ] && [ "${BASH_REMATCH[2]}" -le 1000 ]; then
    echo "--window-size=$ws"
  elif [ -n "${DISPLAY_FLAG:-}" ]; then
    # 有头时没记过大小:给一个装得下 1280×800 可视区的窗口(无头的默认窗口只有 800×600 左右)
    echo "--window-size=1280,900"
  fi
}

# 有头模式(2026-10-19):无头 chromium 的 UA 带 HeadlessChrome,Cloudflare Turnstile 这类人机
# 验证一直过不去(麻豆传媒实例上 claude.ai 卡在 "Just a moment";同机有头对照能进登录页)。
# 有 Xvfb 就给浏览器开一块虚拟屏幕、以有头方式运行;没有 Xvfb 才退回 --headless=new。
# 画面与接管照旧走 CDP(screencast / Input.*),与有头无头无关。
# 屏幕和浏览器一样 setsid 脱离启动它的进程;显示号记在 $BASE/display,下次复用。
XVFB=$(command -v Xvfb 2>/dev/null || true)
DISPLAY_FILE="$BASE/display"
display_alive() {
  [ -n "$1" ] && [ -S "/tmp/.X11-unix/X$1" ] && [ -f "/tmp/.X$1-lock" ] \
    && kill -0 "$(tr -d ' \n' <"/tmp/.X$1-lock" 2>/dev/null)" 2>/dev/null
}
ensure_display() {
  [ -n "$XVFB" ] || return 1
  local n
  n=$(tr -cd '0-9' 2>/dev/null <"$DISPLAY_FILE")
  display_alive "$n" && { echo "$n"; return 0; }
  for n in $(seq 99 160); do
    [ -e "/tmp/.X$n-lock" ] && continue
    setsid "$XVFB" ":$n" -screen 0 1440x1050x24 -nolisten tcp >>"$LOG" 2>&1 </dev/null 8>&- &
    for _ in $(seq 1 20); do
      sleep 0.25
      display_alive "$n" && { echo "$n" >"$DISPLAY_FILE"; echo "$n"; return 0; }
    done
  done
  return 1
}

launch() {
  local from dnum
  local mode=(--headless=new)
  from=$(wc -l 2>/dev/null <"$LOG" || echo 0)
  DISPLAY_FLAG=""
  if dnum=$(ensure_display); then
    DISPLAY_FLAG=":$dnum"
    mode=()
  fi
  # 有头:去掉 --headless,DISPLAY 指到虚拟屏幕
  DISPLAY="${DISPLAY_FLAG:-${DISPLAY:-}}" TMPDIR="$BTMP" setsid "$CHROME" "${mode[@]}" --remote-debugging-port="$1" \
    --remote-debugging-address=127.0.0.1 --user-data-dir="$PROF" \
    --no-first-run --no-default-browser-check --disable-gpu --disable-dev-shm-usage --disable-infobars \
    --window-position=0,0 --password-store=basic \
    $SANDBOX_FLAG $(window_flag) about:blank >>"$LOG" 2>&1 </dev/null 8>&- &
  # ↑ 8>&- 不能省:chromium 常驻,继承了锁句柄就永远不放,下一次启动会卡死在 flock
  for _ in $(seq 1 25); do
    sleep 1
    alive "$1" && return 0
    if [ -z "$SANDBOX_FLAG" ] && tail -n +"$((from + 1))" "$LOG" 2>/dev/null | grep -q "No usable sandbox"; then
      echo "=== $(date -Is) 这个环境没有可用的沙箱,改用 --no-sandbox ===" >>"$LOG"
      touch "$NOSANDBOX_MARK"
      SANDBOX_FLAG="--no-sandbox"
      launch "$1"
      return $?
    fi
  done
  return 1
}

# 挑端口到浏览器起来这一段持全实例的锁(与应用脚本同一把):各 agent 挑的是同一个号段
exec 8>"$HOME/.myclaw-browser/.launch.lock"
flock 8

PORT=""
REUSE=""
if [ -f "$PORTFILE" ]; then
  P=$(cat "$PORTFILE" 2>/dev/null)
  case "$P" in ''|*[!0-9]*) P="" ;; esac
  [ -n "$P" ] && alive "$P" && PORT="$P"
  # 记过的端口没人占就沿用:同一个 agent 尽量不换号
  if [ -z "$PORT" ] && [ -n "$P" ] && [ "$P" -ge 9230 ] && [ "$P" -le 9330 ] \
     && ! (exec 3<>/dev/tcp/127.0.0.1/"$P") 2>/dev/null; then
    REUSE="$P"
  fi
fi

if [ -z "$PORT" ]; then
  FREE=$(avail_mb)
  if [ "$FREE" -lt "$MIN_MB" ]; then
    echo "not enough memory to start a browser: ${FREE} MB available, ${MIN_MB} MB needed" >&2
    exit 3
  fi
  PORT="$REUSE"
  if [ -z "$PORT" ]; then
    # 9222 留给手工调试;别的 agent 登记过的号也跳过(它的浏览器可能正好死了、端口空着)
    for p in $(seq 9230 9330); do
      grep -qsx "$p" "$HOME"/.myclaw-browser/*/port && continue
      if ! alive "$p" && ! (exec 3<>/dev/tcp/127.0.0.1/"$p") 2>/dev/null; then PORT="$p"; break; fi
    done
  fi
  [ -z "$PORT" ] && { echo "no free debugging port" >&2; exit 1; }
  echo "$PORT" >"$PORTFILE"
  launch "$PORT" || { echo "chromium did not come up on $PORT; see $LOG" >&2; exit 1; }
fi
exec 8>&-

echo "$PORT"
