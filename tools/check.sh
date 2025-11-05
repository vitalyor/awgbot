#!/usr/bin/env bash
# AWGBOT quick check v1.3 (owner/mode checks for secret.env)

set -u -o pipefail

# ===== colors =====
RED="\033[31m"; YEL="\033[33m"; GRN="\033[32m"; CYA="\033[36m"; DIM="\033[2m"; RST="\033[0m"

ok=0; warn=0; bad=0
LOG_LEVEL_DEFAULT="notice"
ROOT="/opt/awgbot"
SECRET="$ROOT/secret.env"
COMPOSE="docker compose"
AWGBOT_SVC="awgbot"
PROXY_SVC="docker-proxy"

FULL=0
[[ "${1:-}" == "--full" ]] && FULL=1

say() { printf "%b\n" "$*"; }
good(){ ((ok++));   say "${GRN}✔${RST} $*"; }
mid(){  ((warn++)); say "${YEL}▲${RST} $*"; }
bad(){  ((bad++));  say "${RED}✖${RST} $*"; }
hr(){ say "${DIM}────────────────────────────────────────────────────────${RST}"; }

summary(){
  local total=$((ok+warn+bad))
  say
  if (( bad > 0 )); then
    say "${RED}❌ Есть ошибки:${RST} ${bad}; предупреждений: ${warn}; всего проверок: ${total}"
    return 1
  elif (( warn > 0 )); then
    say "${YEL}⚠️  Есть предупреждения:${RST} ${warn}; ошибок: 0; всего проверок: ${total}"
    return 0
  else
    say "${GRN}✅ Всё в порядке.${RST} Всего проверок: ${total}"
    return 0
  fi
}

# robust getenv with default (.env last assignment)
getenv_default() {
  local key="$1" def="$2" val=""
  if [[ -r .env ]]; then
    val="$(awk -F= -v k="$key" '($1==k){print $2}' .env 2>/dev/null | tail -n1)"
  fi
  if [[ -n "${val// }" ]]; then echo "$val"; else echo "$def"; fi
}

# ===== preflight =====
cd "$ROOT" 2>/dev/null || { echo "Нет каталога $ROOT"; exit 1; }

hr; say "${CYA}AWGBOT quick check$( ((FULL)) && echo ' (full)')${RST}"
say "$(date -Iseconds)"
hr

# 1) compose ok
if $COMPOSE ps >/dev/null 2>&1; then
  good "docker compose доступен"
else
  bad  "docker compose не работает"; summary; exit 1
fi

# 2) services up
psout="$($COMPOSE ps 2>/dev/null || true)"
if echo "$psout" | grep -qE "^$AWGBOT_SVC[[:space:]]"; then :; else
  bad "сервис $AWGBOT_SVC не найден"; summary; exit 1
fi
if echo "$psout" | grep -qE "^$PROXY_SVC[[:space:]]"; then :; else
  bad "сервис $PROXY_SVC не найден"; summary; exit 1
fi

echo "$psout" | awk 'NR==1; /awgbot|docker-proxy/ {print}' | sed 's/^/   /'

if echo "$psout" | grep -E "^$AWGBOT_SVC" | grep -qi healthy; then
  good "awgbot healthy"
else
  mid "awgbot не в состоянии healthy"
fi
if echo "$psout" | grep -E "^$PROXY_SVC" >/dev/null; then
  good "docker-proxy запущен"
else
  bad "docker-proxy не запущен"
fi

# 3) secret.env checks (exist/owner/mode/keys)
if [[ -r "$SECRET" ]]; then
  good "secret.env найден"
  mode="$(stat -c '%a' "$SECRET" 2>/dev/null || echo "?")"
  uid="$(stat -c '%u' "$SECRET" 2>/dev/null || echo "?")"
  gid="$(stat -c '%g' "$SECRET" 2>/dev/null || echo "?")"
  uuser="$(stat -c '%U' "$SECRET" 2>/dev/null || echo "?")"
  ggroup="$(stat -c '%G' "$SECRET" 2>/dev/null || echo "?")"

  # узнаём uid/gid пользователя ПРОЦЕССА внутри контейнера
  BOT_UID="$($COMPOSE exec -T "$AWGBOT_SVC" sh -lc 'id -u' 2>/dev/null || echo "")"
  BOT_GID="$($COMPOSE exec -T "$AWGBOT_SVC" sh -lc 'id -g' 2>/dev/null || echo "")"

  say "   secret.env: ${DIM}mode=$mode owner=$uuser($uid):$ggroup($gid) container_uid/gid=${BOT_UID:-?}:${BOT_GID:-?}${RST}"

  # анализ доступа:
  # режимы, при которых бот гарантированно прочитает файл:
  #  - uid совпадает с BOT_UID и mode позволяет owner read (600/640/644)
  #  - либо gid совпадает с BOT_GID и mode позволяет group read (640/644)
  #  - либо world-readable (644) — читается, но небезопасно
  readable=0
  secure_hint=""
  if [[ -n "$BOT_UID" && "$uid" == "$BOT_UID" ]]; then
    # владелец == bot user
    case "$mode" in
      600|640|644) readable=1 ;;
    esac
    # рекомендуем максимально закрытый вариант
    [[ "$mode" != "600" ]] && secure_hint="рекомендуется chmod 600 $SECRET"
  elif [[ -n "$BOT_GID" && "$gid" == "$BOT_GID" ]]; then
    # группа == bot group
    case "$mode" in
      640|644) readable=1 ;;
    esac
    [[ "$mode" != "640" ]] && secure_hint="для group-read используйте chmod 640 $SECRET"
  else
    # ни владельцем, ни группой контейнера файл не совпадает
    case "$mode" in
      644) readable=1 ;; # всем читаем — работает, но небезопасно
    esac
  fi

  if (( readable )); then
    good "secret.env читается контейнером"
    if [[ -n "$secure_hint" ]]; then
      mid "безопасность: $secure_hint"
    fi
  else
    bad "secret.env НЕ читается контейнером (uid/gid или права не подходят)"
    if [[ -n "$BOT_UID" && -n "$BOT_GID" ]]; then
      say "   ➤ Исправить: ${DIM}chown ${BOT_UID}:${BOT_GID} $SECRET && chmod 600 $SECRET${RST}"
    else
      say "   ➤ Исправить: ${DIM}подстройте владельца/права под пользователя контейнера (обычно uid/gid 10001)${RST}"
    fi
  fi

  # ключи
  grep -Eq '^TELEGRAM_TOKEN=' "$SECRET" && good "TELEGRAM_TOKEN присутствует" || bad "нет TELEGRAM_TOKEN"
  grep -Eq '^ADMIN_IDS=' "$SECRET"      && good "ADMIN_IDS присутствует"      || bad "нет ADMIN_IDS"
else
  bad "secret.env отсутствует или не читается: $SECRET"
fi

# 4) secret.env смонтирован в контейнер
if $COMPOSE exec -T "$AWGBOT_SVC" sh -lc 'test -r /run/secrets/secret.env'; then
  good "secret.env смонтирован в контейнер"
else
  bad  "secret.env не смонтирован внутри контейнера"
fi

# 5) heartbeat / write test
hb="$($COMPOSE exec -T "$AWGBOT_SVC" sh -lc 'python - <<PY
import os,time,sys
p="/app/data/heartbeat"
try:
  age=time.time()-os.path.getmtime(p)
  print(int(age))
except Exception:
  print(-1)
PY' 2>/dev/null || echo -1)"
if [[ "$hb" =~ ^[0-9]+$ && "$hb" -ge 0 && "$hb" -lt 120 ]]; then
  good "heartbeat OK (${hb}s)"
elif [[ "$hb" =~ ^[0-9]+$ && "$hb" -ge 0 ]]; then
  mid  "heartbeat старый (${hb}s)"
else
  bad  "heartbeat не найден"
fi

if $COMPOSE exec -T "$AWGBOT_SVC" sh -lc 'p=/app/data/.wtest; echo ok >"$p" && rm -f "$p"' >/dev/null 2>&1; then
  good "/app/data доступна для записи"
else
  bad  "/app/data недоступна для записи"
fi

# 6) proxy env & daemon
if $COMPOSE exec -T "$AWGBOT_SVC" sh -lc 'echo "$DOCKER_HOST"' | grep -q 'tcp://docker-proxy:2375'; then
  good "DOCKER_HOST=tcp://docker-proxy:2375"
else
  bad  "DOCKER_HOST не установлен на tcp://docker-proxy:2375"
fi

ver="$($COMPOSE exec -T "$AWGBOT_SVC" sh -lc "docker version --format '{{.Server.Version}}'" 2>/dev/null || true)"
if [[ -n "$ver" ]]; then
  good "docker daemon доступен через прокси (v$ver)"
else
  bad  "docker daemon через прокси недоступен"
fi

# 7) контейнеры и конфиги
mapfile -t lines < <($COMPOSE exec -T "$AWGBOT_SVC" sh -lc "docker ps --format '{{.Names}}\t{{.Status}}'" 2>/dev/null || true)
declare -A st; for l in "${lines[@]:-}"; do n="${l%%	*}"; s="${l#*	}"; [[ -n "$n" ]] && st["$n"]="$s"; done

AWG_NAME="$(getenv_default AWG_CONTAINER amnezia-awg)"
XRAY_NAME="$(getenv_default XRAY_CONTAINER amnezia-xray)"
DNS_NAME="$(getenv_default DNS_CONTAINER amnezia-dns)"

check_cont(){
  local name="$1"
  [[ -z "${name// }" ]] && return 0
  local status="${st[$name]:-}"
  local low="${status,,}"
  if [[ -z "$status" ]]; then bad  "$name — не запущен"; return; fi
  if [[ "$low" == *unhealthy* || "$low" == *restarting* ]]; then mid "$name — $status"; else
    if [[ "$low" == *up* || "$low" == *healthy* ]]; then good "$name — $status"; else mid "$name — $status"; fi
  fi
}
check_cont "$AWG_NAME"
check_cont "$XRAY_NAME"
check_cont "$DNS_NAME"
check_cont "awgbot"

XRAY_CFG="$(getenv_default XRAY_CONFIG_PATH /opt/amnezia/xray/server.json)"
AWG_CFG="$(getenv_default AWG_CONFIG_PATH  /opt/amnezia/awg/wg0.conf)"

if $COMPOSE exec -T "$AWGBOT_SVC" sh -lc "docker exec '$XRAY_NAME' sh -lc 'test -r $XRAY_CFG'"; then
  good "XRay конфиг доступен в $XRAY_NAME"
else
  bad  "XRay конфиг НЕ доступен в $XRAY_NAME ($XRAY_CFG)"
fi
if $COMPOSE exec -T "$AWGBOT_SVC" sh -lc "docker exec '$AWG_NAME' sh -lc 'test -r $AWG_CFG'"; then
  good "AmneziaWG конфиг доступен в $AWG_NAME"
else
  bad  "AmneziaWG конфиг НЕ доступен в $AWG_NAME ($AWG_CFG)"
fi

# 8) proxy log level
LVL="$(getenv_default DOCKER_PROXY_LOG_LEVEL "$LOG_LEVEL_DEFAULT")"
say "   docker-proxy log level: ${DIM}${LVL}${RST}"

# 9) full: stats & disk
if (( FULL )); then
  hr
  say "${CYA}📊 Ресурсы (docker stats)${RST}"
  $COMPOSE exec -T "$AWGBOT_SVC" sh -lc 'docker stats --no-stream --format "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"' \
    | awk -F'\t' '{printf "   • %-20s CPU %-8s MEM %-22s (%s)\n",$1,$2,$3,$4}'
  hr
  say "${CYA}💽 Файловая система (/app/data)${RST}"
  $COMPOSE exec -T "$AWGBOT_SVC" sh -lc 'df -h /app/data | tail -n 1 | awk "{print \"   • размер: \" \$2 \"; занято: \" \$3 \"; свободно: \" \$4 \" (\" \$5 \")\"}"'
fi

hr
summary
exit $?