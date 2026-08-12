#!/bin/sh
set -eu
umask 077

source_config=/opt/mihomo/source.yaml
source_binary=/opt/mihomo/clash
runtime_dir=/tmp/mihomo
runtime_config=${runtime_dir}/config.yaml
chain_group=${MIHOMO_CHAIN_GROUP:-链式代理}
mixed_port=${MIHOMO_MIXED_PORT:-17890}

test -r "${source_config}"
test -x "${source_binary}"
printf '%s\n' "${chain_group}" | grep -Eq "^[^,'#[:cntrl:]]+$"
case "${mixed_port}" in
  ''|*[!0-9]*) exit 64 ;;
esac
[ "${mixed_port}" -ge 1 ] && [ "${mixed_port}" -le 65535 ]

for required_key in mixed-port allow-lan bind-address mode external-controller rules; do
  [ "$(grep -c "^${required_key}:" "${source_config}")" -eq 1 ]
done
grep -F "name: '${chain_group}'" "${source_config}" >/dev/null
grep -F "name: '固定出口-US'" "${source_config}" >/dev/null
grep -F "dialer-proxy: TIGR" "${source_config}" >/dev/null

mkdir -p "${runtime_dir}"
chmod 700 "${runtime_dir}"
cp /opt/mihomo/geoip.metadb "${runtime_dir}/geoip.metadb"

# Keep the host proxy definitions, groups, and complete Rule-mode policy.
# The domestic relay is forced DIRECT. Official OpenAI/Codex and
# Anthropic/Claude endpoints are forced through the fixed residential exit.
# These exact overrides precede GeoIP and generic fallback rules.
awk -v chain_group="${chain_group}" -v mixed_port="${mixed_port}" '
  BEGIN { found_rules = 0 }
  /^mixed-port:/ {
    print "mixed-port: " mixed_port
    next
  }
  /^allow-lan:/ {
    print "allow-lan: true"
    next
  }
  /^bind-address:/ {
    print "bind-address: 0.0.0.0"
    next
  }
  /^mode:/ {
    print "mode: rule"
    next
  }
  /^external-controller:/ {
    print "external-controller: 127.0.0.1:19090"
    next
  }
  /^rules:[[:space:]]*$/ {
    print "rules:"
    print "- '\''DOMAIN,sub2api.ziplab.co,DIRECT'\''"
    print "- '\''DOMAIN-SUFFIX,openai.com," chain_group "'\''"
    print "- '\''DOMAIN-SUFFIX,chatgpt.com," chain_group "'\''"
    print "- '\''DOMAIN-SUFFIX,oaistatic.com," chain_group "'\''"
    print "- '\''DOMAIN-SUFFIX,oaiusercontent.com," chain_group "'\''"
    print "- '\''DOMAIN-SUFFIX,anthropic.com," chain_group "'\''"
    print "- '\''DOMAIN-SUFFIX,claude.ai," chain_group "'\''"
    print "- '\''DOMAIN-SUFFIX,claude.com," chain_group "'\''"
    found_rules = 1
    next
  }
  { print }
  END {
    if (!found_rules) {
      exit 42
    }
  }
' "${source_config}" > "${runtime_config}"

grep -qx "mixed-port: ${mixed_port}" "${runtime_config}"
grep -qx "allow-lan: true" "${runtime_config}"
grep -qx "bind-address: 0.0.0.0" "${runtime_config}"
grep -qx "mode: rule" "${runtime_config}"
grep -qx "external-controller: 127.0.0.1:19090" "${runtime_config}"
[ "$(grep -c '^rules:$' "${runtime_config}")" -eq 1 ]
source_rule_count=$(awk 'found && /^-/ {count++} /^rules:[[:space:]]*$/ {found=1} END {print count+0}' "${source_config}")
runtime_rule_count=$(awk 'found && /^-/ {count++} /^rules:[[:space:]]*$/ {found=1} END {print count+0}' "${runtime_config}")
[ "${source_rule_count}" -gt 0 ]
[ "${runtime_rule_count}" -eq "$((source_rule_count + 8))" ]
first_runtime_rule=$(awk 'found && /^-/ {print; exit} /^rules:[[:space:]]*$/ {found=1}' "${runtime_config}")
[ "${first_runtime_rule}" = "- 'DOMAIN,sub2api.ziplab.co,DIRECT'" ]
grep -Fqx -- "- 'DOMAIN,sub2api.ziplab.co,DIRECT'" "${runtime_config}"
for forced_domain in openai.com chatgpt.com oaistatic.com oaiusercontent.com anthropic.com claude.ai claude.com; do
  grep -Fqx -- "- 'DOMAIN-SUFFIX,${forced_domain},${chain_group}'" "${runtime_config}"
done
if ! "${source_binary}" -t -d "${runtime_dir}" -f "${runtime_config}" >/dev/null 2>&1; then
  echo 'Mihomo runtime configuration validation failed' >&2
  exit 78
fi

exec "${source_binary}" -d "${runtime_dir}" -f "${runtime_config}"
