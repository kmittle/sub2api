#!/bin/sh

set -eu

umask 077

runtime_dir=/run/mihomo
source_file=/opt/mihomo-source/tigr.yaml
clash_bin=/opt/mihomo/clash
candidate_file=${runtime_dir}/.fixed-proxy.source
proxy_file=${runtime_dir}/.fixed-proxy.direct
count_file=${runtime_dir}/.extract-counts
config_tmp=${runtime_dir}/.config.yaml.tmp
config_file=${runtime_dir}/config.yaml
test_log=${runtime_dir}/.config-test.log

fail() {
    printf '%s\n' "mihomo-entrypoint: $1" >&2
    exit 1
}

cleanup() {
    rm -f \
        "$candidate_file" \
        "$proxy_file" \
        "$count_file" \
        "$config_tmp" \
        "$test_log"
}

trap cleanup EXIT HUP INT TERM

[ "$(id -u)" = "1000" ] || fail "unexpected runtime uid"
[ "$(id -g)" = "1001" ] || fail "unexpected runtime gid"
[ -r "$source_file" ] || fail "proxy source is not readable"
[ -x "$clash_bin" ] || fail "mihomo binary is not executable"

mkdir -p "$runtime_dir"
chmod 0700 "$runtime_dir"
ln -sf /opt/mihomo-source/geoip.metadb "${runtime_dir}/GeoIP.metadb"

# Read only the top-level inline proxy list. No source or extracted value is
# written to stdout/stderr.
awk -v candidate="$candidate_file" -v counts="$count_file" '
    BEGIN {
        section_count = 0
        match_count = 0
        in_proxy_section = 0
    }

    /^proxies:[[:space:]]*$/ {
        section_count++
        in_proxy_section = 1
        next
    }

    in_proxy_section && /^[[:alnum:]_-]+:[[:space:]]*/ {
        in_proxy_section = 0
    }

    in_proxy_section && /^[[:space:]]*-[[:space:]]*\{.*\}[[:space:]]*$/ {
        if ($0 ~ /(^|[,{])[[:space:]]*name[[:space:]]*:[[:space:]]*["\047]?固定出口-US["\047]?[[:space:]]*(,|})/) {
            print $0 > candidate
            match_count++
        }
    }

    END {
        print section_count, match_count > counts
    }
' "$source_file" || fail "fixed proxy extraction failed"

read -r section_count match_count < "$count_file" || fail "fixed proxy count validation failed"
[ "$section_count" = "1" ] || fail "expected exactly one top-level proxy section"
[ "$match_count" = "1" ] || fail "expected exactly one fixed proxy entry"

# Validate the expected inline SOCKS5 schema without displaying any value.
awk '
    function key_count(key, rest, pattern, count) {
        rest = line
        pattern = "(^|[,{])[[:space:]]*" key "[[:space:]]*:"
        count = 0
        while (match(rest, pattern)) {
            count++
            rest = substr(rest, RSTART + RLENGTH)
        }
        return count
    }

    function empty_value(key, prefix) {
        prefix = "(^|[,{])[[:space:]]*" key "[[:space:]]*:[[:space:]]*"
        if (line ~ (prefix "(,|})"))
            return 1
        if (line ~ (prefix "\"\"[[:space:]]*(,|})"))
            return 1
        if (line ~ (prefix "\047\047[[:space:]]*(,|})"))
            return 1
        if (line ~ (prefix "null[[:space:]]*(,|})"))
            return 1
        if (line ~ (prefix "~[[:space:]]*(,|})"))
            return 1
        return 0
    }

    NR != 1 { exit 1 }

    {
        line = $0

        if (line !~ /^[[:space:]]*-[[:space:]]*\{.*\}[[:space:]]*$/)
            exit 1

        if (key_count("name") != 1 ||
            key_count("type") != 1 ||
            key_count("server") != 1 ||
            key_count("port") != 1 ||
            key_count("username") != 1 ||
            key_count("password") != 1 ||
            key_count("udp") != 1 ||
            key_count("dialer-proxy") != 1)
            exit 1

        if (line !~ /(^|[,{])[[:space:]]*name[[:space:]]*:[[:space:]]*["\047]?固定出口-US["\047]?[[:space:]]*(,|})/)
            exit 1
        if (line !~ /(^|[,{])[[:space:]]*type[[:space:]]*:[[:space:]]*["\047]?socks5["\047]?[[:space:]]*(,|})/)
            exit 1

        if (empty_value("server") ||
            empty_value("port") ||
            empty_value("username") ||
            empty_value("password") ||
            empty_value("dialer-proxy"))
            exit 1

        if (!match(line, /(^|[,{])[[:space:]]*port[[:space:]]*:[[:space:]]*[0-9]+[[:space:]]*(,|})/))
            exit 1
        port_field = substr(line, RSTART, RLENGTH)
        sub(/^.*port[[:space:]]*:[[:space:]]*/, "", port_field)
        sub(/[[:space:]]*(,|})$/, "", port_field)
        if ((port_field + 0) < 1 || (port_field + 0) > 65535)
            exit 1

        if (line !~ /,[[:space:]]*dialer-proxy[[:space:]]*:[[:space:]]*[^,}[:space:]][^,}]*[[:space:]]*}[[:space:]]*$/)
            exit 1
    }
' "$candidate_file" || fail "fixed proxy schema validation failed"

# The required chain-removal is deliberately narrow: dialer-proxy must be the
# final mapping field, and no other proxy field is rewritten.
awk '
    NR != 1 { exit 1 }
    {
        line = $0
        changed = sub(/,[[:space:]]*dialer-proxy[[:space:]]*:[[:space:]]*[^,}[:space:]][^,}]*[[:space:]]*}[[:space:]]*$/, "}", line)
        if (changed != 1)
            exit 1
        print line
    }
' "$candidate_file" > "$proxy_file" || fail "direct proxy transformation failed"
chmod 0600 "$proxy_file"

awk '
    /(^|[,{])[[:space:]]*dialer-proxy[[:space:]]*:/ { exit 1 }
    index(tolower($0), "tigr") != 0 { exit 1 }
' "$proxy_file" || fail "forbidden proxy chaining remains"

{
    printf '%s\n' \
        'mode: rule' \
        'mixed-port: 17890' \
        'allow-lan: true' \
        'bind-address: 172.30.0.2' \
        'ipv6: false' \
        'log-level: info' \
        'external-controller: 127.0.0.1:19090' \
        'listeners:' \
        '  - name: KIMI-DIRECT' \
        '    type: mixed' \
        '    port: 17891' \
        '    listen: 0.0.0.0' \
        '    proxy: DIRECT' \
        '    udp: true' \
        'profile:' \
        '  store-selected: false' \
        '  store-fake-ip: false' \
        'dns:' \
        '  enable: true' \
        '  listen: 172.30.0.2:1053' \
        '  ipv6: false' \
        '  enhanced-mode: redir-host' \
        '  use-hosts: false' \
        '  use-system-hosts: false' \
        '  respect-rules: true' \
        '  default-nameserver:' \
        '    - 1.1.1.1' \
        '  nameserver:' \
        '    - https://1.1.1.1/dns-query#DIRECT' \
        '  direct-nameserver:' \
        '    - https://1.1.1.1/dns-query#DIRECT' \
        '  proxy-server-nameserver:' \
        '    - https://1.1.1.1/dns-query#DIRECT' \
        '  nameserver-policy:' \
        '    "+.kimi.com": https://1.1.1.1/dns-query#DIRECT' \
        '    "+.moonshot.cn": https://1.1.1.1/dns-query#DIRECT' \
        '    "+.kimi.ai": https://1.1.1.1/dns-query#DIRECT' \
        '    "+.openai.com": https://1.1.1.1/dns-query#AI-US' \
        '    "+.chatgpt.com": https://1.1.1.1/dns-query#AI-US' \
        '    "+.oaistatic.com": https://1.1.1.1/dns-query#AI-US' \
        '    "+.oaiusercontent.com": https://1.1.1.1/dns-query#AI-US' \
        '    "+.oaistatsig.com": https://1.1.1.1/dns-query#AI-US' \
        '    "+.openaimerge.com": https://1.1.1.1/dns-query#AI-US' \
        '    "openaiapi-site.azureedge.net": https://1.1.1.1/dns-query#AI-US' \
        '    "+.anthropic.com": https://1.1.1.1/dns-query#AI-US' \
        '    "+.claude.ai": https://1.1.1.1/dns-query#AI-US' \
        '    "+.claude.com": https://1.1.1.1/dns-query#AI-US' \
        '    "+.claudeusercontent.com": https://1.1.1.1/dns-query#AI-US' \
        '    "+.googleapis.com": https://1.1.1.1/dns-query#AI-US' \
        '    "+.googleapis.cn": https://1.1.1.1/dns-query#AI-US' \
        '    "+.google.com": https://1.1.1.1/dns-query#AI-US' \
        '    "+.gstatic.com": https://1.1.1.1/dns-query#AI-US' \
        '    "+.googleusercontent.com": https://1.1.1.1/dns-query#AI-US' \
        '    "+.google.dev": https://1.1.1.1/dns-query#AI-US' \
        'proxies:'
    awk '{ sub(/^[[:space:]]*-[[:space:]]*/, "  - "); print }' "$proxy_file"
    printf '%s\n' \
        'proxy-groups:' \
        '  - name: AI-US' \
        '    type: select' \
        '    proxies:' \
        '      - 固定出口-US' \
        'rules:' \
        '  - DOMAIN-SUFFIX,kimi.com,DIRECT' \
        '  - DOMAIN-SUFFIX,moonshot.cn,DIRECT' \
        '  - DOMAIN-SUFFIX,kimi.ai,DIRECT' \
        '  - DOMAIN-SUFFIX,openai.com,AI-US' \
        '  - DOMAIN-SUFFIX,chatgpt.com,AI-US' \
        '  - DOMAIN-SUFFIX,oaistatic.com,AI-US' \
        '  - DOMAIN-SUFFIX,oaiusercontent.com,AI-US' \
        '  - DOMAIN-SUFFIX,oaistatsig.com,AI-US' \
        '  - DOMAIN-SUFFIX,openaimerge.com,AI-US' \
        '  - DOMAIN,openaiapi-site.azureedge.net,AI-US' \
        '  - DOMAIN-SUFFIX,anthropic.com,AI-US' \
        '  - DOMAIN-SUFFIX,claude.ai,AI-US' \
        '  - DOMAIN-SUFFIX,claude.com,AI-US' \
        '  - DOMAIN-SUFFIX,claudeusercontent.com,AI-US' \
        '  - DOMAIN-SUFFIX,googleapis.com,AI-US' \
        '  - DOMAIN-SUFFIX,googleapis.cn,AI-US' \
        '  - DOMAIN-SUFFIX,google.com,AI-US' \
        '  - DOMAIN-SUFFIX,gstatic.com,AI-US' \
        '  - DOMAIN-SUFFIX,googleusercontent.com,AI-US' \
        '  - DOMAIN-SUFFIX,google.dev,AI-US' \
        '  - DOMAIN-SUFFIX,x.ai,AI-US' \
        '  - DOMAIN-SUFFIX,grok.com,AI-US' \
        '  - MATCH,AI-US'
} > "$config_tmp"
chmod 0600 "$config_tmp"

: > "$test_log"
chmod 0600 "$test_log"
if ! "$clash_bin" -t -d "$runtime_dir" -f "$config_tmp" > "$test_log" 2>&1; then
    fail "generated configuration validation failed"
fi

rm -f "$test_log"
mv "$config_tmp" "$config_file"
chmod 0600 "$config_file"
rm -f "$candidate_file" "$proxy_file" "$count_file"

if [ "${MIHOMO_VALIDATE_ONLY:-0}" = "1" ]; then
    exit 0
fi

trap - EXIT HUP INT TERM
exec "$clash_bin" -d "$runtime_dir" -f "$config_file"
