#!/bin/sh

set -eu

docker_command=${DOCKER_COMMAND:-docker}
application_container=${SUB2API_CONTAINER_NAME:-sub2api}
country_url=${MIHOMO_COUNTRY_CHECK_URL:-http://ipinfo.io/country}
ip_url=${MIHOMO_IP_CHECK_URL:-http://ipinfo.io/ip}

fail() {
    printf 'mihomo route check failed: %s\n' "$1" >&2
    exit 1
}

fetch_value() {
    proxy_url=$1
    url=$2

    if ! value=$("$docker_command" exec \
        --env HTTP_PROXY="$proxy_url" \
        --env HTTPS_PROXY="$proxy_url" \
        --env ALL_PROXY= \
        --env NO_PROXY= \
        --env http_proxy="$proxy_url" \
        --env https_proxy="$proxy_url" \
        --env all_proxy= \
        --env no_proxy= \
        "$application_container" \
        wget -Y on -q -T 15 -O - "$url"); then
        return 1
    fi

    printf '%s' "$value" | tr -d '\r\n[:space:]'
}

fetch_host_value() {
    url=$1

    if ! value=$(HTTP_PROXY= HTTPS_PROXY= ALL_PROXY= NO_PROXY= \
        http_proxy= https_proxy= all_proxy= no_proxy= \
        wget -Y off -q -T 15 -O - "$url"); then
        return 1
    fi

    printf '%s' "$value" | tr -d '\r\n[:space:]'
}

kimi_ip=$(fetch_value http://mihomo:17891 "$ip_url") || fail 'KIMI direct route is unreachable'
direct_ip=$(fetch_host_value "$ip_url") || fail 'host direct route is unreachable'
us_country=$(fetch_value http://mihomo:17890 "$country_url") || fail 'US residential exit is unreachable'

[ "$us_country" = "US" ] || fail "port 17890 exited from '$us_country', expected 'US'"
[ -n "$kimi_ip" ] || fail 'port 17891 returned an empty public IP'
[ "$kimi_ip" = "$direct_ip" ] || fail 'port 17891 does not match the host direct public IP'

printf 'mihomo routes verified: default=%s kimi=direct\n' "$us_country"
