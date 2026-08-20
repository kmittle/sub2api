#!/bin/sh

set -eu

repo_dir=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
tmp_dir=$(mktemp -d)
container_prefix=sub2api-mihomo-entrypoint-test-$$
image='alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce'

cleanup() {
    docker rm -f "${container_prefix}-valid" "${container_prefix}-invalid" >/dev/null 2>&1 || true
    rm -rf "$tmp_dir"
}
trap cleanup EXIT HUP INT TERM

mkdir -p "$tmp_dir/bin"
chmod 0755 "$tmp_dir" "$tmp_dir/bin"

cat > "$tmp_dir/tigr-valid.yaml" <<'EOF'
proxies:
  - {name: 'fixed residential US', type: socks5, server: 198.51.100.10, port: 1080, username: test-us, password: test-us-password, udp: true, dialer-proxy: TIGR}
EOF
sed -i 's/fixed residential US/固定出口-US/' "$tmp_dir/tigr-valid.yaml"

cat > "$tmp_dir/tigr-invalid.yaml" <<'EOF'
proxies:
  - {name: 'fixed residential US', type: socks5, server: 198.51.100.10, port: 1080, username: test-us, password: test-us-password, udp: true}
EOF
sed -i 's/fixed residential US/固定出口-US/' "$tmp_dir/tigr-invalid.yaml"

: > "$tmp_dir/geoip.metadb"

cat > "$tmp_dir/bin/clash" <<'EOF'
#!/bin/sh
set -eu

config=
previous=
for argument in "$@"; do
    if [ "$previous" = "-f" ]; then
        config=$argument
        break
    fi
    previous=$argument
done

[ -n "$config" ]
grep -Fq 'proxy: DIRECT' "$config"
grep -Fq -- '- 固定出口-US' "$config"
grep -Fq 'DOMAIN-SUFFIX,kimi.com,DIRECT' "$config"
grep -Fq 'DOMAIN-SUFFIX,moonshot.cn,DIRECT' "$config"
grep -Fq 'DOMAIN-SUFFIX,x.ai,AI-US' "$config"
grep -Fq 'MATCH,AI-US' "$config"
[ "$(grep -c '^  - {name:' "$config")" -eq 1 ]
! grep -Fq 'dialer-proxy:' "$config"
! grep -Fq 'KIMI-CN-EXIT' "$config"
! grep -Fq 'GEOIP,CN,DIRECT' "$config"
EOF
chmod 0755 "$tmp_dir/bin/clash"
chmod 0644 "$tmp_dir/tigr-valid.yaml" "$tmp_dir/tigr-invalid.yaml" "$tmp_dir/geoip.metadb"

run_validation() {
    name=$1
    tigr_config=$2

    docker run --rm --name "${container_prefix}-${name}" \
        --memory=900m --memory-swap=1100m --cpus=1 --pids-limit=256 \
        --network=none --read-only --user=1000:1001 \
        --tmpfs /run/mihomo:rw,noexec,nosuid,nodev,mode=0700,uid=1000,gid=1001 \
        --env MIHOMO_VALIDATE_ONLY=1 \
        --volume "$repo_dir/deploy/mihomo-docker-entrypoint.sh:/opt/mihomo/mihomo-docker-entrypoint.sh:ro" \
        --volume "$tmp_dir/bin/clash:/opt/mihomo/clash:ro" \
        --volume "$tigr_config:/opt/mihomo-source/tigr.yaml:ro" \
        --volume "$tmp_dir/geoip.metadb:/opt/mihomo-source/geoip.metadb:ro" \
        --entrypoint /bin/sh \
        "$image" /opt/mihomo/mihomo-docker-entrypoint.sh
}

run_validation valid "$tmp_dir/tigr-valid.yaml"

if run_validation invalid "$tmp_dir/tigr-invalid.yaml" >/dev/null 2>&1; then
    printf '%s\n' 'expected unchained source proxy validation to fail' >&2
    exit 1
fi

printf '%s\n' 'mihomo entrypoint routing tests passed'
