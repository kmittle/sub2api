#!/bin/sh

set -eu

repo_dir=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
tmp_dir=$(mktemp -d)

cleanup() {
    rm -rf "$tmp_dir"
}
trap cleanup EXIT HUP INT TERM

mkdir -p "$tmp_dir/bin"

cat > "$tmp_dir/bin/docker" <<'EOF'
#!/bin/sh
set -eu

proxy_url=
for argument in "$@"; do
    case "$argument" in
        HTTP_PROXY=*) proxy_url=${argument#HTTP_PROXY=} ;;
    esac
done

case "$proxy_url" in
    http://mihomo:17890)
        [ "${FAIL_US_ROUTE:-0}" != "1" ] || exit 1
        printf 'US\n'
        ;;
    http://mihomo:17891)
        printf '203.0.113.20\n'
        ;;
    *)
        exit 1
        ;;
esac
EOF

cat > "$tmp_dir/bin/wget" <<'EOF'
#!/bin/sh
set -eu

printf '203.0.113.20\n'
EOF

chmod 0755 "$tmp_dir/bin/docker" "$tmp_dir/bin/wget"

output=$(PATH="$tmp_dir/bin:$PATH" DOCKER_COMMAND=docker \
    "$repo_dir/deploy/check-mihomo-routes.sh")
[ "$output" = 'mihomo routes verified: default=US kimi=direct' ]

if PATH="$tmp_dir/bin:$PATH" DOCKER_COMMAND=docker FAIL_US_ROUTE=1 \
    "$repo_dir/deploy/check-mihomo-routes.sh" >/dev/null 2>&1; then
    printf '%s\n' 'expected failed US residential route to fail validation' >&2
    exit 1
fi

printf '%s\n' 'mihomo route check tests passed'
