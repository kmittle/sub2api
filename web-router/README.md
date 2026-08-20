# Sub2API web router

This directory contains deployment-local Mihomo assets. Proxy definitions,
subscription files, databases, and binaries are ignored by Git because they
contain credentials or machine-specific data. Keep any local file containing
credentials readable only by its owner (`chmod 600`).

Required local files:

- `clash`: Mihomo-compatible executable.
- `geoip.metadb`: GeoIP database used by Mihomo.
- `tigr.yaml`: source containing exactly one inline fixed US residential proxy
  named `固定出口-US`. The container removes that entry's `dialer-proxy` field
  so this US server connects to it directly.

Runtime routing is fail-closed:

- Port `17890`: all non-KIMI traffic uses the fixed US residential exit.
- Port `17891`: all traffic uses the host's direct route, preserving the
  previously working KIMI setup.
- KIMI domains received on port `17890` are also forced to `DIRECT`.

After deployment, run `sudo ./deploy/check-mihomo-routes.sh`. It fails unless
port `17890` exits in the US and port `17891` matches the host's direct public
IP.
