#!/usr/bin/env bash
set -euo pipefail
exec 3<>/dev/tcp/127.0.0.1/8080
printf 'GET /actuator/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n' >&3
head -n 1 <&3 | grep -q ' 200 '
