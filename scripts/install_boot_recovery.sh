#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
UNIT_SOURCE="${ROOT_DIR}/systemd/energy-data-pipeline.service"
PORTAL=192.9.65.61:3260

render_fstab() {
  awk '
    BEGIN {
      demand_uuid = "UUID=a85a78d5-555f-4783-bf3a-e93088a03b55"
      renewable_uuid = "UUID=c8b2ccb1-0d59-43f7-b0cc-18fa6d591dd8"
      desired = "defaults,_netdev,nofail,x-systemd.automount,x-systemd.device-timeout=30s,x-systemd.mount-timeout=30s"
      demand_old = "defaults,_netdev,nofail,x-systemd.automount"
      renewable_old = "defaults,nofail,x-systemd.automount"
    }
    $1 == demand_uuid {
      demand++
      if (NF != 6 || $2 != "/mnt/iscsi" || $3 != "ext4" ||
          ($4 != demand_old && $4 != desired) || $5 != 0 || $6 != 2) {
        print "unexpected iSCSI fstab entry: " $0 > "/dev/stderr"
        invalid = 1
        next
      }
      print demand_uuid " /mnt/iscsi ext4 " desired " 0 2"
      next
    }
    $1 == renewable_uuid {
      renewable++
      if (NF != 6 || $2 != "/mnt/iscsi-renewable" || $3 != "ext4" ||
          ($4 != renewable_old && $4 != desired) || $5 != 0 || $6 != 2) {
        print "unexpected iSCSI fstab entry: " $0 > "/dev/stderr"
        invalid = 1
        next
      }
      print renewable_uuid " /mnt/iscsi-renewable ext4 " desired " 0 2"
      next
    }
    { print }
    END {
      if (demand != 1 || renewable != 1) {
        print "expected exactly one entry for each iSCSI UUID" > "/dev/stderr"
        invalid = 1
      }
      if (invalid) exit 42
    }
  ' "$1"
}

if [[ ${1:-} == --render-fstab ]]; then
  if [[ $# -ne 2 ]]; then
    echo "Usage: $0 --render-fstab FSTAB" >&2
    exit 2
  fi
  render_fstab "$2"
  exit
fi

if [[ ${EUID} -ne 0 ]]; then
  echo "Run as root: sudo $0" >&2
  exit 1
fi

FSTAB=/etc/fstab
FSTAB_TMP=$(mktemp)
FSTAB_BACKUP="/etc/fstab.energy-data-pipeline.$(date +%Y%m%d%H%M%S).bak"
trap 'rm -f "${FSTAB_TMP}"' EXIT

test -f "${UNIT_SOURCE}"
test -f /mnt/nvme/weather-pipeline/docker/docker-compose.yml
test -f /mnt/iscsi/postgres/demand-postgres/PG_VERSION
test -f /mnt/iscsi-renewable/postgres/pv-data-postgres/PG_VERSION

render_fstab "${FSTAB}" > "${FSTAB_TMP}"

findmnt --verify --verbose --tab-file "${FSTAB_TMP}"
systemd-analyze verify "${UNIT_SOURCE}"
iscsiadm -m node -p "${PORTAL}" >/dev/null
iscsiadm -m node -p "${PORTAL}" \
  --op update -n node.startup -v automatic

cp -a "${FSTAB}" "${FSTAB_BACKUP}"
install -o root -g root -m 0644 "${FSTAB_TMP}" "${FSTAB}"
install -o root -g root -m 0644 "${UNIT_SOURCE}" \
  /etc/systemd/system/energy-data-pipeline.service

systemctl enable iscsid.service open-iscsi.service
systemctl daemon-reload
systemctl enable energy-data-pipeline.service
systemctl restart energy-data-pipeline.service

echo "fstab backup: ${FSTAB_BACKUP}"
systemctl is-enabled energy-data-pipeline.service
systemctl is-active energy-data-pipeline.service
