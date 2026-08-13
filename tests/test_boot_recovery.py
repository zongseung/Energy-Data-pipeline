import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/install_boot_recovery.sh"
UNIT = ROOT / "systemd/energy-data-pipeline.service"
DEMAND_UUID = "a85a78d5-555f-4783-bf3a-e93088a03b55"
PV_UUID = "c8b2ccb1-0d59-43f7-b0cc-18fa6d591dd8"
OPTIONS = (
    "defaults,_netdev,nofail,x-systemd.automount,"
    "x-systemd.device-timeout=30s,x-systemd.mount-timeout=30s"
)


def _render(tmp_path: Path, demand: str, pv: str):
    fstab = tmp_path / "fstab"
    fstab.write_text(f"# test\n{demand}\n{pv}\n", encoding="utf-8")
    return subprocess.run(
        [str(SCRIPT), "--render-fstab", str(fstab)],
        capture_output=True,
        text=True,
        check=False,
    )


def test_fstab_renderer_accepts_only_known_layout_and_is_idempotent(tmp_path):
    demand = (
        f"UUID={DEMAND_UUID} /mnt/iscsi ext4 "
        "defaults,_netdev,nofail,x-systemd.automount 0 2"
    )
    pv = (
        f"UUID={PV_UUID} /mnt/iscsi-renewable ext4 "
        "defaults,nofail,x-systemd.automount 0 2"
    )

    first = _render(tmp_path, demand, pv)
    assert first.returncode == 0, first.stderr
    assert f"UUID={DEMAND_UUID} /mnt/iscsi ext4 {OPTIONS} 0 2" in first.stdout
    assert f"UUID={PV_UUID} /mnt/iscsi-renewable ext4 {OPTIONS} 0 2" in first.stdout

    installed = tmp_path / "installed"
    installed.write_text(first.stdout, encoding="utf-8")
    second = subprocess.run(
        [str(SCRIPT), "--render-fstab", str(installed)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert second.returncode == 0, second.stderr
    assert second.stdout == first.stdout


def test_fstab_renderer_rejects_unexpected_matching_row(tmp_path):
    result = _render(
        tmp_path,
        f"UUID={DEMAND_UUID} /wrong ext4 defaults 0 2",
        f"UUID={PV_UUID} /mnt/iscsi-renewable ext4 {OPTIONS} 0 2",
    )
    assert result.returncode != 0
    assert "unexpected iSCSI fstab entry" in result.stderr


def test_fstab_renderer_rejects_missing_uuid(tmp_path):
    result = _render(
        tmp_path,
        f"UUID={DEMAND_UUID} /mnt/iscsi ext4 {OPTIONS} 0 2",
        "UUID=other /mnt/other ext4 defaults 0 2",
    )
    assert result.returncode != 0
    assert "expected exactly one entry for each iSCSI UUID" in result.stderr


def test_service_always_attempts_both_cleanup_commands():
    unit = UNIT.read_text(encoding="utf-8")
    assert "ExecStop=" not in unit
    cleanup = next(line for line in unit.splitlines() if line.startswith("ExecStopPost="))
    assert cleanup.count("/usr/bin/docker compose") == 2
    assert cleanup.count(" &") == 2


def test_service_retries_when_iscsi_mounts_are_late():
    unit = UNIT.read_text(encoding="utf-8")
    assert "After=network-online.target open-iscsi.service remote-fs.target" in unit
    assert "mnt-iscsi.automount" in unit
    assert r"mnt-iscsi\x2drenewable.automount" in unit
    assert "RequiresMountsFor=" not in unit
    assert "Restart=on-failure" in unit


def test_service_stops_before_iscsi_mounts():
    unit = UNIT.read_text(encoding="utf-8")
    assert r"After=mnt-iscsi.mount mnt-iscsi\x2drenewable.mount" in unit


def test_iscsi_node_preflight_precedes_system_file_changes():
    script = SCRIPT.read_text(encoding="utf-8")
    copy = script.index('cp -a "${FSTAB}"')
    assert script.index("iscsiadm -m node") < copy
    assert script.index("--op update") < copy
