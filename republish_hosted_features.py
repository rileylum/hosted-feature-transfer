"""
Republish Hosted Feature Services from local backups.

Reads backup folders produced by backup_hosted_features.py and for each:
  1. Deletes the old broken service if it still exists in the portal.
  2. Zips and uploads the local File Geodatabase.
  3. Publishes a new hosted feature service.
  4. Restores item metadata (title, tags, description, thumbnail, etc.).
  5. Restores layer definitions (renderer, edit tracking, capabilities).
  6. Reassigns ownership if the original owner differs from the current user.
  7. Restores sharing settings (org, everyone, groups).

After all services are republished, scans Web Maps and Web Mapping
Applications for references to old service URLs and remaps them to the
new URLs.

Usage:
  python republish_hosted_features.py <backup_dir>
  python republish_hosted_features.py <backup_dir> --single My_Service
  python republish_hosted_features.py <backup_dir> --dry-run
"""

import argparse
import json
import sys
import shutil
import logging
import time
import zipfile
import tempfile
from pathlib import Path
import datetime

from arcgis.gis import GIS

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PORTAL_URL = "pro"
USERNAME = None
PASSWORD = None

LOG_LEVEL = logging.INFO

# Publish polling settings.
PUBLISH_POLL_INTERVAL = 5   # seconds between status checks
PUBLISH_TIMEOUT = 600       # max seconds to wait for publish

# How many web map / app items to fetch per search page.
PAGE_SIZE = 100

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def connect() -> GIS:
    """Return an authenticated GIS connection."""
    if PORTAL_URL.lower() == "pro":
        log.info("Connecting via active ArcGIS Pro portal sign-in …")
        gis = GIS("pro")
    else:
        log.info("Connecting to %s …", PORTAL_URL)
        gis = GIS(PORTAL_URL, USERNAME, PASSWORD)
    log.info("Signed in as %s", gis.properties.user.username)
    return gis


# ---------------------------------------------------------------------------
# Load backup
# ---------------------------------------------------------------------------

def load_backup(backup_dir: Path) -> dict:
    """
    Read metadata.json and locate the .gdb in a backup subfolder.

    Returns dict with keys: 'metadata', 'gdb_path', 'thumbnail_path'.
    """
    meta_path = backup_dir / "metadata.json"
    if not meta_path.exists():
        raise FileNotFoundError(f"No metadata.json in {backup_dir}")

    with open(meta_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    # Find the .gdb folder.
    gdbs = [p for p in backup_dir.iterdir() if p.suffix == ".gdb" and p.is_dir()]
    if not gdbs:
        raise FileNotFoundError(f"No .gdb folder in {backup_dir}")
    gdb_path = gdbs[0]

    # Optional thumbnail.
    thumbnails = list(backup_dir.glob("thumbnail.*"))
    thumbnail_path = thumbnails[0] if thumbnails else None

    return {
        "metadata": metadata,
        "gdb_path": gdb_path,
        "thumbnail_path": thumbnail_path,
    }


# ---------------------------------------------------------------------------
# Zip FGDB
# ---------------------------------------------------------------------------

def zip_gdb(gdb_path: Path) -> Path:
    """Create a temporary zip of the .gdb folder for upload."""
    zip_path = Path(tempfile.mkdtemp()) / f"{gdb_path.stem}.gdb.zip"
    log.info("  Zipping %s …", gdb_path.name)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for file in gdb_path.rglob("*"):
            arcname = str(file.relative_to(gdb_path.parent))
            zf.write(file, arcname)
    return zip_path


# ---------------------------------------------------------------------------
# Delete existing service
# ---------------------------------------------------------------------------

def delete_existing(gis: GIS, old_id: str) -> bool:
    """Delete the old service if it still exists. Returns True if deleted."""
    try:
        item = gis.content.get(old_id)
    except Exception:
        item = None

    if item is None:
        log.info("  Old item %s not found — nothing to delete", old_id)
        return False

    log.info("  Deleting old item '%s' (%s) …", item.title, old_id)
    item.delete(force=True)
    return True


# ---------------------------------------------------------------------------
# Publish service
# ---------------------------------------------------------------------------

def publish_service(gis: GIS, meta: dict, gdb_zip: Path) -> "Item":
    """Upload the FGDB zip and publish as a new hosted feature service."""
    item_meta = meta["item"]
    pub_params = meta["publish_parameters"]

    item_properties = {
        "title": item_meta["title"],
        "tags": ",".join(item_meta.get("tags") or []),
        "snippet": item_meta.get("snippet") or "",
        "description": item_meta.get("description") or "",
        "type": "File Geodatabase",
    }

    log.info("  Uploading FGDB …")
    uploaded = gis.content.add(item_properties, data=str(gdb_zip))
    log.info("  Uploaded as item %s — publishing …", uploaded.id)

    # publish() uses 'future' param (not 'wait'). Use future=True to avoid
    # blocking on one long HTTP connection that can lose its token.
    publish_future = uploaded.publish(publish_parameters=pub_params, future=True)

    # Poll the Future until the publish completes.
    elapsed = 0
    while not publish_future.done():
        if elapsed >= PUBLISH_TIMEOUT:
            raise RuntimeError(
                f"Publish timed out after {PUBLISH_TIMEOUT}s"
            )
        time.sleep(PUBLISH_POLL_INTERVAL)
        elapsed += PUBLISH_POLL_INTERVAL
        if elapsed % 30 == 0:
            log.info("  Still publishing … (%ds elapsed)", elapsed)

    published = publish_future.result()

    # Re-authenticate to ensure fresh token for subsequent operations.
    gis = connect()
    published = gis.content.get(published.id)
    log.info("  Published: '%s' (%s)", published.title, published.id)
    log.info("  New URL: %s", published.url)

    return published


# ---------------------------------------------------------------------------
# Restore item metadata
# ---------------------------------------------------------------------------

def restore_item_metadata(published, meta: dict, thumbnail_path: Path = None):
    """Update the published item with the original metadata."""
    item_meta = meta["item"]

    update_props = {
        "title": item_meta["title"],
        "snippet": item_meta.get("snippet") or "",
        "description": item_meta.get("description") or "",
        "tags": ",".join(item_meta.get("tags") or []),
        "accessInformation": item_meta.get("accessInformation") or "",
        "licenseInfo": item_meta.get("licenseInfo") or "",
    }

    kwargs = {"item_properties": update_props}
    if thumbnail_path and thumbnail_path.exists():
        kwargs["thumbnail"] = str(thumbnail_path)

    log.info("  Restoring item metadata …")
    published.update(**kwargs)


# ---------------------------------------------------------------------------
# Restore layer definitions
# ---------------------------------------------------------------------------

def restore_layer_definitions(published, meta: dict):
    """Restore renderer, capabilities, edit tracking on each layer/table."""
    layers_meta = meta.get("layers") or []
    tables_meta = meta.get("tables") or []

    service_layers = getattr(published, "layers", []) or []
    service_tables = getattr(published, "tables", []) or []

    for saved, live in _match_layers(layers_meta, service_layers):
        update = {}
        if saved.get("drawingInfo"):
            update["drawingInfo"] = saved["drawingInfo"]
        if saved.get("editingInfo"):
            update["editingInfo"] = saved["editingInfo"]
        if saved.get("editFieldsInfo"):
            update["editFieldsInfo"] = saved["editFieldsInfo"]
        if saved.get("capabilities"):
            update["capabilities"] = saved["capabilities"]
        if saved.get("hasAttachments") is not None:
            update["hasAttachments"] = saved["hasAttachments"]

        if update:
            log.info("    Updating layer %s definition …", saved.get("name"))
            try:
                live.manager.update_definition(update)
            except Exception:
                log.warning("    Could not update definition for layer %s",
                            saved.get("name"), exc_info=True)

    for saved, live in _match_layers(tables_meta, service_tables):
        update = {}
        if saved.get("editingInfo"):
            update["editingInfo"] = saved["editingInfo"]
        if saved.get("editFieldsInfo"):
            update["editFieldsInfo"] = saved["editFieldsInfo"]
        if saved.get("capabilities"):
            update["capabilities"] = saved["capabilities"]

        if update:
            log.info("    Updating table %s definition …", saved.get("name"))
            try:
                live.manager.update_definition(update)
            except Exception:
                log.warning("    Could not update definition for table %s",
                            saved.get("name"), exc_info=True)


def _match_layers(saved_list, live_list):
    """Pair saved metadata entries with live layers by index/id."""
    live_by_id = {lyr.properties.get("id"): lyr for lyr in live_list}
    for saved in saved_list:
        sid = saved.get("id")
        if sid is not None and sid in live_by_id:
            yield saved, live_by_id[sid]
        else:
            log.warning("    No matching live layer for saved id=%s name=%s",
                        sid, saved.get("name"))


# ---------------------------------------------------------------------------
# Restore ownership
# ---------------------------------------------------------------------------

def restore_ownership(gis: GIS, published, meta: dict):
    """Reassign the item to its original owner if different."""
    original_owner = meta["item"].get("owner")
    current_user = gis.properties.user.username

    if not original_owner or original_owner == current_user:
        return

    log.info("  Reassigning ownership to %s …", original_owner)
    try:
        published.reassign_to(target_owner=original_owner)
    except Exception:
        log.warning("  Could not reassign to %s — may need admin privileges",
                    original_owner, exc_info=True)


# ---------------------------------------------------------------------------
# Restore sharing
# ---------------------------------------------------------------------------

def restore_sharing(published, meta: dict):
    """Restore sharing settings from the backup metadata."""
    sharing = meta.get("sharing", {})
    everyone = sharing.get("everyone", False)
    org = sharing.get("org", False)
    group_ids = [g["id"] for g in sharing.get("groups", []) if g.get("id")]

    log.info("  Restoring sharing (everyone=%s, org=%s, groups=%d) …",
             everyone, org, len(group_ids))
    try:
        published.share(everyone=everyone, org=org, groups=group_ids)
    except Exception:
        log.warning("  Could not fully restore sharing", exc_info=True)


# ---------------------------------------------------------------------------
# Republish one service
# ---------------------------------------------------------------------------

def republish_one(gis: GIS, backup_dir: Path, dry_run: bool = False):
    """
    Full republish pipeline for one backup folder.

    Returns (old_url, new_url) tuple, or None on failure.
    """
    backup = load_backup(backup_dir)
    meta = backup["metadata"]
    gdb_path = backup["gdb_path"]
    thumbnail_path = backup["thumbnail_path"]

    old_url = meta["item"].get("url", "")
    old_id = meta["item"].get("id", "")
    title = meta["item"].get("title", backup_dir.name)

    if dry_run:
        existing = None
        try:
            existing = gis.content.get(old_id)
        except Exception:
            pass
        log.info("  [DRY RUN] Would %s old item %s",
                 "delete" if existing else "skip (not found)", old_id)
        log.info("  [DRY RUN] Would publish '%s' from %s", title, gdb_path.name)
        log.info("  [DRY RUN] Old URL: %s", old_url)
        return None

    try:
        delete_existing(gis, old_id)

        gdb_zip = zip_gdb(gdb_path)
        try:
            published = publish_service(gis, meta, gdb_zip)
        finally:
            # Clean up temp zip.
            if gdb_zip.exists():
                gdb_zip.unlink()
            gdb_zip.parent.rmdir()

        restore_item_metadata(published, meta, thumbnail_path)
        restore_layer_definitions(published, meta)
        restore_ownership(gis, published, meta)
        restore_sharing(published, meta)

        new_url = published.url
        log.info("  Done: %s → %s", old_url, new_url)
        return (old_url, new_url)

    except Exception:
        log.exception("  FAILED to republish '%s'", title)
        return None


# ---------------------------------------------------------------------------
# Update web maps and apps
# ---------------------------------------------------------------------------

def _search_all(gis: GIS, query: str) -> list:
    """Paginated search returning all matching items."""
    items = []
    start = 1
    while True:
        batch = gis.content.search(
            query=query, max_items=PAGE_SIZE, start=start,
            sort_field="title", sort_order="asc",
        )
        if not batch:
            break
        items.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        start += PAGE_SIZE
    return items


def update_web_maps_and_apps(gis: GIS, url_map: dict, dry_run: bool = False):
    """
    Scan all Web Maps and Web Mapping Applications for old service URLs
    and replace them with the new URLs.
    """
    if not url_map:
        log.info("No URL mappings — skipping web map/app updates.")
        return

    log.info("Scanning web maps and apps for %d URL mapping(s) …", len(url_map))

    items = []
    items.extend(_search_all(gis, 'type:"Web Map"'))
    items.extend(_search_all(gis, 'type:"Web Mapping Application"'))
    log.info("Found %d web maps / apps to scan", len(items))

    updated_count = 0
    for item in items:
        try:
            data = item.get_data()
        except Exception:
            continue

        if data is None:
            continue

        data_str = json.dumps(data, ensure_ascii=False)
        original_str = data_str

        for old_url, new_url in url_map.items():
            if old_url in data_str:
                data_str = data_str.replace(old_url, new_url)

        if data_str == original_str:
            continue

        if dry_run:
            matched = [old for old in url_map if old in original_str]
            log.info("  [DRY RUN] Would update '%s' (%s) — matched URLs: %s",
                     item.title, item.id, matched)
            continue

        updated_data = json.loads(data_str)
        log.info("  Updating '%s' (%s, type=%s) …", item.title, item.id, item.type)
        try:
            item.update(data=updated_data)
            updated_count += 1
        except Exception:
            log.warning("  Could not update '%s'", item.title, exc_info=True)

    log.info("Updated %d web map(s) / app(s).", updated_count)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Republish hosted feature services from backup."
    )
    parser.add_argument(
        "backup_dir",
        help="Path to the timestamped backup root directory.",
    )
    parser.add_argument(
        "--single",
        metavar="FOLDER_NAME",
        help="Republish only this subfolder (sanitised service name).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would happen without making changes.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    backup_root = Path(args.backup_dir)

    if not backup_root.exists():
        log.error("Backup directory does not exist: %s", backup_root)
        sys.exit(1)

    gis = connect()

    # Discover service subfolders (each has a metadata.json).
    if args.single:
        subdirs = [backup_root / args.single]
        if not subdirs[0].exists():
            log.error("Subfolder not found: %s", subdirs[0])
            sys.exit(1)
    else:
        subdirs = sorted(
            d for d in backup_root.iterdir()
            if d.is_dir() and (d / "metadata.json").exists()
        )

    if not subdirs:
        log.warning("No service backups found in %s", backup_root)
        return

    log.info("Found %d service backup(s) to republish", len(subdirs))

    url_map = {}
    succeeded, failed = 0, 0

    for i, sd in enumerate(subdirs, 1):
        log.info("[%d/%d] %s", i, len(subdirs), sd.name)
        result = republish_one(gis, sd, dry_run=args.dry_run)
        if result is not None:
            old_url, new_url = result
            if old_url and new_url:
                url_map[old_url] = new_url
            succeeded += 1
        elif not args.dry_run:
            failed += 1

    if not args.dry_run:
        log.info("Republished %d, failed %d out of %d",
                 succeeded, failed, len(subdirs))

    # Phase 2: remap web maps / apps.
    update_web_maps_and_apps(gis, url_map, dry_run=args.dry_run)

    log.info("All done.")


if __name__ == "__main__":
    main()
