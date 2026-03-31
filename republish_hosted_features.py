"""
Republish Hosted Feature Services from local backups.

Reads backup folders produced by backup_hosted_features.py and for each:
  1. Tries to overwrite the existing service from the .aprx (preserves
     the original item ID and URL so web maps/apps keep working).
  2. If overwrite fails, falls back to delete and recreate:
     a. Deletes the old broken service from portal and ArcGIS Server.
     b. Zips and uploads the local File Geodatabase.
     c. Publishes a new hosted feature service.
     d. Restores item metadata (title, tags, description, thumbnail, etc.).
     e. Restores layer definitions (renderer, edit tracking, capabilities).
     f. Reassigns ownership if the original owner differs from the current user.
     g. Restores sharing settings (org, everyone, groups).

After all services are republished, scans Web Maps and Web Mapping
Applications for references to old service URLs and remaps them to the
new URLs (only needed for services that were recreated, not overwritten).

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
import arcpy

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
    Read metadata.json and locate the .gdb and .aprx in a backup subfolder.

    Returns dict with keys: 'metadata', 'gdb_path', 'aprx_path', 'thumbnail_path'.
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

    # Find the .aprx project.
    aprxs = list(backup_dir.glob("*.aprx"))
    aprx_path = aprxs[0] if aprxs else None

    # Optional thumbnail.
    thumbnails = list(backup_dir.glob("thumbnail.*"))
    thumbnail_path = thumbnails[0] if thumbnails else None

    return {
        "metadata": metadata,
        "gdb_path": gdb_path,
        "aprx_path": aprx_path,
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

def delete_existing(gis: GIS, old_id: str, service_name: str = None) -> bool:
    """
    Delete the old service if it still exists, by item ID, portal search,
    and ArcGIS Server service directory.

    A previous failed publish can leave behind a registered service in
    ArcGIS Server even when there's no portal item. This checks all three
    locations.
    """
    deleted = False

    # 1. Try by item ID.
    try:
        item = gis.content.get(old_id)
    except Exception:
        item = None

    if item is not None:
        log.info("  Deleting old item '%s' (%s) …", item.title, old_id)
        item.delete(force=True)
        deleted = True

    # 2. Search portal for any existing service with the same name.
    if service_name:
        query = (
            f'type:"Feature Service" typekeywords:"Hosted Service"'
            f' title:"{service_name}"'
        )
        matches = gis.content.search(query=query, max_items=10)
        for match in matches:
            if match.title == service_name and match.id != old_id:
                log.info("  Deleting orphan portal item '%s' (%s) …",
                         match.title, match.id)
                match.delete(force=True)
                deleted = True

    # 3. Check ArcGIS Server directly for orphan services with no portal item.
    if service_name:
        deleted = _delete_server_orphan(gis, service_name) or deleted

    if not deleted:
        log.info("  No existing items or services found to delete")

    return deleted


def _delete_server_orphan(gis: GIS, service_name: str) -> bool:
    """Check the Hosted folder on federated servers for an orphan service."""
    deleted = False
    try:
        servers = gis.admin.servers.list()
    except Exception:
        log.debug("  Could not list federated servers (may need admin role)")
        return False

    for server in servers:
        try:
            if server.services.exists(
                folder_name="Hosted",
                name=service_name,
                service_type="FeatureServer",
            ):
                log.info("  Found orphan service '%s' on server — deleting …",
                         service_name)
                hosted = server.services.list(folder="Hosted")
                for svc in hosted:
                    svc_name = getattr(svc.properties, "serviceName",
                                       getattr(svc.properties, "name", ""))
                    if svc_name == service_name:
                        svc.delete()
                        log.info("  Deleted orphan server service '%s'",
                                 service_name)
                        deleted = True
                        break
        except Exception:
            log.warning("  Error checking server for orphan service '%s'",
                        service_name, exc_info=True)

    return deleted


# ---------------------------------------------------------------------------
# Publish service
# ---------------------------------------------------------------------------

def publish_service(gis: GIS, meta: dict, gdb_zip: Path) -> "Item":
    """Upload the FGDB zip and publish as a new hosted feature service."""
    item_meta = meta["item"]
    pub_params = meta["publish_parameters"]

    # Use a distinct title for the uploaded FGDB so it doesn't clash with
    # the service name that ArcGIS will create during publish.
    item_properties = {
        "title": item_meta["title"] + " (source FGDB)",
        "tags": ",".join(item_meta.get("tags") or []),
        "snippet": item_meta.get("snippet") or "",
        "description": item_meta.get("description") or "",
        "type": "File Geodatabase",
    }

    log.info("  Uploading FGDB …")
    uploaded = gis.content.add(item_properties, data=str(gdb_zip))
    log.info("  Uploaded as item %s — publishing …", uploaded.id)

    # Call the REST API publish endpoint directly so we can poll for
    # completion without holding a long HTTP connection. This works across
    # all arcgis API versions (the Python publish() method's async support
    # varies by version and has known bugs).
    user = gis.properties.user.username
    publish_url = f"{gis.url}/sharing/rest/content/users/{user}/publish"
    publish_params = {
        "itemId": uploaded.id,
        "fileType": "fileGeodatabase",
        "publishParameters": json.dumps(pub_params),
        "f": "json",
    }
    resp = gis._con.post(publish_url, publish_params)
    log.info("  Publish response: %s", resp)

    # Check for top-level error.
    if resp.get("error"):
        raise RuntimeError(f"Publish request failed: {resp['error']}")

    services = resp.get("services", [])
    if not services:
        raise RuntimeError(f"Publish request returned no services: {resp}")

    service_info = services[0]
    job_id = service_info.get("jobId")
    service_item_id = service_info.get("serviceItemId")
    log.info("  Publish job started (jobId: %s, serviceItemId: %s)",
             job_id, service_item_id)

    # Poll the status endpoint on the service item until complete.
    status_url = (
        f"{gis.url}/sharing/rest/content/users/{user}"
        f"/items/{service_item_id}/status"
    )
    elapsed = 0
    while True:
        status_resp = gis._con.get(status_url, {
            "jobType": "publish",
            "jobId": job_id,
            "f": "json",
        })
        status = status_resp.get("status")
        log.info("  Publish status: %s", status_resp)

        if status == "completed":
            break

        if status == "failed":
            raise RuntimeError(
                f"Publish failed: {status_resp.get('statusMessage')}"
            )

        if elapsed >= PUBLISH_TIMEOUT:
            raise RuntimeError(
                f"Publish timed out after {PUBLISH_TIMEOUT}s"
            )

        time.sleep(PUBLISH_POLL_INTERVAL)
        elapsed += PUBLISH_POLL_INTERVAL
        if elapsed % 30 == 0:
            log.info("  Still publishing … (%ds elapsed)", elapsed)

    # Re-authenticate and fetch the published service item.
    gis = connect()
    published = gis.content.get(service_item_id)
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
# Overwrite publish from .aprx
# ---------------------------------------------------------------------------

def try_overwrite_publish(gis: GIS, aprx_path: Path, meta: dict) -> bool:
    """
    Attempt to overwrite the existing hosted feature service from the .aprx.

    Returns True if the overwrite succeeded, False otherwise.
    This preserves the original item ID and URL.
    """
    service_name = meta["publish_parameters"].get("name")
    if not service_name:
        log.info("  No service name in metadata — skipping overwrite attempt")
        return False

    if not aprx_path or not aprx_path.exists():
        log.info("  No .aprx found — skipping overwrite attempt")
        return False

    # Only attempt overwrite if the service actually exists in the portal.
    old_id = meta["item"].get("id", "")
    service_exists = False
    try:
        existing = gis.content.get(old_id)
        if existing is not None:
            service_exists = True
    except Exception:
        pass

    if not service_exists:
        log.info("  Service does not exist in portal — skipping overwrite attempt")
        return False

    log.info("  Attempting overwrite publish from %s …", aprx_path.name)

    sddraft_path = str(aprx_path.parent / f"{service_name}.sddraft")
    sd_path = str(aprx_path.parent / f"{service_name}.sd")
    aprx = None

    try:
        aprx = arcpy.mp.ArcGISProject(str(aprx_path))
        mp = aprx.listMaps()[0]

        sddraft = mp.getWebLayerSharingDraft(
            "HOSTING_SERVER", "FEATURE", service_name
        )
        sddraft.overwriteExistingService = True

        # Apply metadata from the backup.
        item_meta = meta.get("item", {})
        if item_meta.get("snippet"):
            sddraft.summary = item_meta["snippet"]
        if item_meta.get("tags"):
            sddraft.tags = ",".join(item_meta["tags"])
        if item_meta.get("description"):
            sddraft.description = item_meta["description"]
        if item_meta.get("accessInformation"):
            sddraft.credits = item_meta["accessInformation"]

        sddraft.exportToSDDraft(sddraft_path)

        # Release the aprx lock before staging so the GDB is not locked
        # if we need to fall back to the FGDB publish path.
        del aprx
        aprx = None

        arcpy.server.StageService(sddraft_path, sd_path)
        arcpy.server.UploadServiceDefinition(sd_path, "HOSTING_SERVER")

        log.info("  Overwrite publish succeeded for '%s'", service_name)
        return True

    except Exception:
        log.warning("  Overwrite publish failed — will fall back to "
                    "delete and recreate", exc_info=True)
        return False

    finally:
        # Release aprx lock if still held.
        if aprx is not None:
            del aprx
        # Clean up staging files.
        for f in (sddraft_path, sd_path):
            p = Path(f)
            if p.exists():
                p.unlink()


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
    aprx_path = backup["aprx_path"]
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
        log.info("  [DRY RUN] Would try overwrite publish from .aprx first")
        log.info("  [DRY RUN] Would %s old item %s if overwrite fails",
                 "delete" if existing else "skip (not found)", old_id)
        log.info("  [DRY RUN] Would publish '%s' from %s", title, gdb_path.name)
        log.info("  [DRY RUN] Old URL: %s", old_url)
        return None

    try:
        # Step 1: Try overwrite publish from .aprx — preserves item ID and URL.
        if try_overwrite_publish(gis, aprx_path, meta):
            log.info("  Done (overwrite): URL unchanged %s", old_url)
            return (old_url, old_url)  # URL didn't change

        # Step 2: Fall back to delete and recreate from FGDB.
        log.info("  Falling back to delete and recreate …")
        service_name = meta["publish_parameters"].get("name") or title
        delete_existing(gis, old_id, service_name)

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
        log.info("  Done (recreated): %s → %s", old_url, new_url)
        return (old_url, new_url)

    except Exception:
        log.exception("  FAILED to republish '%s'", title)
        return None


# ---------------------------------------------------------------------------
# Update web maps and apps
# ---------------------------------------------------------------------------

def _search_all(gis: GIS, query: str) -> list:
    """Search returning all matching items (compatible with arcgis 1.x)."""
    items = gis.content.search(
        query=query, max_items=10000,
        sort_field="title", sort_order="asc",
    )
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

        log.info("  Updating '%s' (%s, type=%s) …", item.title, item.id, item.type)
        try:
            # In arcgis 1.x, JSON data must be passed via the 'text' key
            # in item_properties, not as a 'data' kwarg.
            item.update(item_properties={"text": data_str})
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
