"""
Backup all Hosted Feature Services from ArcGIS Online / Portal.

For each hosted feature service this script:
  1. Exports it to a File Geodatabase (FGDB).
  2. Downloads and extracts the FGDB locally.
  3. Creates an ArcGIS Pro project (.aprx) with map layers pointing at the
     local FGDB, so the service can be republished if the hosted datastore
     is ever lost or corrupted.

Requirements:
  - ArcGIS Pro (provides arcpy and the arcgis Python API)
  - A blank ArcGIS Pro project template (see TEMPLATE_APRX below)
  - Credentials for the target portal / ArcGIS Online org

Usage:
  python backup_hosted_features.py                          # back up everything
  python backup_hosted_features.py --single "My Service"    # back up one by title
  python backup_hosted_features.py --single abc123def456    # back up one by item ID
"""

import argparse
import os
import sys
import shutil
import logging
import zipfile
import time
from pathlib import Path
from datetime import datetime

from arcgis.gis import GIS
import arcpy

# ---------------------------------------------------------------------------
# Configuration – edit these to match your environment
# ---------------------------------------------------------------------------

# Portal / AGOL connection.
# For ArcGIS Online use "https://www.arcgis.com" or your org URL.
# Set to "pro" to use the active ArcGIS Pro portal sign-in.
PORTAL_URL = "pro"  # or "https://your-org.maps.arcgis.com"
USERNAME = None      # None when using "pro" auth
PASSWORD = None      # None when using "pro" auth

# A blank .aprx to clone for every service.  Create one in ArcGIS Pro with a
# single empty map named "Map" and save it here.
TEMPLATE_APRX = r"C:\GIS\Templates\Blank.aprx"

# Root folder where backups are written.
# Structure: OUTPUT_ROOT / <sanitised_service_name> / {.aprx, .gdb}
OUTPUT_ROOT = r"C:\GIS\HostedBackups"

# Optional: limit to services owned by a specific user (None = all visible).
OWNER_FILTER = None  # e.g. "jsmith_health"

# Optional: limit to a specific folder in the portal (None = all folders).
FOLDER_FILTER = None

# How many items to fetch per search page (max 10000).
PAGE_SIZE = 100

# Seconds to wait between export-status checks.
EXPORT_POLL_INTERVAL = 5

# Maximum time (seconds) to wait for an export to finish.
EXPORT_TIMEOUT = 600

# Log level
LOG_LEVEL = logging.INFO

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")


def sanitize_name(name: str) -> str:
    """Turn a service title into a filesystem-safe folder/file name."""
    keep = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_- ")
    return "".join(c if c in keep else "_" for c in name).strip().replace(" ", "_")


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


def search_hosted_feature_services(gis: GIS) -> list:
    """Return all hosted Feature Service items visible to the signed-in user."""
    query = 'type:"Feature Service" typekeywords:"Hosted Service"'
    if OWNER_FILTER:
        query += f" owner:{OWNER_FILTER}"

    items = []
    start = 1
    while True:
        batch = gis.content.search(
            query=query,
            max_items=PAGE_SIZE,
            start=start,
            sort_field="title",
            sort_order="asc",
        )
        if not batch:
            break
        items.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        start += PAGE_SIZE

    if FOLDER_FILTER:
        items = [i for i in items if getattr(i, "ownerFolder", None) == FOLDER_FILTER]

    log.info("Found %d hosted feature service(s)", len(items))
    return items


def export_and_download(item, dest_dir: Path) -> Path:
    """
    Export a hosted feature service to FGDB, download & extract it.

    Returns the path to the extracted .gdb folder.
    """
    export_title = f"{item.title}_backup_{timestamp}"
    log.info("  Exporting '%s' → FGDB …", item.title)

    exported = item.export(
        title=export_title,
        export_format="File Geodatabase",
        wait=True,
    )

    zip_path = dest_dir / f"{sanitize_name(item.title)}.gdb.zip"
    log.info("  Downloading to %s …", zip_path)
    exported.download(save_path=str(dest_dir), file_name=zip_path.name)

    # Clean up the temporary export item from the portal.
    try:
        exported.delete()
    except Exception:
        log.warning("  Could not delete temporary export item '%s'", export_title)

    # Extract the FGDB from the zip.
    log.info("  Extracting …")
    extract_dir = dest_dir / "extracted"
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    # Find the .gdb inside the extracted contents.
    gdbs = list(extract_dir.rglob("*.gdb"))
    if not gdbs:
        raise RuntimeError(f"No .gdb found in {zip_path}")
    src_gdb = gdbs[0]

    # Move the .gdb up into dest_dir with a clean name.
    final_gdb = dest_dir / f"{sanitize_name(item.title)}.gdb"
    if final_gdb.exists():
        shutil.rmtree(final_gdb)
    shutil.move(str(src_gdb), str(final_gdb))

    # Tidy up temporary files.
    shutil.rmtree(extract_dir, ignore_errors=True)
    if zip_path.exists():
        zip_path.unlink()

    log.info("  FGDB ready: %s", final_gdb)
    return final_gdb


def create_aprx(item, gdb_path: Path, dest_dir: Path) -> Path:
    """
    Clone the template .aprx into dest_dir and add every feature class
    from the downloaded FGDB as a layer in the map.
    """
    aprx_name = f"{sanitize_name(item.title)}.aprx"
    aprx_path = dest_dir / aprx_name

    log.info("  Creating project %s …", aprx_path)
    shutil.copy2(TEMPLATE_APRX, aprx_path)

    aprx = arcpy.mp.ArcGISProject(str(aprx_path))
    mp = aprx.listMaps()[0]  # expects the template to have at least one map

    # Walk the FGDB and add every feature class / table.
    arcpy.env.workspace = str(gdb_path)

    # Feature classes at the root level.
    for fc in arcpy.ListFeatureClasses() or []:
        fc_path = str(gdb_path / fc)
        log.info("    + layer: %s", fc)
        mp.addDataFromPath(fc_path)

    # Feature classes inside feature datasets.
    for ds in arcpy.ListDatasets(feature_type="Feature") or []:
        for fc in arcpy.ListFeatureClasses(feature_dataset=ds) or []:
            fc_path = str(gdb_path / ds / fc)
            log.info("    + layer: %s/%s", ds, fc)
            mp.addDataFromPath(fc_path)

    # Standalone tables.
    for tbl in arcpy.ListTables() or []:
        tbl_path = str(gdb_path / tbl)
        log.info("    + table: %s", tbl)
        mp.addDataFromPath(tbl_path)

    aprx.save()
    del aprx  # release file lock
    log.info("  Project saved: %s", aprx_path)
    return aprx_path


def backup_item(item, root: Path) -> bool:
    """Run the full backup pipeline for a single hosted feature service."""
    safe_name = sanitize_name(item.title)
    dest_dir = root / safe_name
    dest_dir.mkdir(parents=True, exist_ok=True)

    try:
        gdb_path = export_and_download(item, dest_dir)
        create_aprx(item, gdb_path, dest_dir)
        return True
    except Exception:
        log.exception("  FAILED to back up '%s'", item.title)
        return False


def parse_args():
    parser = argparse.ArgumentParser(
        description="Backup hosted feature services to local FGDB + .aprx projects."
    )
    parser.add_argument(
        "--single",
        metavar="TITLE_OR_ID",
        help="Back up a single service by its title or portal item ID.",
    )
    return parser.parse_args()


def find_single_item(gis: GIS, title_or_id: str):
    """Locate one hosted feature service by item ID or title."""
    # Try as an item ID first.
    try:
        item = gis.content.get(title_or_id)
        if item is not None:
            return item
    except Exception:
        pass

    # Fall back to a title search.
    query = (
        f'type:"Feature Service" typekeywords:"Hosted Service" title:"{title_or_id}"'
    )
    results = gis.content.search(query=query, max_items=10)
    # Exact-match on title (search is fuzzy).
    exact = [r for r in results if r.title == title_or_id]
    if exact:
        return exact[0]
    if results:
        return results[0]
    return None


def main():
    args = parse_args()

    root = Path(OUTPUT_ROOT) / timestamp
    root.mkdir(parents=True, exist_ok=True)
    log.info("Backup root: %s", root)

    gis = connect()

    if args.single:
        item = find_single_item(gis, args.single)
        if item is None:
            log.error("Could not find a hosted feature service matching '%s'", args.single)
            sys.exit(1)
        items = [item]
        log.info("Single-item mode: '%s' (id: %s)", item.title, item.id)
    else:
        items = search_hosted_feature_services(gis)

    if not items:
        log.warning("Nothing to back up.")
        return

    succeeded, failed = 0, 0
    for i, item in enumerate(items, 1):
        log.info("[%d/%d] %s  (id: %s, owner: %s)",
                 i, len(items), item.title, item.id, item.owner)
        if backup_item(item, root):
            succeeded += 1
        else:
            failed += 1

    log.info("Done.  %d succeeded, %d failed out of %d total.",
             succeeded, failed, len(items))

    # Write a manifest so you know exactly what was captured.
    manifest = root / "manifest.txt"
    with open(manifest, "w") as f:
        f.write(f"Backup timestamp: {timestamp}\n")
        f.write(f"Portal: {gis.url}\n")
        f.write(f"User: {gis.properties.user.username}\n\n")
        for item in items:
            f.write(f"{item.title}  |  {item.id}  |  owner={item.owner}\n")
    log.info("Manifest written to %s", manifest)


if __name__ == "__main__":
    main()
