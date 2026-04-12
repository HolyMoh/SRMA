"""
Coordinate normalisation and annotation utilities.

All coordinates are normalised to top-left-origin before use.
pdfplumber uses bottom-left origin; PyMuPDF (fitz) uses top-left origin.
"""
from __future__ import annotations
from typing import Optional

from models.extraction_schema import ExtractionLocation


def normalise_pdfplumber_coords(
    bbox: tuple[float, float, float, float],
    page_height: float,
) -> tuple[float, float, float, float]:
    """Convert pdfplumber bbox (bottom-left origin) to top-left origin.

    pdfplumber bbox: (x0, top, x1, bottom) where top < bottom in PDF space
    (y increases upward from bottom-left corner).
    After normalisation: y0=top-left, y1=bottom-right in screen space.

    Args:
        bbox: (x0, y0_pdf, x1, y1_pdf) in pdfplumber/PDF coordinate space
        page_height: total page height in points

    Returns:
        (x0, y0, x1, y1) in top-left-origin coordinate system
    """
    x0, y0_pdf, x1, y1_pdf = bbox
    # pdfplumber already flips y relative to PDF origin in most versions,
    # so bbox from pdfplumber is (x0, top, x1, bottom) with y increasing downward
    # Direct passthrough — pdfplumber normalises internally
    return (float(x0), float(y0_pdf), float(x1), float(y1_pdf))


def normalise_fitz_coords(
    bbox: tuple[float, float, float, float],
) -> tuple[float, float, float, float]:
    """Normalise PyMuPDF (fitz) bbox to top-left origin.

    fitz Rect/bbox: (x0, y0, x1, y1) already uses top-left origin.
    This function validates ordering and casts to float.

    Args:
        bbox: (x0, y0, x1, y1) from fitz

    Returns:
        (x0, y0, x1, y1) normalised floats
    """
    x0, y0, x1, y1 = bbox
    x0, y0, x1, y1 = float(x0), float(y0), float(x1), float(y1)
    # Ensure correct ordering
    if x0 > x1:
        x0, x1 = x1, x0
    if y0 > y1:
        y0, y1 = y1, y0
    return (x0, y0, x1, y1)


def build_highlight_annotation(
    loc: ExtractionLocation,
    color: tuple[float, float, float] = (1.0, 1.0, 0.0),
) -> dict:
    """Build a highlight annotation descriptor from an ExtractionLocation.

    Returns a dict suitable for rendering in the Streamlit PDF viewer.
    Falls back to None fields if bbox is not available (table-only location).

    Args:
        loc: ExtractionLocation with coordinate data
        color: RGB tuple in [0,1] range (default: yellow)

    Returns:
        dict with keys: page, x0, y0, x1, y1, color, table_id
        bbox fields are None for table-only locations.
    """
    return {
        "page": loc.page,
        "x0": loc.x0,
        "y0": loc.y0,
        "x1": loc.x1,
        "y1": loc.y1,
        "color": list(color),
        "table_id": loc.table_id,
        "cell_row": loc.cell_row,
        "cell_col": loc.cell_col,
        "has_bbox": all(v is not None for v in [loc.x0, loc.y0, loc.x1, loc.y1]),
    }


def expand_bbox(
    bbox: tuple[float, float, float, float],
    margin: float = 2.0,
) -> tuple[float, float, float, float]:
    """Expand a bounding box by a uniform margin in all directions.

    Useful for highlight annotations to ensure the text is fully enclosed.

    Args:
        bbox: (x0, y0, x1, y1)
        margin: pixels/points to expand in each direction

    Returns:
        Expanded (x0, y0, x1, y1)
    """
    x0, y0, x1, y1 = bbox
    return (x0 - margin, y0 - margin, x1 + margin, y1 + margin)


def bbox_overlap(
    a: tuple[float, float, float, float],
    b: tuple[float, float, float, float],
) -> float:
    """Compute intersection-over-union (IoU) overlap between two bboxes.

    Args:
        a, b: (x0, y0, x1, y1) bounding boxes

    Returns:
        IoU in [0.0, 1.0]
    """
    ax0, ay0, ax1, ay1 = a
    bx0, by0, bx1, by1 = b
    ix0 = max(ax0, bx0)
    iy0 = max(ay0, by0)
    ix1 = min(ax1, bx1)
    iy1 = min(ay1, by1)
    if ix1 <= ix0 or iy1 <= iy0:
        return 0.0
    inter = (ix1 - ix0) * (iy1 - iy0)
    area_a = (ax1 - ax0) * (ay1 - ay0)
    area_b = (bx1 - bx0) * (by1 - by0)
    union = area_a + area_b - inter
    return inter / union if union > 0 else 0.0
