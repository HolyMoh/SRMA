"""
Semantic table parser for clinical study PDFs.

SemanticTable is @dataclass — never serialised, never returned by the LLM,
never written to the audit log.

Its bbox field is ExtractionLocation (Pydantic, validated) — the only
Pydantic object within this dataclass.

Cell indexing: zero-based into cell_matrix, header rows excluded.
Spanning headers are additive — they do not shift cell_col indices.

Import direction: models.extraction_schema (for ExtractionLocation),
                  utils.coordinate_utils, config
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

from models.extraction_schema import ExtractionLocation
from utils.coordinate_utils import normalise_pdfplumber_coords


# ── Data structures ────────────────────────────────────────────────────────────

@dataclass
class SpanningHeader:
    """Metadata for a multi-column spanning header cell."""
    text: str
    start_col: int    # zero-based column index of first spanned column
    end_col: int      # inclusive end column index
    row_level: int    # 0 = top-most header row, 1 = second, etc.


@dataclass
class SemanticTable:
    """Parsed table from a PDF page.

    NEVER serialised, NEVER returned by LLM, NEVER written to audit log.
    Internal parser helper only.
    """
    table_id: str
    page: int
    column_headers: list[str]           # normalised header strings (row 0+)
    cell_matrix: list[list[str]]        # data cells: cell_matrix[row][col]
    raw_rows: list[list[str]]           # raw pre-normalised rows (debug only)
    spanning_headers: list[SpanningHeader] = field(default_factory=list)
    bbox: Optional[ExtractionLocation] = None    # pydantic, validated
    caption: Optional[str] = None
    footnotes: list[str] = field(default_factory=list)

    def get_cell(self, row: int, col: int) -> Optional[str]:
        """Safe zero-based cell access. Returns None if out of bounds."""
        if row < 0 or row >= len(self.cell_matrix):
            return None
        row_data = self.cell_matrix[row]
        if col < 0 or col >= len(row_data):
            return None
        return row_data[col]

    def find_column(self, header_pattern: str) -> Optional[int]:
        """Return zero-based column index for the first header matching pattern.

        Case-insensitive regex match against column_headers.
        Returns None if not found.
        """
        pat = re.compile(header_pattern, re.IGNORECASE)
        for i, h in enumerate(self.column_headers):
            if pat.search(h):
                return i
        return None

    def find_row_by_label(self, label_pattern: str, label_col: int = 0) -> Optional[int]:
        """Return zero-based row index where label_col matches label_pattern.

        Returns None if not found.
        """
        pat = re.compile(label_pattern, re.IGNORECASE)
        for row_i, row in enumerate(self.cell_matrix):
            if label_col < len(row) and pat.search(str(row[label_col])):
                return row_i
        return None

    def cell_location(self, row: int, col: int) -> ExtractionLocation:
        """Build an ExtractionLocation for a specific cell.

        Returns a table-based location (no bbox coordinates).
        Raises IndexError if cell is out of bounds.
        """
        if self.get_cell(row, col) is None:
            raise IndexError(
                f"Cell ({row}, {col}) out of bounds in table '{self.table_id}'"
            )
        return ExtractionLocation(
            page=self.page,
            table_id=self.table_id,
            cell_row=row,
            cell_col=col,
        )


# ── Parser ─────────────────────────────────────────────────────────────────────

class TableSemanticParser:
    """Parse pdfplumber table extractions into SemanticTable objects."""

    def __init__(self, header_row_count: int = 1) -> None:
        """
        Args:
            header_row_count: number of header rows at the top of each table
                (default 1). Multi-row headers (spanning) are auto-detected
                when the first data row looks like it still contains headers.
        """
        self._header_row_count = header_row_count

    def parse_table(
        self,
        raw_table: list[list[str | None]],
        page: int,
        table_index: int,
        page_height: float,
        table_bbox: Optional[tuple[float, float, float, float]] = None,
        caption: Optional[str] = None,
        footnotes: Optional[list[str]] = None,
    ) -> SemanticTable:
        """Parse a raw pdfplumber table into a SemanticTable.

        Args:
            raw_table: list of rows, each row is list of cell strings/None
            page: 1-based page number
            table_index: 0-based index on the page
            page_height: for coordinate normalisation
            table_bbox: raw pdfplumber bbox (x0, top, x1, bottom) or None
            caption: caption text if available
            footnotes: footnote strings

        Returns:
            SemanticTable (never serialised)
        """
        table_id = f"T{page}_{table_index}"

        # Normalise None → ""
        normalised_rows: list[list[str]] = []
        for row in raw_table:
            normalised_rows.append([str(c).strip() if c is not None else "" for c in row])

        if not normalised_rows:
            return SemanticTable(
                table_id=table_id,
                page=page,
                column_headers=[],
                cell_matrix=[],
                raw_rows=[],
                caption=caption,
                footnotes=footnotes or [],
            )

        # Auto-detect spanning headers and header row count
        header_rows, data_rows, spanning_headers = self._split_headers(normalised_rows)

        # Column headers: use last header row (most specific)
        column_headers = header_rows[-1] if header_rows else []

        # Pad all data rows to header width
        n_cols = len(column_headers)
        padded_data: list[list[str]] = []
        for row in data_rows:
            padded = row[:n_cols] + [""] * max(0, n_cols - len(row))
            padded_data.append(padded)

        # Build bbox ExtractionLocation (validated Pydantic)
        bbox_loc: Optional[ExtractionLocation] = None
        if table_bbox is not None:
            try:
                nx0, ny0, nx1, ny1 = normalise_pdfplumber_coords(table_bbox, page_height)
                bbox_loc = ExtractionLocation(
                    page=page,
                    table_id=table_id,
                    x0=nx0, y0=ny0, x1=nx1, y1=ny1,
                )
            except Exception:
                # Fallback: table-only location
                bbox_loc = ExtractionLocation(
                    page=page,
                    table_id=table_id,
                )

        return SemanticTable(
            table_id=table_id,
            page=page,
            column_headers=column_headers,
            cell_matrix=padded_data,
            raw_rows=normalised_rows,
            spanning_headers=spanning_headers,
            bbox=bbox_loc,
            caption=caption,
            footnotes=footnotes or [],
        )

    def _split_headers(
        self,
        rows: list[list[str]],
    ) -> tuple[list[list[str]], list[list[str]], list[SpanningHeader]]:
        """Detect and split header rows from data rows.

        Heuristic: a row is considered a spanning header if it has many empty
        cells but at least one non-empty cell that spans multiple columns.
        Conservative: default to self._header_row_count if auto-detection fails.

        Returns:
            (header_rows, data_rows, spanning_headers)
        """
        spanning_headers: list[SpanningHeader] = []
        if not rows:
            return [], [], []

        # Detect multi-row headers by checking if a row has fewer unique
        # non-empty cells than expected (suggests spanning)
        detected_header_count = self._header_row_count
        for i, row in enumerate(rows[:3]):  # check first 3 rows
            non_empty = [c for c in row if c]
            if len(non_empty) < len(row) * 0.5 and i == 0:
                # First row looks like spanning header — peek at second
                detected_header_count = max(detected_header_count, 2)
                # Record spanning structure
                j = 0
                while j < len(row):
                    if row[j]:
                        # Find how many consecutive empty cells follow
                        end = j + 1
                        while end < len(row) and not row[end]:
                            end += 1
                        if end > j + 1:
                            spanning_headers.append(SpanningHeader(
                                text=row[j],
                                start_col=j,
                                end_col=end - 1,
                                row_level=i,
                            ))
                    j += 1

        split_at = min(detected_header_count, len(rows))
        header_rows = rows[:split_at]
        data_rows = rows[split_at:]

        return header_rows, data_rows, spanning_headers


def render_table_as_gfm(table: SemanticTable) -> str:
    """Render a SemanticTable as a GitHub-Flavoured Markdown table.

    Used when passing table content to the LLM for extraction.
    Includes table_id header and spatial tags for cell provenance.

    Returns:
        GFM table string with header comment noting table_id and page.
    """
    lines: list[str] = []
    lines.append(f"<!-- table_id={table.table_id} page={table.page} -->")
    if table.caption:
        lines.append(f"<!-- caption: {table.caption} -->")

    if not table.column_headers:
        return "\n".join(lines)

    # Header row
    header_str = "| " + " | ".join(table.column_headers) + " |"
    separator_str = "| " + " | ".join(["---"] * len(table.column_headers)) + " |"
    lines.append(header_str)
    lines.append(separator_str)

    # Data rows with cell coordinates as GFM comments (inline)
    for row_i, row in enumerate(table.cell_matrix):
        cells: list[str] = []
        for col_i, cell in enumerate(row):
            # Annotate cell with zero-based indices for LLM provenance
            cell_tag = f"[R{row_i}C{col_i}]"
            cell_content = f"{cell} {cell_tag}" if cell else cell_tag
            cells.append(cell_content)
        lines.append("| " + " | ".join(cells) + " |")

    if table.footnotes:
        for fn in table.footnotes:
            lines.append(f"<!-- footnote: {fn} -->")

    return "\n".join(lines)
