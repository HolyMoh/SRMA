"""
PDF processor for the SRMA Extraction Engine.

Responsibilities:
- Quality gate: returns PDFQuality enum, never raises
- Coordinate normalisation after every bbox extraction
- tenacity retry on per-page parser: stop_after_attempt(3), wait_fixed(2)
- Extraction order: tables → semantic parser → text blocks with spatial tags
  → section ranking
- ParsedPDF is @dataclass — never serialised, never returned by LLM,
  never written to audit log

Import direction: models.extraction_schema (PDFQuality),
                  engine.table_semantic_parser, utils.coordinate_utils,
                  utils.audit_logger, config
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import config
from models.extraction_schema import PDFQuality, ExtractionLocation
from engine.table_semantic_parser import (
    SemanticTable, TableSemanticParser, render_table_as_gfm
)
from utils.coordinate_utils import normalise_pdfplumber_coords, normalise_fitz_coords
from utils.audit_logger import get_logger

# ── Data structures ────────────────────────────────────────────────────────────

@dataclass
class TextBlock:
    """A text block from a PDF page with spatial coordinates."""
    text: str
    page: int
    x0: float
    y0: float
    x1: float
    y1: float
    section_label: Optional[str] = None   # detected section name (Methods, Results…)


@dataclass
class ParsedPDF:
    """Result of parsing a PDF file.

    NEVER serialised, NEVER returned by LLM, NEVER written to audit log.
    Internal parser dataclass only.
    """
    filename: str
    quality: PDFQuality
    total_pages: int
    tables: list[SemanticTable]           # all detected tables, all pages
    text_blocks: list[TextBlock]          # all text blocks with spatial tags
    abstract_text: str                    # concatenated abstract section
    methods_text: str                     # concatenated methods section
    full_text: str                        # full document text (methods + results)
    page_texts: dict[int, str]            # {1-based page: raw text}
    warnings: list[str]


# ── Section detection patterns ─────────────────────────────────────────────────

_SECTION_PATTERNS: dict[str, re.Pattern] = {
    "abstract":     re.compile(r'^\s*abstract\s*$', re.IGNORECASE | re.MULTILINE),
    "introduction": re.compile(r'^\s*(?:introduction|background)\s*$', re.IGNORECASE | re.MULTILINE),
    "methods":      re.compile(r'^\s*(?:method|methods|materials?\s+and\s+methods?|study\s+design)\s*$', re.IGNORECASE | re.MULTILINE),
    "results":      re.compile(r'^\s*results?\s*$', re.IGNORECASE | re.MULTILINE),
    "discussion":   re.compile(r'^\s*discussion\s*$', re.IGNORECASE | re.MULTILINE),
    "conclusion":   re.compile(r'^\s*conclusions?\s*$', re.IGNORECASE | re.MULTILINE),
}

_CHAR_DENSITY_THRESHOLD = 0.1   # chars per pixel² — below this is LOW_DENSITY

# ── Processor class ────────────────────────────────────────────────────────────

class PDFProcessor:
    """Parse a PDF file into a ParsedPDF dataclass."""

    def __init__(self) -> None:
        self._logger = get_logger()
        self._table_parser = TableSemanticParser()

    def process(self, pdf_path: str | Path, pdf_filename: str) -> ParsedPDF:
        """Parse the PDF at pdf_path.

        Quality gate runs first; returns ParsedPDF with appropriate PDFQuality.
        Never raises — logs and returns degraded ParsedPDF on error.

        Args:
            pdf_path: path to the PDF file
            pdf_filename: display name for logging

        Returns:
            ParsedPDF (never raises)
        """
        pdf_path = Path(pdf_path)
        if not pdf_path.exists():
            self._logger.log_pdf_skipped(pdf_filename, "File not found")
            return ParsedPDF(
                filename=pdf_filename,
                quality=PDFQuality.UNREADABLE,
                total_pages=0,
                tables=[], text_blocks=[], abstract_text="",
                methods_text="", full_text="", page_texts={},
                warnings=[f"File not found: {pdf_path}"],
            )

        try:
            return self._parse_pdf(pdf_path, pdf_filename)
        except Exception as e:
            self._logger.log_parser_error(pdf_filename, None, str(e))
            self._logger.log_pdf_skipped(pdf_filename, f"Fatal parse error: {e}")
            return ParsedPDF(
                filename=pdf_filename,
                quality=PDFQuality.UNREADABLE,
                total_pages=0,
                tables=[], text_blocks=[], abstract_text="",
                methods_text="", full_text="", page_texts={},
                warnings=[f"Fatal parse error: {e}"],
            )

    def _parse_pdf(self, pdf_path: Path, pdf_filename: str) -> ParsedPDF:
        """Internal parse implementation."""
        import pdfplumber

        all_tables: list[SemanticTable] = []
        all_text_blocks: list[TextBlock] = []
        page_texts: dict[int, str] = {}
        warnings: list[str] = []
        total_pages = 0

        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)

            for page_obj in pdf.pages:
                page_num = page_obj.page_number  # 1-based
                page_height = float(page_obj.height)

                try:
                    page_tables, page_blocks, page_text = self._parse_page(
                        page_obj, page_num, page_height, pdf_filename
                    )
                    all_tables.extend(page_tables)
                    all_text_blocks.extend(page_blocks)
                    page_texts[page_num] = page_text
                except Exception as e:
                    self._logger.log_parser_error(pdf_filename, page_num, str(e))
                    warnings.append(f"Page {page_num}: {e}")
                    page_texts[page_num] = ""

        # Quality gate
        quality = self._assess_quality(page_texts, total_pages, pdf_filename)

        # Section extraction
        full_text = "\n".join(page_texts.values())
        abstract_text = self._extract_section(full_text, "abstract", "introduction")
        methods_text = self._extract_section(full_text, "methods", "results")

        return ParsedPDF(
            filename=pdf_filename,
            quality=quality,
            total_pages=total_pages,
            tables=all_tables,
            text_blocks=all_text_blocks,
            abstract_text=abstract_text,
            methods_text=methods_text,
            full_text=full_text,
            page_texts=page_texts,
            warnings=warnings,
        )

    def _parse_page(
        self,
        page_obj: Any,
        page_num: int,
        page_height: float,
        pdf_filename: str,
    ) -> tuple[list[SemanticTable], list[TextBlock], str]:
        """Parse one page with retry. Returns (tables, text_blocks, text)."""
        from tenacity import retry, stop_after_attempt, wait_fixed

        @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
        def _parse_with_retry():
            return self._parse_page_once(page_obj, page_num, page_height, pdf_filename)

        return _parse_with_retry()

    def _parse_page_once(
        self,
        page_obj: Any,
        page_num: int,
        page_height: float,
        pdf_filename: str,
    ) -> tuple[list[SemanticTable], list[TextBlock], str]:
        """Single attempt at parsing one page.

        Extraction order: tables → text blocks with spatial tags → section ranking.
        """
        tables: list[SemanticTable] = []
        text_blocks: list[TextBlock] = []

        # ── Tables ────────────────────────────────────────────────────────────
        raw_tables = page_obj.extract_tables() or []
        table_bboxes = []
        try:
            for tbl_obj in (page_obj.find_tables() or []):
                table_bboxes.append(tbl_obj.bbox)
        except Exception:
            table_bboxes = [None] * len(raw_tables)
        # Pad bbox list to match raw_tables length
        while len(table_bboxes) < len(raw_tables):
            table_bboxes.append(None)

        for tbl_idx, (raw_table, tbl_bbox) in enumerate(zip(raw_tables, table_bboxes)):
            if not raw_table:
                continue
            try:
                sem_table = self._table_parser.parse_table(
                    raw_table=raw_table,
                    page=page_num,
                    table_index=tbl_idx,
                    page_height=page_height,
                    table_bbox=tbl_bbox,
                )
                tables.append(sem_table)
            except Exception as e:
                self._logger.log_parser_error(
                    pdf_filename, page_num,
                    f"Table {tbl_idx} parse error: {e}"
                )

        # ── Text blocks with spatial tags ─────────────────────────────────────
        words = page_obj.extract_words(
            keep_blank_chars=False,
            x_tolerance=3,
            y_tolerance=3,
        ) or []

        # Group words into line-level blocks by y-coordinate proximity
        lines = self._group_words_to_lines(words, y_tolerance=5.0)
        page_text_parts: list[str] = []

        for line_words in lines:
            if not line_words:
                continue
            text = " ".join(w["text"] for w in line_words)
            x0 = min(float(w["x0"]) for w in line_words)
            y0 = min(float(w["top"]) for w in line_words)
            x1 = max(float(w["x1"]) for w in line_words)
            y1 = max(float(w["bottom"]) for w in line_words)
            # Normalise coordinates (pdfplumber already uses top-left origin)
            x0, y0, x1, y1 = normalise_pdfplumber_coords((x0, y0, x1, y1), page_height)
            # Spatial tag format: [PG:{page} | LOC:{x0},{y0},{x1},{y1}]
            spatial_tag = f"[PG:{page_num} | LOC:{x0:.1f},{y0:.1f},{x1:.1f},{y1:.1f}]"
            page_text_parts.append(f"{spatial_tag} {text}")
            text_blocks.append(TextBlock(
                text=text,
                page=page_num,
                x0=x0, y0=y0, x1=x1, y1=y1,
            ))

        page_text = "\n".join(page_text_parts)
        return tables, text_blocks, page_text

    @staticmethod
    def _group_words_to_lines(
        words: list[dict],
        y_tolerance: float = 5.0,
    ) -> list[list[dict]]:
        """Group word dicts into lines based on y-coordinate proximity."""
        if not words:
            return []
        sorted_words = sorted(words, key=lambda w: (w.get("top", 0), w.get("x0", 0)))
        lines: list[list[dict]] = []
        current_line: list[dict] = [sorted_words[0]]
        current_y = float(sorted_words[0].get("top", 0))

        for word in sorted_words[1:]:
            word_y = float(word.get("top", 0))
            if abs(word_y - current_y) <= y_tolerance:
                current_line.append(word)
            else:
                lines.append(current_line)
                current_line = [word]
                current_y = word_y
        if current_line:
            lines.append(current_line)
        return lines

    def _assess_quality(
        self,
        page_texts: dict[int, str],
        total_pages: int,
        pdf_filename: str,
    ) -> PDFQuality:
        """Assess PDF quality from extracted text density.

        Returns PDFQuality enum (never raises).
        """
        if total_pages == 0:
            self._logger.log_pdf_quality_warning(
                pdf_filename, "UNREADABLE", "No pages extracted"
            )
            return PDFQuality.UNREADABLE

        total_chars = sum(len(t) for t in page_texts.values())
        chars_per_page = total_chars / total_pages

        if chars_per_page < 50:
            self._logger.log_pdf_quality_warning(
                pdf_filename, "UNREADABLE",
                f"chars_per_page={chars_per_page:.1f} — likely scanned/image PDF"
            )
            return PDFQuality.UNREADABLE
        elif chars_per_page < 200:
            self._logger.log_pdf_quality_warning(
                pdf_filename, "LOW_DENSITY",
                f"chars_per_page={chars_per_page:.1f} — possible OCR issues"
            )
            return PDFQuality.LOW_DENSITY

        return PDFQuality.OK

    @staticmethod
    def _extract_section(
        full_text: str,
        start_section: str,
        end_section: str,
    ) -> str:
        """Extract text between two section headings (case-insensitive).

        Returns empty string if section not found.
        """
        start_pat = _SECTION_PATTERNS.get(start_section)
        end_pat = _SECTION_PATTERNS.get(end_section)
        if not start_pat:
            return ""

        start_match = start_pat.search(full_text)
        if not start_match:
            return ""

        end_text = full_text[start_match.end():]
        if end_pat:
            end_match = end_pat.search(end_text)
            if end_match:
                return end_text[:end_match.start()].strip()

        return end_text.strip()

    def format_for_llm(
        self,
        parsed: ParsedPDF,
        max_chars: int = 400_000,
    ) -> str:
        """Format a ParsedPDF for LLM context input.

        Includes GFM tables and spatially-tagged text blocks.
        Truncates to max_chars to respect token limits.

        Args:
            parsed: ParsedPDF result
            max_chars: maximum characters to include

        Returns:
            Formatted string for LLM context
        """
        parts: list[str] = []

        # Tables first (highest information density)
        for table in parsed.tables:
            gfm = render_table_as_gfm(table)
            parts.append(gfm)

        # Spatially-tagged text
        parts.append("\n--- FULL TEXT WITH SPATIAL TAGS ---\n")
        parts.append(parsed.full_text)

        combined = "\n\n".join(parts)
        if len(combined) > max_chars:
            combined = combined[:max_chars] + "\n[TRUNCATED]"
        return combined
