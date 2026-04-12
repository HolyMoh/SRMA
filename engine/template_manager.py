"""
Excel schema template manager.

Responsibilities:
- Parse user-uploaded Excel schema via openpyxl (all sheets)
- Resolve column headers via variable_dictionary.resolve()
- Log SYNONYM_LOAD events for each resolved header
- Build dependency graph via networkx; raise SchemaCycleError on cycle
- Detect unit row (first cell matches r'(?i)^(unit|units|expected_unit)$')
- After extraction: walk sorted graph, call calculators for unresolved nodes

Import direction: config, utils.errors, utils.variable_dictionary,
                  utils.audit_logger, utils.calculators
Does NOT import models.extraction_schema (avoids circular dep on BaseModel).
Does NOT import streamlit — SchemaCycleError is raised here, caught by app.py.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import networkx as nx
import openpyxl

import config
from utils.errors import SchemaCycleError
from utils.variable_dictionary import resolve
from utils.audit_logger import get_logger
import utils.calculators as calculators

# ── Compiled patterns ──────────────────────────────────────────────────────────

_FORMULA_TOKEN_RE = re.compile(r'[A-Za-z_][A-Za-z0-9_]+')
_UNIT_ROW_RE = re.compile(r'(?i)^(unit|units|expected_unit)$')

# Excel built-in function names to filter out when parsing formula dependencies
_EXCEL_BUILTINS: frozenset[str] = frozenset({
    "ABS", "AND", "AVERAGE", "AVERAGEIF", "AVERAGEIFS", "CEILING", "CHOOSE",
    "CONCATENATE", "COUNT", "COUNTA", "COUNTIF", "COUNTIFS", "DATE", "DATEDIF",
    "DAY", "EDATE", "EOMONTH", "EXACT", "FALSE", "FIND", "FLOOR", "HLOOKUP",
    "HOUR", "IF", "IFERROR", "IFNA", "INDEX", "INDIRECT", "INT", "ISBLANK",
    "ISERROR", "ISNA", "ISNUMBER", "ISTEXT", "LEFT", "LEN", "LOOKUP", "LOWER",
    "MATCH", "MAX", "MID", "MIN", "MOD", "MONTH", "NOT", "NOW", "OFFSET",
    "OR", "RAND", "RANDBETWEEN", "REPLACE", "REPT", "RIGHT", "ROUND",
    "ROUNDDOWN", "ROUNDUP", "ROW", "ROWS", "SEARCH", "SUM", "SUMIF", "SUMIFS",
    "SUMPRODUCT", "TEXT", "TODAY", "TRIM", "TRUE", "UPPER", "VLOOKUP", "YEAR",
    "NA", "ISBLANK", "COLUMN", "COLUMNS", "LN", "LOG", "LOG10", "POWER",
    "SQRT", "EXP", "PI", "SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN",
})


# ── Data structures ────────────────────────────────────────────────────────────

@dataclass
class VariableSpec:
    """Specification for a single variable from the schema template."""
    raw_header: str
    canonical_name: str
    resolution_score: float
    sheet_name: str
    col_index: int        # 0-based column index in sheet
    expected_unit: Optional[str] = None   # from unit row; None if no unit row
    formula: Optional[str] = None         # raw formula string from cell, if any
    formula_deps: list[str] = field(default_factory=list)   # canonical dep names
    extra_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ParsedSchema:
    """Result of parsing all sheets from the Excel template."""
    variables: list[VariableSpec]
    dependency_graph: nx.DiGraph
    topological_order: list[str]    # canonical names in computation order
    user_synonyms: dict[str, list[str]]  # extra synonyms discovered from headers
    sheet_names: list[str]
    warnings: list[str]


# ── Parser ─────────────────────────────────────────────────────────────────────

class TemplateManager:
    """Parse and manage an Excel extraction schema template."""

    def __init__(self, excel_path: str | Path) -> None:
        self._path = Path(excel_path)
        self._logger = get_logger()

    def load(self) -> ParsedSchema:
        """Parse the Excel file and return a ParsedSchema.

        Raises:
            SchemaCycleError: if a cycle is detected in the dependency graph.
            FileNotFoundError: if the Excel path does not exist.
        """
        if not self._path.exists():
            raise FileNotFoundError(f"Schema template not found: {self._path}")

        wb = openpyxl.load_workbook(self._path, data_only=False)
        sheet_names = wb.sheetnames
        all_variables: list[VariableSpec] = []
        warnings: list[str] = []
        user_synonyms: dict[str, list[str]] = {}

        for sheet_name in sheet_names:
            ws = wb[sheet_name]
            sheet_vars, sheet_warns, sheet_synonyms = self._parse_sheet(
                ws, sheet_name
            )
            all_variables.extend(sheet_vars)
            warnings.extend(sheet_warns)
            for canonical, variants in sheet_synonyms.items():
                user_synonyms.setdefault(canonical, []).extend(variants)

        # Build dependency graph
        graph = nx.DiGraph()
        for var in all_variables:
            graph.add_node(var.canonical_name)
        for var in all_variables:
            for dep in var.formula_deps:
                graph.add_edge(dep, var.canonical_name)  # dep → var (dep must come first)

        # Cycle detection
        try:
            topo_order = list(nx.topological_sort(graph))
        except nx.NetworkXUnfeasible:
            # Cycle detected — identify it and raise SchemaCycleError
            cycles = list(nx.simple_cycles(graph))
            cycle_strs = [" → ".join(c + [c[0]]) for c in cycles[:3]]
            raise SchemaCycleError(
                f"Cycle detected in variable dependency graph: "
                f"{'; '.join(cycle_strs)}. "
                f"Please remove circular formula dependencies from the schema."
            )

        return ParsedSchema(
            variables=all_variables,
            dependency_graph=graph,
            topological_order=topo_order,
            user_synonyms=user_synonyms,
            sheet_names=sheet_names,
            warnings=warnings,
        )

    def _parse_sheet(
        self,
        ws: openpyxl.worksheet.worksheet.Worksheet,
        sheet_name: str,
    ) -> tuple[list[VariableSpec], list[str], dict[str, list[str]]]:
        """Parse a single worksheet.

        Returns (variables, warnings, user_synonyms_from_this_sheet).
        """
        variables: list[VariableSpec] = []
        warnings: list[str] = []
        user_synonyms: dict[str, list[str]] = {}

        if ws.max_row < 1 or ws.max_column < 1:
            return variables, warnings, user_synonyms

        # Read all rows into list-of-lists for easy indexing
        rows: list[list[Any]] = []
        for row in ws.iter_rows(values_only=False):
            rows.append([cell for cell in row])

        if not rows:
            return variables, warnings, user_synonyms

        # Header row = first row
        header_row = rows[0]

        # Unit row detection: scan first few rows for a row whose first cell
        # matches r'(?i)^(unit|units|expected_unit)$'
        unit_row_index: Optional[int] = None
        for row_i, row in enumerate(rows[1:], start=1):
            if row and row[0] is not None:
                first_val = str(row[0].value or "").strip()
                if _UNIT_ROW_RE.match(first_val):
                    unit_row_index = row_i
                    break

        # Build expected units map: {col_index: unit_string}
        expected_units: dict[int, str] = {}
        if unit_row_index is not None:
            unit_row = rows[unit_row_index]
            for col_i, cell in enumerate(unit_row):
                if col_i == 0:
                    continue  # skip label cell
                val = str(cell.value or "").strip()
                if val:
                    expected_units[col_i] = val

        # Process each header column
        for col_i, header_cell in enumerate(header_row):
            raw_header = str(header_cell.value or "").strip()
            if not raw_header:
                continue

            # Resolve header to canonical name
            canonical_name, score = resolve(raw_header)
            if canonical_name == "UNRESOLVED":
                # Add as user synonym candidate with raw_header as variant
                canonical_name = self._slugify(raw_header)
                score = 0.0
                warnings.append(
                    f"[{sheet_name}] col {col_i}: '{raw_header}' unresolved "
                    f"(score={score:.1f}), using slug '{canonical_name}'"
                )
            else:
                self._logger.log_synonym_load(
                    pdf_filename="schema",
                    canonical_name=canonical_name,
                    resolved_header=raw_header,
                    score=score,
                )
                # Register as extra synonym for future use
                user_synonyms.setdefault(canonical_name, []).append(raw_header)

            # Check for formula in any non-header cell of this column
            formula: Optional[str] = None
            formula_deps: list[str] = []
            for row_i, row in enumerate(rows[1:], start=1):
                if row_i == unit_row_index:
                    continue
                if col_i >= len(row):
                    continue
                cell = row[col_i]
                cell_val = cell.value
                if cell_val and isinstance(cell_val, str) and cell_val.startswith("="):
                    formula = cell_val
                    formula_deps = self._extract_formula_deps(formula)
                    break

            var_spec = VariableSpec(
                raw_header=raw_header,
                canonical_name=canonical_name,
                resolution_score=score,
                sheet_name=sheet_name,
                col_index=col_i,
                expected_unit=expected_units.get(col_i),
                formula=formula,
                formula_deps=formula_deps,
            )
            variables.append(var_spec)

        return variables, warnings, user_synonyms

    @staticmethod
    def _extract_formula_deps(formula: str) -> list[str]:
        """Extract variable name tokens from an Excel formula string.

        Filters out Excel built-in function names.
        Returns a list of canonical-looking variable names.
        """
        tokens = _FORMULA_TOKEN_RE.findall(formula)
        deps: list[str] = []
        seen: set[str] = set()
        for tok in tokens:
            if tok.upper() in _EXCEL_BUILTINS:
                continue
            if tok in seen:
                continue
            seen.add(tok)
            deps.append(tok)
        return deps

    @staticmethod
    def _slugify(text: str) -> str:
        """Convert a raw header string to a safe canonical slug."""
        slug = re.sub(r'[^a-z0-9]+', '_', text.lower()).strip('_')
        return slug or "unknown"

    def resolve_calculated_variables(
        self,
        extracted: dict[str, Any],
        schema: ParsedSchema,
    ) -> dict[str, tuple[Any, str]]:
        """Walk the topological order and compute derived variables.

        Args:
            extracted: {canonical_name: numeric_value} for already-extracted vars
            schema: ParsedSchema from load()

        Returns:
            {canonical_name: (result, trace)} for each successfully computed var
        """
        results: dict[str, tuple[Any, str]] = {}
        resolved_values = dict(extracted)

        for canonical_name in schema.topological_order:
            # Find spec for this variable
            spec = next(
                (v for v in schema.variables if v.canonical_name == canonical_name),
                None,
            )
            if spec is None or spec.formula is None:
                continue
            if canonical_name in resolved_values:
                continue  # already extracted

            # Check all deps are resolved
            dep_values: dict[str, Any] = {}
            all_resolved = True
            for dep in spec.formula_deps:
                if dep in resolved_values:
                    dep_values[dep] = resolved_values[dep]
                else:
                    all_resolved = False
                    break

            if not all_resolved:
                results[canonical_name] = (
                    None,
                    f"CALCULATION_FAILED: dependency '{dep}' not resolved",
                )
                continue

            # Evaluate via sympy
            result, trace = calculators.evaluate_formula(
                spec.formula.lstrip("="),
                dep_values,
            )
            results[canonical_name] = (result, trace)
            if result is not None:
                resolved_values[canonical_name] = result

        return results
