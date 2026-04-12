"""
SRMA Extraction Engine — Streamlit application.

Responsibilities:
- Catches SchemaCycleError from template_manager.py → st.error()
- Split-screen: 55% PDF viewer, 45% data editor
- Click-to-highlight with table-bbox fallback
- Design override UI → propagates through StudyMetadata, ROB_TOOL_MAP,
  DESIGN_OVERRIDE audit event
- Multi-sheet export: Extraction_{pdf}, Appraisal_{pdf}, NOS_{pdf},
  StudyMetadata, Audit_Summary
- Does NOT import from utils.errors.PDFUnreadableError in production flow

Import rule: app.py may import from all project modules.
             No module other than app.py may import streamlit.
"""
from __future__ import annotations

import asyncio
import io
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import streamlit as st

import config
from models.extraction_schema import (
    StudyDesign, StudyMetadata, DesignCandidate,
    TreatmentArm, CostTracker, PDFQuality,
    ExtractedVariable, RoBAssessment, NOSResult,
)
from utils.errors import SchemaCycleError
from utils.audit_logger import get_logger, AuditLogger
from utils.rob_framework import ROB_TOOL_MAP, get_tools_for_design
from engine.template_manager import TemplateManager, ParsedSchema
from engine.pdf_processor import PDFProcessor, ParsedPDF
from engine.llm_orchestrator import LLMOrchestrator

# ── Page configuration ─────────────────────────────────────────────────────────

st.set_page_config(
    page_title="SRMA Extraction Engine",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Session state initialisation ───────────────────────────────────────────────

def _init_session_state() -> None:
    defaults: dict[str, Any] = {
        "api_key": "",
        "schema": None,              # ParsedSchema
        "schema_warnings": [],
        "pdf_parsed": {},            # {filename: ParsedPDF}
        "extracted_data": {},        # {filename: list[ExtractedVariable]}
        "rob_assessments": {},       # {filename: RoBAssessment}
        "study_metadata": {},        # {filename: StudyMetadata}
        "design_overrides": {},      # {filename: StudyDesign}
        "highlighted_location": {},  # {filename: ExtractionLocation}
        "active_pdf": None,
        "cost_trackers": {},         # {filename: CostTracker}
        "audit_logger": get_logger("audit_log.jsonl"),
        "extraction_running": False,
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


_init_session_state()

# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("SRMA Extraction Engine")
    st.caption("PRISMA 2020-compliant data extraction")

    st.divider()

    # API key
    api_key_input = st.text_input(
        "Anthropic API Key",
        value=st.session_state.api_key,
        type="password",
        help="Required for LLM extraction. Never stored persistently.",
    )
    if api_key_input != st.session_state.api_key:
        st.session_state.api_key = api_key_input

    st.divider()

    # Schema upload
    st.subheader("1. Upload Schema")
    schema_file = st.file_uploader(
        "Excel extraction schema (.xlsx)",
        type=["xlsx"],
        help="Upload your variable extraction schema. "
             "First row = variable headers. "
             "Optional unit row: first cell = 'unit' or 'units'.",
    )

    if schema_file is not None:
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp.write(schema_file.getbuffer())
            tmp_path = tmp.name

        try:
            mgr = TemplateManager(tmp_path)
            schema = mgr.load()
            st.session_state.schema = schema
            st.session_state.schema_warnings = schema.warnings
            st.success(
                f"Schema loaded: {len(schema.variables)} variables "
                f"across {len(schema.sheet_names)} sheet(s)"
            )
            if schema.warnings:
                with st.expander(f"Schema warnings ({len(schema.warnings)})", expanded=False):
                    for w in schema.warnings:
                        st.warning(w)
        except SchemaCycleError as e:
            st.error(f"Schema cycle error: {e}")
            st.session_state.schema = None
        except Exception as e:
            st.error(f"Failed to load schema: {e}")
            st.session_state.schema = None
        finally:
            os.unlink(tmp_path)

    st.divider()

    # PDF upload
    st.subheader("2. Upload PDFs")
    pdf_files = st.file_uploader(
        "PDF study files",
        type=["pdf"],
        accept_multiple_files=True,
        help="Upload one or more study PDFs for extraction.",
    )

    if pdf_files:
        processor = PDFProcessor()
        for pdf_file in pdf_files:
            fname = pdf_file.name
            if fname not in st.session_state.pdf_parsed:
                with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                    tmp.write(pdf_file.getbuffer())
                    tmp_path = tmp.name
                with st.spinner(f"Parsing {fname}…"):
                    parsed = processor.process(tmp_path, fname)
                st.session_state.pdf_parsed[fname] = parsed
                os.unlink(tmp_path)

                if parsed.quality == PDFQuality.UNREADABLE:
                    st.error(f"{fname}: UNREADABLE (possible scanned PDF)")
                elif parsed.quality == PDFQuality.LOW_DENSITY:
                    st.warning(f"{fname}: LOW DENSITY — extraction may be incomplete")
                else:
                    st.success(
                        f"{fname}: OK — {parsed.total_pages} pages, "
                        f"{len(parsed.tables)} tables"
                    )

    st.divider()

    # Run extraction
    st.subheader("3. Run Extraction")
    run_disabled = (
        not st.session_state.api_key
        or st.session_state.schema is None
        or not st.session_state.pdf_parsed
        or st.session_state.extraction_running
    )

    if st.button(
        "Extract All PDFs",
        disabled=run_disabled,
        type="primary",
        help="Requires API key, schema, and at least one PDF.",
    ):
        st.session_state.extraction_running = True
        _run_extraction_batch()
        st.session_state.extraction_running = False

    if st.session_state.extraction_running:
        st.info("Extraction in progress…")

    st.divider()

    # Export
    st.subheader("4. Export Results")
    if st.session_state.extracted_data:
        xlsx_bytes = _build_export_xlsx()
        st.download_button(
            label="Download Excel Export",
            data=xlsx_bytes,
            file_name="srma_extraction_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        st.caption("No extracted data yet.")


# ── Main content area ──────────────────────────────────────────────────────────

def _run_extraction_batch() -> None:
    """Run extraction for all loaded PDFs."""
    schema: Optional[ParsedSchema] = st.session_state.schema
    if schema is None:
        st.error("No schema loaded.")
        return

    orchestrator = LLMOrchestrator(
        api_key=st.session_state.api_key,
        model="claude-opus-4-6",
    )
    logger: AuditLogger = st.session_state.audit_logger

    for fname, parsed_pdf in st.session_state.pdf_parsed.items():
        if parsed_pdf.quality == PDFQuality.UNREADABLE:
            logger.log_pdf_skipped(fname, "UNREADABLE quality")
            continue

        cost_tracker = CostTracker(pdf_filename=fname)
        st.session_state.cost_trackers[fname] = cost_tracker

        try:
            # Pass 0: design detection
            loop = asyncio.new_event_loop()
            design, confidence, alternatives, warnings = loop.run_until_complete(
                orchestrator.run_pass0(
                    abstract_and_methods=(
                        parsed_pdf.abstract_text + "\n\n" + parsed_pdf.methods_text
                    )[:config.PASS0_MAX_INPUT_TOKENS],
                    pdf_filename=fname,
                    cost_tracker=cost_tracker,
                )
            )
            loop.close()

            # Apply user design override if set
            override = st.session_state.design_overrides.get(fname)
            design_overridden = False
            if override is not None and override != design:
                logger.log_design_override(
                    pdf_filename=fname,
                    original_design=design.value,
                    overridden_design=override.value,
                )
                design = override
                design_overridden = True

            # Block appraisal if confidence too low (and not overridden)
            appraisal_blocked = (
                confidence < config.DESIGN_BLOCK_APPRAISAL_THRESHOLD
                and not design_overridden
            )

            candidate_rob_tools = get_tools_for_design(design)
            metadata = StudyMetadata(
                study_id=fname,
                treatment_arms=[],   # populated by extraction if possible
                primary_outcomes=[],
                population_description="",
                detected_study_design=design,
                design_confidence=confidence,
                design_alternatives=alternatives,
                design_overridden_by_user=design_overridden,
                candidate_rob_tools=candidate_rob_tools,
                pass0_confidence=confidence,
                pass0_warnings=warnings,
            )
            st.session_state.study_metadata[fname] = metadata

            # Mode A extraction (variable extraction)
            llm_input = PDFProcessor().format_for_llm(
                parsed_pdf,
                max_chars=config.EXTRACTION_MAX_INPUT_TOKENS * 4,  # approx chars→tokens
            )
            extracted_vars: list[ExtractedVariable] = []
            loop2 = asyncio.new_event_loop()

            for var_spec in schema.variables:
                var_context = (
                    f"variable: {var_spec.raw_header}\n"
                    f"canonical_name: {var_spec.canonical_name}\n"
                    f"expected_unit: {var_spec.expected_unit or 'not specified'}"
                )
                result = loop2.run_until_complete(
                    orchestrator.extract_variable(
                        variable_context=var_context,
                        pdf_text=llm_input,
                        pdf_filename=fname,
                        cost_tracker=cost_tracker,
                        study_metadata=metadata,
                    )
                )
                if result is not None:
                    extracted_vars.append(result)
                    logger.log_extraction(
                        pdf_filename=fname,
                        variable=result.variable,
                        canonical_name=result.canonical_name,
                        status=result.status,
                        value=result.value,
                        confidence=result.confidence,
                    )

            loop2.close()
            st.session_state.extracted_data[fname] = extracted_vars

        except Exception as e:
            from utils.errors import CostCapExceededError
            if isinstance(e, CostCapExceededError):
                logger.log_cost_cap_hit(
                    fname,
                    cost_tracker.estimated_usd,
                    config.COST_HARD_STOP_USD,
                )
                st.warning(f"{fname}: Cost cap hit — skipping. ({e})")
            else:
                logger.log_parser_error(fname, None, str(e))
                st.error(f"{fname}: Extraction error — {e}")


def _build_export_xlsx() -> bytes:
    """Build a multi-sheet Excel export and return bytes."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Per-PDF extraction sheets
        for fname, vars_list in st.session_state.extracted_data.items():
            sheet_name = f"Extraction_{Path(fname).stem}"[:31]  # Excel 31-char limit
            if vars_list:
                rows = []
                for v in vars_list:
                    row = {
                        "variable": v.variable,
                        "canonical_name": v.canonical_name,
                        "value": v.value,
                        "unit": v.unit,
                        "status": v.status,
                        "confidence": v.confidence,
                        "study_arm": v.study_arm,
                        "arm_role": v.arm_role,
                        "evidence_quote": v.evidence_quote,
                        "uncertainty_note": v.uncertainty_note,
                        "source_type": v.source_type,
                        "verification_flag": v.verification_flag,
                        "calculation_trace": v.calculation_trace,
                        "page": v.location.page if v.location else None,
                        "table_id": v.location.table_id if v.location else None,
                        "outcome_name": v.context.outcome_name if v.context else None,
                        "timepoint": v.context.timepoint if v.context else None,
                        "analysis_population": v.context.analysis_population if v.context else None,
                    }
                    # Append evidence as cell comment if configured
                    if config.EXPORT_EVIDENCE_AS_CELL_COMMENT and v.evidence_quote:
                        row["evidence_quote_comment"] = v.evidence_quote
                    rows.append(row)
                df = pd.DataFrame(rows)
            else:
                df = pd.DataFrame()
            df.to_excel(writer, sheet_name=sheet_name, index=False)

        # Per-PDF appraisal sheets
        for fname, assessment in st.session_state.rob_assessments.items():
            sheet_name = f"Appraisal_{Path(fname).stem}"[:31]
            rows = []
            for tool, signals_list in assessment.signals.items():
                for sig in signals_list:
                    rows.append({
                        "tool": tool,
                        "domain_id": sig.domain_id,
                        "domain_name": sig.domain_name,
                        "signalling_question": sig.signalling_question,
                        "signal_answer": sig.signal_answer,
                        "status": sig.status,
                        "evidence_quote": sig.evidence_quote,
                        "source_type": sig.source_type,
                    })
            pd.DataFrame(rows).to_excel(writer, sheet_name=sheet_name, index=False)

            # NOS sheet if applicable
            if assessment.nos_result is not None:
                nos = assessment.nos_result
                nos_sheet = f"NOS_{Path(fname).stem}"[:31]
                nos_rows = [
                    {"item_id": k, "evidence": v}
                    for k, v in nos.item_evidence.items()
                ]
                if nos.item_stars:
                    for row in nos_rows:
                        row["stars"] = nos.item_stars.get(row["item_id"])
                pd.DataFrame(nos_rows).to_excel(writer, sheet_name=nos_sheet, index=False)

        # StudyMetadata sheet
        meta_rows = []
        for fname, meta in st.session_state.study_metadata.items():
            meta_rows.append({
                "study_id": meta.study_id,
                "detected_study_design": meta.detected_study_design.value,
                "design_confidence": meta.design_confidence,
                "design_overridden_by_user": meta.design_overridden_by_user,
                "candidate_rob_tools": json.dumps(meta.candidate_rob_tools),
                "design_alternatives": json.dumps([
                    {"design": c.design.value, "confidence": c.confidence}
                    for c in meta.design_alternatives
                ]),
                "tool_mismatch_warning": st.session_state.rob_assessments.get(
                    fname, type("_", (), {"tool_mismatch_warning": None})()
                ).tool_mismatch_warning,
                "pass0_confidence": meta.pass0_confidence,
                "pass0_warnings": "; ".join(meta.pass0_warnings),
            })
        if meta_rows:
            pd.DataFrame(meta_rows).to_excel(
                writer, sheet_name="StudyMetadata", index=False
            )

        # Audit Summary sheet
        logger: AuditLogger = st.session_state.audit_logger
        cost_df = logger.get_cost_summary()
        if not cost_df.empty:
            cost_df.to_excel(writer, sheet_name="Audit_Summary", index=False)

    output.seek(0)
    return output.read()


# ── Main view ──────────────────────────────────────────────────────────────────

pdf_names = list(st.session_state.pdf_parsed.keys())

if not pdf_names:
    st.info(
        "Upload an Excel schema and one or more PDF files using the sidebar to begin."
    )
    st.stop()

# PDF selector
active_pdf = st.selectbox(
    "Select active PDF",
    options=pdf_names,
    index=0,
    key="active_pdf_selector",
)
st.session_state.active_pdf = active_pdf
if active_pdf is None or active_pdf not in st.session_state.pdf_parsed:
    st.info("Select a PDF to continue.")
    st.stop()
parsed_pdf: ParsedPDF = st.session_state.pdf_parsed[active_pdf]

# Design override UI
st.subheader("Study Design")
meta = st.session_state.study_metadata.get(active_pdf)
if meta is not None:
    current_design = st.session_state.design_overrides.get(
        active_pdf, meta.detected_study_design
    )
    col_design, col_conf = st.columns([3, 1])
    with col_design:
        design_options = [d for d in StudyDesign]
        design_labels = [d.value for d in design_options]
        current_idx = design_options.index(current_design) if current_design in design_options else 0
        new_design_label = st.selectbox(
            "Study design (override auto-detection)",
            options=design_labels,
            index=current_idx,
            help=(
                f"Auto-detected: {meta.detected_study_design.value} "
                f"(confidence={meta.design_confidence:.2f}). "
                "Override only if auto-detection is wrong."
            ),
        )
        new_design = StudyDesign(new_design_label)
    with col_conf:
        st.metric(
            "Detection confidence",
            f"{meta.design_confidence:.0%}",
            delta=None,
        )
    if new_design != current_design:
        st.session_state.design_overrides[active_pdf] = new_design
        logger: AuditLogger = st.session_state.audit_logger
        logger.log_design_override(
            pdf_filename=active_pdf,
            original_design=current_design.value,
            overridden_design=new_design.value,
        )
        st.warning(
            f"Design overridden: {current_design.value} → {new_design.value}. "
            "Re-run extraction to apply."
        )
    if meta.pass0_warnings:
        with st.expander("Pass 0 warnings", expanded=False):
            for w in meta.pass0_warnings:
                st.warning(w)
else:
    st.caption("Run extraction to see design detection results.")

st.divider()

# Split-screen: 55% PDF viewer, 45% data editor
viewer_col, data_col = st.columns([55, 45])

with viewer_col:
    st.subheader("PDF Viewer")

    if parsed_pdf.quality == PDFQuality.UNREADABLE:
        st.error(
            f"{active_pdf} is UNREADABLE. "
            "Possible scanned/image PDF. Consider OCR preprocessing."
        )
    else:
        # Display PDF using streamlit-pdf-viewer if available
        try:
            from streamlit_pdf_viewer import pdf_viewer  # type: ignore
            # Locate the uploaded PDF bytes from session
            pdf_bytes = st.session_state.get(f"_pdf_bytes_{active_pdf}")
            if pdf_bytes:
                highlighted_loc = st.session_state.highlighted_location.get(active_pdf)
                annotations = []
                if highlighted_loc:
                    from utils.coordinate_utils import build_highlight_annotation
                    ann = build_highlight_annotation(highlighted_loc, color=(1.0, 1.0, 0.0))
                    if ann.get("has_bbox"):
                        annotations.append({
                            "page": ann["page"],
                            "x": ann["x0"],
                            "y": ann["y0"],
                            "width": ann["x1"] - ann["x0"],
                            "height": ann["y1"] - ann["y0"],
                            "color": "rgba(255, 255, 0, 0.4)",
                        })
                pdf_viewer(
                    pdf_bytes,
                    annotations=annotations,
                    height=700,
                )
            else:
                st.info(
                    "PDF preview not available. "
                    "Re-upload the PDF to enable the viewer."
                )
        except ImportError:
            st.info(
                "streamlit-pdf-viewer not installed. "
                "Install it to enable PDF highlighting."
            )

        # Page text preview (fallback / always shown)
        with st.expander("Page text preview", expanded=False):
            page_num = st.number_input(
                "Page", min_value=1,
                max_value=parsed_pdf.total_pages,
                value=1,
            )
            page_text = parsed_pdf.page_texts.get(page_num, "")
            st.text_area(
                f"Page {page_num} text",
                value=page_text[:4000],
                height=400,
                disabled=True,
            )

with data_col:
    st.subheader("Extracted Data")

    extracted = st.session_state.extracted_data.get(active_pdf, [])
    if not extracted:
        st.info("No extracted data yet. Run extraction from the sidebar.")
    else:
        # Build DataFrame for display
        rows = []
        for v in extracted:
            rows.append({
                "variable": v.variable,
                "canonical": v.canonical_name,
                "value": v.value,
                "unit": v.unit,
                "status": v.status,
                "conf": f"{v.confidence:.2f}",
                "arm": v.study_arm,
                "page": v.location.page if v.location else None,
                "evidence": (v.evidence_quote or "")[:80],
            })
        df = pd.DataFrame(rows)

        # Colour-code status
        def _status_style(val: str) -> str:
            colours = {
                "EXTRACTED": "background-color: #d4edda",
                "CALCULATED": "background-color: #cce5ff",
                "NOT_REPORTED": "background-color: #f8d7da",
                "AMBIGUOUS": "background-color: #fff3cd",
                "EXTRACTION_FAILED": "background-color: #f8d7da; color: #721c24",
            }
            return colours.get(val, "")

        styled = df.style.applymap(_status_style, subset=["status"])
        selected = st.dataframe(
            styled,
            use_container_width=True,
            height=500,
            on_select="rerun",
            selection_mode="single-row",
        )

        # Click-to-highlight: update highlighted location from selected row
        if (
            selected
            and selected.get("selection")
            and selected["selection"].get("rows")
        ):
            row_idx = selected["selection"]["rows"][0]
            if row_idx < len(extracted):
                clicked_var = extracted[row_idx]
                st.session_state.highlighted_location[active_pdf] = clicked_var.location
                # Show detail panel
                with st.expander("Variable detail", expanded=True):
                    st.json(clicked_var.model_dump())

    # RoB assessment tab
    st.subheader("Risk of Bias")
    assessment = st.session_state.rob_assessments.get(active_pdf)
    if assessment is None:
        st.info("No RoB assessment yet.")
    else:
        st.write(f"**Tool(s) applied:** {', '.join(assessment.tools_applied)}")
        st.write(f"**Assessment source:** {assessment.assessment_source}")
        if assessment.tool_mismatch_warning:
            st.warning(assessment.tool_mismatch_warning)
        if assessment.overall_judgements:
            st.write("**Overall judgements:**")
            for tool, jdg in assessment.overall_judgements.items():
                st.write(f"- {tool}: {jdg}")

    # Cost tracker
    cost = st.session_state.cost_trackers.get(active_pdf)
    if cost:
        st.divider()
        cost_col1, cost_col2 = st.columns(2)
        with cost_col1:
            st.metric("Estimated cost (USD)", f"${cost.estimated_usd:.4f}")
            if cost.estimated_usd >= config.COST_WARN_USD:
                st.warning(f"Cost warning: >${config.COST_WARN_USD:.2f}")
        with cost_col2:
            st.metric("Input tokens", f"{cost.total_input_tokens:,}")
            st.metric("Output tokens", f"{cost.total_output_tokens:,}")

# ── Audit log viewer ───────────────────────────────────────────────────────────

with st.expander("Audit log", expanded=False):
    logger_inst: AuditLogger = st.session_state.audit_logger
    if active_pdf:
        audit_df = logger_inst.get_audit_dataframe(active_pdf)
        if not audit_df.empty:
            st.dataframe(audit_df, use_container_width=True, height=300)
        else:
            st.info("No audit events for this PDF yet.")
