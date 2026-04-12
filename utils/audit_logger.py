"""
Append-only JSONL audit logger for the SRMA engine.

18 event types:
  EXTRACTION, CALCULATION, VALIDATION_REJECT, API_CALL, API_RETRY,
  COST_CAP_HIT, IRR_RESULT, PDF_QUALITY_WARNING, PDF_SKIPPED,
  PARSER_ERROR, PASS0_FAILED, PASS0_WARNING, STUDY_DESIGN_DETECTED,
  DESIGN_OVERRIDE, SYNONYM_LOAD, ROB_SIGNAL, ROB_JUDGMENT,
  NOS_ITEM_EVIDENCE

Imports: config, standard library only. No project models.
"""
from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import config

# ── Event type literal ─────────────────────────────────────────────────────────

AuditEventType = Literal[
    "EXTRACTION",
    "CALCULATION",
    "VALIDATION_REJECT",
    "API_CALL",
    "API_RETRY",
    "COST_CAP_HIT",
    "IRR_RESULT",
    "PDF_QUALITY_WARNING",
    "PDF_SKIPPED",
    "PARSER_ERROR",
    "PASS0_FAILED",
    "PASS0_WARNING",
    "STUDY_DESIGN_DETECTED",
    "DESIGN_OVERRIDE",
    "SYNONYM_LOAD",
    "ROB_SIGNAL",
    "ROB_JUDGMENT",
    "NOS_ITEM_EVIDENCE",
]

_VALID_EVENT_TYPES: set[str] = {
    "EXTRACTION", "CALCULATION", "VALIDATION_REJECT", "API_CALL", "API_RETRY",
    "COST_CAP_HIT", "IRR_RESULT", "PDF_QUALITY_WARNING", "PDF_SKIPPED",
    "PARSER_ERROR", "PASS0_FAILED", "PASS0_WARNING", "STUDY_DESIGN_DETECTED",
    "DESIGN_OVERRIDE", "SYNONYM_LOAD", "ROB_SIGNAL", "ROB_JUDGMENT",
    "NOS_ITEM_EVIDENCE",
}

# ── Logger class ───────────────────────────────────────────────────────────────

class AuditLogger:
    """Thread-safe append-only JSONL audit logger."""

    def __init__(self, log_path: str = "audit_log.jsonl") -> None:
        self._log_path = Path(log_path)
        self._lock = threading.Lock()
        self._log_path.parent.mkdir(parents=True, exist_ok=True)

    def log(
        self,
        event_type: str,
        pdf_filename: str,
        payload: dict[str, Any],
    ) -> None:
        """Append one audit event to the JSONL file.

        event_type must be one of the 18 defined types.
        Silently ignores unknown event types to avoid crashing the batch.
        """
        if event_type not in _VALID_EVENT_TYPES:
            return
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "event": event_type,
            "pdf": pdf_filename,
            **payload,
        }
        line = json.dumps(record, default=str)
        with self._lock:
            with open(self._log_path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")

    # ── Typed helpers ──────────────────────────────────────────────────────────

    def log_extraction(
        self,
        pdf_filename: str,
        variable: str,
        canonical_name: str,
        status: str,
        value: Any = None,
        confidence: float = 0.0,
    ) -> None:
        self.log("EXTRACTION", pdf_filename, {
            "variable": variable,
            "canonical_name": canonical_name,
            "status": status,
            "value": value,
            "confidence": confidence,
        })

    def log_calculation(
        self,
        pdf_filename: str,
        variable: str,
        result: Any,
        trace: str,
    ) -> None:
        self.log("CALCULATION", pdf_filename, {
            "variable": variable,
            "result": result,
            "calculation_trace": trace,
        })

    def log_validation_reject(
        self,
        pdf_filename: str,
        variable: str,
        error_msg: str,
        raw_llm_output: str,
    ) -> None:
        self.log("VALIDATION_REJECT", pdf_filename, {
            "variable": variable,
            "error": error_msg,
            "raw_llm_output": raw_llm_output[:2000],  # truncate for storage
        })

    def log_api_call(
        self,
        pdf_filename: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        estimated_usd: float,
    ) -> None:
        self.log("API_CALL", pdf_filename, {
            "model": model,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "estimated_usd": estimated_usd,
        })

    def log_api_retry(
        self,
        pdf_filename: str,
        attempt: int,
        error_type: str,
        error_msg: str,
    ) -> None:
        self.log("API_RETRY", pdf_filename, {
            "attempt": attempt,
            "error_type": error_type,
            "error_msg": error_msg,
        })

    def log_cost_cap_hit(
        self,
        pdf_filename: str,
        estimated_usd: float,
        hard_stop_usd: float,
    ) -> None:
        self.log("COST_CAP_HIT", pdf_filename, {
            "estimated_usd": estimated_usd,
            "hard_stop_usd": hard_stop_usd,
        })

    def log_irr_result(
        self,
        pdf_filename: str,
        variable: str,
        variable_type: str,
        n_compared: int,
        cohen_kappa: float | None = None,
        percentage_agreement: float | None = None,
    ) -> None:
        self.log("IRR_RESULT", pdf_filename, {
            "variable": variable,
            "variable_type": variable_type,
            "n_compared": n_compared,
            "cohen_kappa": cohen_kappa,
            "percentage_agreement": percentage_agreement,
        })

    def log_pdf_quality_warning(
        self,
        pdf_filename: str,
        quality: str,
        detail: str,
    ) -> None:
        self.log("PDF_QUALITY_WARNING", pdf_filename, {
            "quality": quality,
            "detail": detail,
        })

    def log_pdf_skipped(
        self,
        pdf_filename: str,
        reason: str,
    ) -> None:
        self.log("PDF_SKIPPED", pdf_filename, {"reason": reason})

    def log_parser_error(
        self,
        pdf_filename: str,
        page: int | None,
        error_msg: str,
    ) -> None:
        self.log("PARSER_ERROR", pdf_filename, {
            "page": page,
            "error": error_msg,
        })

    def log_pass0_failed(
        self,
        pdf_filename: str,
        error_msg: str,
    ) -> None:
        self.log("PASS0_FAILED", pdf_filename, {"error": error_msg})

    def log_pass0_warning(
        self,
        pdf_filename: str,
        warning: str,
    ) -> None:
        self.log("PASS0_WARNING", pdf_filename, {"warning": warning})

    def log_study_design_detected(
        self,
        pdf_filename: str,
        design: str,
        confidence: float,
        method: str,
    ) -> None:
        self.log("STUDY_DESIGN_DETECTED", pdf_filename, {
            "design": design,
            "confidence": confidence,
            "method": method,
        })

    def log_design_override(
        self,
        pdf_filename: str,
        original_design: str,
        overridden_design: str,
    ) -> None:
        self.log("DESIGN_OVERRIDE", pdf_filename, {
            "original_design": original_design,
            "overridden_design": overridden_design,
        })

    def log_synonym_load(
        self,
        pdf_filename: str,
        canonical_name: str,
        resolved_header: str,
        score: float,
    ) -> None:
        self.log("SYNONYM_LOAD", pdf_filename, {
            "canonical_name": canonical_name,
            "resolved_header": resolved_header,
            "score": score,
        })

    def log_rob_signal(
        self,
        pdf_filename: str,
        study_id: str,
        tool: str,
        domain_id: str,
        status: str,
        signal_answer: str | None = None,
    ) -> None:
        self.log("ROB_SIGNAL", pdf_filename, {
            "study_id": study_id,
            "tool": tool,
            "domain_id": domain_id,
            "status": status,
            "signal_answer": signal_answer,
        })

    def log_rob_judgment(
        self,
        pdf_filename: str,
        study_id: str,
        tool: str,
        domain_id: str,
        judgment: str | None,
        status: str,
    ) -> None:
        self.log("ROB_JUDGMENT", pdf_filename, {
            "study_id": study_id,
            "tool": tool,
            "domain_id": domain_id,
            "judgment": judgment,
            "status": status,
        })

    def log_nos_item_evidence(
        self,
        pdf_filename: str,
        study_id: str,
        item_id: str,
        evidence_text: str,
    ) -> None:
        self.log("NOS_ITEM_EVIDENCE", pdf_filename, {
            "study_id": study_id,
            "item_id": item_id,
            "evidence_text": evidence_text[:500],
        })

    # ── Query helpers ──────────────────────────────────────────────────────────

    def _read_records(self) -> list[dict[str, Any]]:
        """Read all JSONL records from the log file."""
        if not self._log_path.exists():
            return []
        records = []
        with open(self._log_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        return records

    def get_audit_dataframe(self, pdf_filename: str) -> "Any":
        """Return a pandas DataFrame of audit records for the given PDF.

        Imports pandas lazily to avoid hard dependency at module load.
        """
        import pandas as pd
        records = [r for r in self._read_records() if r.get("pdf") == pdf_filename]
        return pd.DataFrame(records) if records else pd.DataFrame()

    def get_cost_summary(self) -> "Any":
        """Return a pandas DataFrame summarising per-PDF costs from API_CALL events."""
        import pandas as pd
        records = [r for r in self._read_records() if r.get("event") == "API_CALL"]
        if not records:
            return pd.DataFrame(
                columns=["pdf", "total_input_tokens", "total_output_tokens", "estimated_usd"]
            )
        df = pd.DataFrame(records)
        summary = (
            df.groupby("pdf")
            .agg(
                total_input_tokens=("input_tokens", "sum"),
                total_output_tokens=("output_tokens", "sum"),
                estimated_usd=("estimated_usd", "sum"),
            )
            .reset_index()
        )
        return summary

    def get_irr_summary(self) -> "Any":
        """Return a pandas DataFrame of IRR_RESULT events."""
        import pandas as pd
        records = [r for r in self._read_records() if r.get("event") == "IRR_RESULT"]
        return pd.DataFrame(records) if records else pd.DataFrame()


# ── Module-level default logger instance ──────────────────────────────────────

_default_logger: AuditLogger | None = None


def get_logger(log_path: str = "audit_log.jsonl") -> AuditLogger:
    """Return the module-level default logger, creating it on first call."""
    global _default_logger
    if _default_logger is None:
        _default_logger = AuditLogger(log_path=log_path)
    return _default_logger
