"""
LLM orchestration for the SRMA Extraction Engine.

Stage 1: Module-level system prompt string constants.
Stage 2: Full orchestration implementation with asyncio semaphore,
         tenacity retry, cost tracking, and Pydantic validation.
"""
from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Optional

import config
from models.extraction_schema import (
    CostTracker, ExtractedVariable, RoBSignal, RoBJudgment,
    StudyDesign, StudyMetadata, DesignCandidate,
)
from utils.errors import CostCapExceededError
from utils.audit_logger import get_logger

# ── System prompt constants ────────────────────────────────────────────────────

EXTRACTION_SYSTEM_PROMPT = """
You are a clinical data extraction specialist supporting a PRISMA 2020-compliant systematic review.

RULES — violations cause rejection and one retry:

1. Extract ONLY values explicitly and literally stated in the provided text or GFM tables. No inference, no clinical reasoning, no extrapolation.

2. THE INTEGER ZERO IS A VALID CLINICAL VALUE. An explicitly stated zero must be extracted as value=0, status="EXTRACTED". Never return null for an explicitly reported zero.

3. If a value is absent: value=null, status="NOT_REPORTED".

4. If the same variable appears with conflicting values: status="AMBIGUOUS". Include all conflicting occurrences with page numbers in evidence_quote.

5. Every non-null value requires a verbatim evidence_quote from the source. Never modify, annotate, or paraphrase it. If confidence is below 0.75, populate the separate uncertainty_note field — never alter evidence_quote.

6. ClinicalContextBundle: populate outcome_name, timepoint, analysis_population, estimate_type, comparison_group, effect_direction, and adjustment_covariates ONLY from explicit source statements. Leave null if absent. Do not infer analysis_population from study design type.

7. Spatial coordinates: copy exact values from [PG:n | LOC:x0,y0,x1,y1] tags into location. For GFM table values: set table_id and cell_row/cell_col (zero-based, header rows excluded); leave x0/y0/x1/y1 null.

8. source_type: classify as text_block, table_cell, footnote, header_derived, or caption_derived.

9. Do not extract from figures, forest plots, Kaplan-Meier curves, bar charts, or visual-only content.

10. Arm attribution: use only arm names from study_metadata.treatment_arms. Uncertain → status="AMBIGUOUS", study_arm=null. Never invent arm names.

11. Do not convert units. Set unit_mismatch=true only if an expected unit is provided in the extraction context and the PDF unit differs.

12. Confidence: 0.95–1.0 = explicit verbatim; 0.75–0.94 = clearly stated; below 0.75 = lower confidence and populate uncertainty_note. Maximum 0.85 if value is not visible verbatim in evidence_quote.

13. Return valid JSON matching ExtractedVariable schema exactly. No markdown fences, no preamble.
"""

ROB_SIGNAL_SYSTEM_PROMPT = """
You are a clinical appraisal evidence specialist supporting a PRISMA 2020-compliant systematic review.

You are in SIGNAL EXTRACTION MODE. Extract methodological facts relevant to a specified signalling question. Do not assign domain judgments.

RULES:

1. Extract ONLY verbatim methodological facts from the provided text.

2. Return the most relevant statement as evidence_quote.

3. If the paper directly answers the signalling question, populate signal_answer from: Yes, Probably yes, Probably no, No, No information.

4. If no relevant evidence is present: status="NOT_REPORTED". All payload fields (evidence_quote, location, source_type, signal_answer) must be null.

5. If evidence is contradictory: status="AMBIGUOUS". Include all conflicting instances with page numbers in evidence_quote. location may be null when multiple conflicting locations exist.

6. Do not assign final domain-level judgments.

7. Do not extract from figures or visual-only elements.

8. Return valid JSON matching RoBSignal schema exactly. No preamble.
"""

ROB_JUDGMENT_SYSTEM_PROMPT = """
You are a clinical appraisal evidence specialist supporting a PRISMA 2020-compliant systematic review.

You are in JUDGMENT EXTRACTION MODE. Extract explicitly stated risk-of-bias domain judgments only.

RULES:

1. Extract ONLY judgments explicitly stated in the source using formal appraisal vocabulary.

2. The judgment must exactly match one of the allowed values for the specified tool and domain. If informal language is used, return status="AMBIGUOUS" and include the exact wording in rationale_quote.

3. rationale_quote must be the verbatim justification. If none is stated, set rationale_quote to "No rationale provided in source."

4. assessment_source: EXPLICIT_IN_PDF or EXTRACTED_FROM_REVIEW_TABLE.

5. If no explicit judgment is present: status="NOT_REPORTED". All payload fields (judgment, rationale_quote, location, source_type, assessment_source) must be null.

6. If conflicting judgments exist: status="AMBIGUOUS". Include all in rationale_quote. judgment may be null. location may be null.

7. Do not infer judgments from methodological description. Signalling evidence without a formal judgment → NOT_REPORTED.

8. Return valid JSON matching RoBJudgment schema exactly. No preamble.
"""

# ── Design detection keyword patterns ─────────────────────────────────────────

_DESIGN_KEYWORDS: dict[str, list[str]] = {
    "RCT": [
        r"\brandomis(?:ed|ation)\b", r"\brandomiz(?:ed|ation)\b",
        r"\brandomised controlled trial\b", r"\bRCT\b",
        r"\bcontrolled trial\b", r"\bblind(?:ed|ing)\b",
        r"\bplacebo\b", r"\bparallel.?group\b",
    ],
    "CLUSTER_RCT": [
        r"\bcluster.{0,10}random(?:is|iz)\b",
        r"\bcluster.{0,10}trial\b",
        r"\bgroup.?randomis(?:ed|ation)\b",
    ],
    "CROSSOVER_RCT": [
        r"\bcross.?over\b", r"\bcrossover trial\b", r"\bcarryover\b",
        r"\bwashout period\b",
    ],
    "NON_RANDOMISED_INT": [
        r"\bnon.?randomis(?:ed|ation)\b", r"\bnon.?randomiz(?:ed|ation)\b",
        r"\bquasi.?experimental\b", r"\bcontrolled before.?and.?after\b",
        r"\binterrupted time series\b",
    ],
    "PROSPECTIVE_COHORT": [
        r"\bprospective cohort\b", r"\bprospective study\b",
        r"\bfollow.?up study\b", r"\bprospective observational\b",
    ],
    "RETROSPECTIVE_COHORT": [
        r"\bretrospective cohort\b", r"\bretrospective study\b",
        r"\bmedical records\b", r"\belectronic health records\b",
        r"\bretrospective observational\b",
    ],
    "CASE_CONTROL": [
        r"\bcase.?control\b", r"\bcases and controls\b",
        r"\border\b.*\bcontrol\b",
    ],
    "CROSS_SECTIONAL": [
        r"\bcross.?sectional\b", r"\bprevalence study\b",
        r"\bsurvey\b", r"\bdescriptive study\b",
    ],
    "DIAGNOSTIC_ACCURACY": [
        r"\bdiagnostic accuracy\b", r"\bsensitivity and specificity\b",
        r"\bAUC\b", r"\bROC\b", r"\breference standard\b",
        r"\bindex test\b", r"\bQUADAS\b",
    ],
    "SYSTEMATIC_REVIEW": [
        r"\bsystematic review\b", r"\bmeta.?analysis\b",
        r"\bAMSTAR\b", r"\bPRISMA\b", r"\bpooled analysis\b",
    ],
    "ECONOMIC_CEA": [
        r"\bcost.?effectiveness\b", r"\bICER\b",
        r"\bcost per \w+ gained\b",
    ],
    "ECONOMIC_CUA": [
        r"\bcost.?utility\b", r"\bQALY\b", r"\bquality.?adjusted\b",
    ],
    "ECONOMIC_CMA": [
        r"\bcost.?minimis(?:ation|ation)\b", r"\bequally effective\b",
    ],
    "ECONOMIC_CBA": [
        r"\bcost.?benefit\b", r"\bwillingness.?to.?pay\b",
    ],
    "QUALITATIVE": [
        r"\bqualitative\b", r"\bthematic analysis\b",
        r"\bgrounded theory\b", r"\bethnograph\b",
        r"\bfocus group\b", r"\binterview\b.*\bqualitative\b",
    ],
    "ANIMAL": [
        r"\banimal study\b", r"\bmouse\b", r"\brat\b", r"\bmurine\b",
        r"\bin vivo\b", r"\bpreclinical\b", r"\bSYRCLE\b",
    ],
}


def detect_design_keywords(text: str) -> list[DesignCandidate]:
    """Keyword-regex first pass for study design detection.

    Returns a list of DesignCandidate sorted by descending confidence.
    Each hit contributes 0.15 confidence; max 0.95 per design.
    """
    text_lower = text.lower()
    scores: dict[str, float] = {}
    for design_name, patterns in _DESIGN_KEYWORDS.items():
        hit_count = sum(
            1 for p in patterns if re.search(p, text_lower, re.IGNORECASE)
        )
        if hit_count > 0:
            scores[design_name] = min(0.95, hit_count * 0.15)

    candidates = []
    for design_name, score in sorted(scores.items(), key=lambda x: -x[1]):
        try:
            design_enum = StudyDesign[design_name]
            candidates.append(DesignCandidate(design=design_enum, confidence=score))
        except KeyError:
            pass

    return candidates


# ── Async orchestrator ─────────────────────────────────────────────────────────

class LLMOrchestrator:
    """Async LLM orchestration with semaphore, retry, and cost tracking."""

    def __init__(self, api_key: str, model: str = "claude-opus-4-6") -> None:
        import anthropic
        self._client = anthropic.AsyncAnthropic(api_key=api_key)
        self._model = model
        self._semaphore = asyncio.Semaphore(config.SEMAPHORE_N)
        self._logger = get_logger()

    async def call_claude(
        self,
        system_prompt: str,
        user_message: str,
        pdf_filename: str,
        cost_tracker: CostTracker,
        max_tokens: int = 4096,
    ) -> str:
        """Call Claude with tenacity retry on transient errors.

        Raises:
            CostCapExceededError: propagated from CostTracker.record()
            anthropic.APIError: after 5 failed attempts
        """
        from tenacity import (
            retry, stop_after_attempt, wait_exponential,
            retry_if_exception_type
        )
        import anthropic

        @retry(
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=1, max=30),
            retry=retry_if_exception_type((
                anthropic.RateLimitError,
                anthropic.APITimeoutError,
                anthropic.InternalServerError,
            )),
            reraise=True,
        )
        async def _call_with_retry(attempt_num: list[int]) -> str:
            attempt_num[0] += 1
            if attempt_num[0] > 1:
                self._logger.log_api_retry(
                    pdf_filename=pdf_filename,
                    attempt=attempt_num[0],
                    error_type="retry",
                    error_msg=f"Attempt {attempt_num[0]}",
                )
            async with self._semaphore:
                response = await self._client.messages.create(
                    model=self._model,
                    max_tokens=max_tokens,
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_message}],
                )
            input_tokens = response.usage.input_tokens
            output_tokens = response.usage.output_tokens
            cost_tracker.record(input_tokens, output_tokens)
            self._logger.log_api_call(
                pdf_filename=pdf_filename,
                model=self._model,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                estimated_usd=cost_tracker.estimated_usd,
            )
            return response.content[0].text

        attempt_num = [0]
        return await _call_with_retry(attempt_num)

    async def extract_variable(
        self,
        variable_context: str,
        pdf_text: str,
        pdf_filename: str,
        cost_tracker: CostTracker,
        study_metadata: Optional[StudyMetadata] = None,
    ) -> ExtractedVariable | None:
        """Extract a single variable, with one validation-error retry.

        Returns ExtractedVariable or None on double failure.
        """
        metadata_json = study_metadata.model_dump_json() if study_metadata else "{}"
        user_msg = (
            f"study_metadata: {metadata_json}\n\n"
            f"extraction_context:\n{variable_context}\n\n"
            f"source_text:\n{pdf_text}"
        )
        raw = await self.call_claude(
            EXTRACTION_SYSTEM_PROMPT, user_msg, pdf_filename, cost_tracker
        )
        try:
            data = json.loads(raw)
            return ExtractedVariable(**data)
        except Exception as e1:
            # Retry with explicit error feedback
            retry_msg = (
                f"{user_msg}\n\n"
                f"Your previous response failed validation: {e1}\n"
                f"Correct the JSON and return a valid ExtractedVariable."
            )
            try:
                raw2 = await self.call_claude(
                    EXTRACTION_SYSTEM_PROMPT, retry_msg, pdf_filename, cost_tracker
                )
                data2 = json.loads(raw2)
                return ExtractedVariable(**data2)
            except Exception as e2:
                self._logger.log_validation_reject(
                    pdf_filename=pdf_filename,
                    variable=variable_context[:80],
                    error_msg=str(e2),
                    raw_llm_output=raw2 if "raw2" in dir() else raw,
                )
                return None

    async def extract_rob_signal(
        self,
        study_id: str,
        tool: str,
        domain_id: str,
        domain_name: str,
        signalling_question: str,
        pdf_text: str,
        pdf_filename: str,
        cost_tracker: CostTracker,
    ) -> RoBSignal | None:
        """Extract RoB signalling evidence for one domain question."""
        user_msg = (
            f"study_id: {study_id}\n"
            f"tool: {tool}\n"
            f"domain_id: {domain_id}\n"
            f"domain_name: {domain_name}\n"
            f"signalling_question: {signalling_question}\n\n"
            f"source_text:\n{pdf_text}"
        )
        raw = await self.call_claude(
            ROB_SIGNAL_SYSTEM_PROMPT, user_msg, pdf_filename, cost_tracker
        )
        try:
            data = json.loads(raw)
            signal = RoBSignal(**data)
            self._logger.log_rob_signal(
                pdf_filename=pdf_filename,
                study_id=study_id,
                tool=tool,
                domain_id=domain_id,
                status=signal.status,
                signal_answer=signal.signal_answer,
            )
            return signal
        except Exception as e:
            retry_msg = (
                f"{user_msg}\n\n"
                f"Previous response failed: {e}. Return valid RoBSignal JSON."
            )
            try:
                raw2 = await self.call_claude(
                    ROB_SIGNAL_SYSTEM_PROMPT, retry_msg, pdf_filename, cost_tracker
                )
                data2 = json.loads(raw2)
                signal2 = RoBSignal(**data2)
                self._logger.log_rob_signal(
                    pdf_filename=pdf_filename,
                    study_id=study_id,
                    tool=tool,
                    domain_id=domain_id,
                    status=signal2.status,
                    signal_answer=signal2.signal_answer,
                )
                return signal2
            except Exception as e2:
                self._logger.log_validation_reject(
                    pdf_filename=pdf_filename,
                    variable=f"ROB_SIGNAL:{tool}:{domain_id}",
                    error_msg=str(e2),
                    raw_llm_output=raw,
                )
                return None

    async def extract_rob_judgment(
        self,
        study_id: str,
        tool: str,
        domain_id: str,
        domain_name: str,
        judgment_vocabulary: list[str],
        pdf_text: str,
        pdf_filename: str,
        cost_tracker: CostTracker,
    ) -> RoBJudgment | None:
        """Extract explicit RoB domain judgment."""
        user_msg = (
            f"study_id: {study_id}\n"
            f"tool: {tool}\n"
            f"domain_id: {domain_id}\n"
            f"domain_name: {domain_name}\n"
            f"allowed_judgments: {judgment_vocabulary}\n\n"
            f"source_text:\n{pdf_text}"
        )
        raw = await self.call_claude(
            ROB_JUDGMENT_SYSTEM_PROMPT, user_msg, pdf_filename, cost_tracker
        )
        try:
            data = json.loads(raw)
            judgment = RoBJudgment(**data)
            self._logger.log_rob_judgment(
                pdf_filename=pdf_filename,
                study_id=study_id,
                tool=tool,
                domain_id=domain_id,
                judgment=judgment.judgment,
                status=judgment.status,
            )
            return judgment
        except Exception as e:
            retry_msg = (
                f"{user_msg}\n\n"
                f"Previous response failed: {e}. Return valid RoBJudgment JSON."
            )
            try:
                raw2 = await self.call_claude(
                    ROB_JUDGMENT_SYSTEM_PROMPT, retry_msg, pdf_filename, cost_tracker
                )
                data2 = json.loads(raw2)
                judgment2 = RoBJudgment(**data2)
                self._logger.log_rob_judgment(
                    pdf_filename=pdf_filename,
                    study_id=study_id,
                    tool=tool,
                    domain_id=domain_id,
                    judgment=judgment2.judgment,
                    status=judgment2.status,
                )
                return judgment2
            except Exception as e2:
                self._logger.log_validation_reject(
                    pdf_filename=pdf_filename,
                    variable=f"ROB_JUDGMENT:{tool}:{domain_id}",
                    error_msg=str(e2),
                    raw_llm_output=raw,
                )
                return None

    async def run_pass0(
        self,
        abstract_and_methods: str,
        pdf_filename: str,
        cost_tracker: CostTracker,
    ) -> tuple[StudyDesign, float, list[DesignCandidate], list[str]]:
        """Pass 0: study design detection.

        Two-step: keyword regex → LLM fallback.
        Returns (detected_design, confidence, alternatives, warnings).
        On failure: returns (UNKNOWN, 0.0, [], []).
        """
        warnings: list[str] = []

        # Step 1: keyword regex
        candidates = detect_design_keywords(abstract_and_methods)
        if candidates and candidates[0].confidence >= config.DESIGN_WARN_THRESHOLD:
            top = candidates[0]
            self._logger.log_study_design_detected(
                pdf_filename=pdf_filename,
                design=top.design.value,
                confidence=top.confidence,
                method="keyword_regex",
            )
            if top.confidence < config.DESIGN_WARN_THRESHOLD:
                warnings.append(
                    f"Low-confidence design detection: {top.design.value} "
                    f"({top.confidence:.2f})"
                )
                self._logger.log_pass0_warning(pdf_filename, warnings[-1])
            return top.design, top.confidence, candidates[1:5], warnings

        # Step 2: LLM fallback
        pass0_prompt = (
            "Classify the study design from the abstract and methods section below.\n"
            "Return JSON: {\"design\": \"<StudyDesign.value>\", \"confidence\": <0.0-1.0>, "
            "\"alternatives\": [{\"design\": \"<value>\", \"confidence\": <float>}], "
            "\"warnings\": [\"<string>\"]}\n\n"
            f"Valid design values: {[d.value for d in StudyDesign]}\n\n"
            f"Text:\n{abstract_and_methods[:config.PASS0_MAX_INPUT_TOKENS]}"
        )
        try:
            raw = await self.call_claude(
                "You are a study design classification expert.",
                pass0_prompt,
                pdf_filename,
                cost_tracker,
                max_tokens=512,
            )
            data = json.loads(raw)
            design_val = data.get("design", "unknown")
            confidence = float(data.get("confidence", 0.0))
            alternatives_raw = data.get("alternatives", [])
            llm_warnings = data.get("warnings", [])
            warnings.extend(llm_warnings)

            # Resolve design enum
            try:
                design_enum = StudyDesign(design_val)
            except ValueError:
                design_enum = StudyDesign.UNKNOWN
                confidence = 0.0

            # Build alternative candidates
            alternatives: list[DesignCandidate] = []
            for alt in alternatives_raw:
                try:
                    alternatives.append(DesignCandidate(
                        design=StudyDesign(alt["design"]),
                        confidence=float(alt["confidence"]),
                    ))
                except (KeyError, ValueError):
                    pass

            if confidence < config.DESIGN_WARN_THRESHOLD:
                warn_msg = (
                    f"Design detection confidence {confidence:.2f} below "
                    f"warning threshold {config.DESIGN_WARN_THRESHOLD}"
                )
                warnings.append(warn_msg)
                self._logger.log_pass0_warning(pdf_filename, warn_msg)

            self._logger.log_study_design_detected(
                pdf_filename=pdf_filename,
                design=design_enum.value,
                confidence=confidence,
                method="llm_fallback",
            )
            return design_enum, confidence, alternatives, warnings

        except Exception as e:
            self._logger.log_pass0_failed(pdf_filename, str(e))
            return StudyDesign.UNKNOWN, 0.0, [], [f"Pass 0 failed: {e}"]
