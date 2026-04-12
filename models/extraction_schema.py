from __future__ import annotations
from pydantic import BaseModel, Field, model_validator
from typing import Optional, Literal
from enum import Enum
import config
from utils.errors import CostCapExceededError

# ── Enums ──────────────────────────────────────────────────────────────────────

class StudyDesign(Enum):
    RCT                      = "randomised_controlled_trial"
    CLUSTER_RCT              = "cluster_randomised_trial"
    CROSSOVER_RCT            = "crossover_rct"
    NON_RANDOMISED_INT       = "non_randomised_interventional"
    PROSPECTIVE_COHORT       = "prospective_cohort"
    RETROSPECTIVE_COHORT     = "retrospective_cohort"
    CASE_CONTROL             = "case_control"
    CROSS_SECTIONAL          = "cross_sectional"
    DIAGNOSTIC_ACCURACY      = "diagnostic_accuracy"
    SYSTEMATIC_REVIEW        = "systematic_review"
    ECONOMIC_CEA             = "economic_cost_effectiveness"
    ECONOMIC_CUA             = "economic_cost_utility"
    ECONOMIC_CMA             = "economic_cost_minimisation"
    ECONOMIC_CBA             = "economic_cost_benefit"
    QUALITATIVE              = "qualitative"
    ANIMAL                   = "animal_study"
    UNKNOWN                  = "unknown"


class NOSStarSource(Enum):
    EXPLICIT     = "explicit"
    RULE_BASED   = "rule_based"
    HUMAN_REVIEW = "human_review"


class PDFQuality(Enum):
    OK           = "ok"
    LOW_DENSITY  = "low_density"
    UNREADABLE   = "unreadable"


# ── Support models ──────────────────────────────────────────────────────────────

class TreatmentArm(BaseModel):
    name: str
    role: Literal["INTERVENTION", "COMPARATOR", "CONTROL", "UNKNOWN"]


class DesignCandidate(BaseModel):
    design: StudyDesign
    confidence: float = Field(ge=0.0, le=1.0)


class ClinicalContextBundle(BaseModel):
    outcome_name: Optional[str] = None
    timepoint: Optional[str] = None
    analysis_population: Optional[Literal[
        "ITT", "modified ITT", "per-protocol",
        "safety", "evaluable", "full analysis set", "not reported"
    ]] = None
    estimate_type: Optional[Literal[
        "adjusted", "unadjusted", "change_from_baseline",
        "endpoint_value", "event_count", "event_rate",
        "median", "mean_difference", "proportion"
    ]] = None
    comparison_group: Optional[str] = None
    effect_direction: Optional[Literal[
        "favours_intervention", "favours_comparator",
        "no_difference", "not_reported"
    ]] = None
    adjustment_covariates: Optional[str] = None


class CostTracker(BaseModel):
    """Per-PDF cost tracker. CostCapExceededError is imported from utils/errors.py."""
    pdf_filename: str
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    estimated_usd: float = 0.0

    def record(self, input_tokens: int, output_tokens: int) -> None:
        self.total_input_tokens += input_tokens
        self.total_output_tokens += output_tokens
        self.estimated_usd = (
            self.total_input_tokens  / 1000 * config.COST_PER_1K_INPUT_TOKENS +
            self.total_output_tokens / 1000 * config.COST_PER_1K_OUTPUT_TOKENS
        )
        if self.estimated_usd >= config.COST_HARD_STOP_USD:
            raise CostCapExceededError(
                f"{self.pdf_filename}: hard stop at "
                f"${config.COST_HARD_STOP_USD:.2f}. "
                f"Actual: ${self.estimated_usd:.4f}."
            )


# ── Location ────────────────────────────────────────────────────────────────────

class ExtractionLocation(BaseModel):
    page: int
    table_id: Optional[str] = None
    # Zero-based indices into cell_matrix after header normalisation.
    # cell_matrix[0][0] is the first data cell. Header rows live in column_headers.
    # Spanning header metadata is additive — does not shift cell_col indices.
    cell_row: Optional[int] = None
    cell_col: Optional[int] = None
    x0: Optional[float] = None
    y0: Optional[float] = None
    x1: Optional[float] = None
    y1: Optional[float] = None
    coordinate_system: Literal["top-left-origin"] = "top-left-origin"

    @model_validator(mode="after")
    def validate_location(self) -> ExtractionLocation:
        has_coords = all(v is not None for v in [self.x0, self.y0, self.x1, self.y1])
        has_table  = self.table_id is not None
        if not has_coords and not has_table:
            raise ValueError(
                "ExtractionLocation requires either (x0,y0,x1,y1) or table_id. "
                "page alone is insufficient provenance."
            )
        if has_coords and config.BBOX_ASSERT_ORDER:
            if self.x0 > self.x1 or self.y0 > self.y1:
                raise ValueError(
                    f"Invalid bbox: x0={self.x0} x1={self.x1} "
                    f"y0={self.y0} y1={self.y1}. "
                    "Probable coordinate normalisation failure in coordinate_utils.py."
                )
        return self


# ── LLM output contracts ────────────────────────────────────────────────────────

class ExtractedVariable(BaseModel):
    variable: str
    canonical_name: str
    context: ClinicalContextBundle
    study_arm: Optional[str] = None
    arm_role: Optional[Literal[
        "INTERVENTION", "COMPARATOR", "CONTROL", "OVERALL"
    ]] = None
    value: Optional[float | str] = None
    unit: Optional[str] = None
    unit_mismatch: bool = False
    evidence_quote: Optional[str] = None
    # Low-confidence notes go here. Never modify evidence_quote with commentary.
    uncertainty_note: Optional[str] = None
    source_type: Optional[Literal[
        "text_block", "table_cell", "footnote",
        "header_derived", "caption_derived"
    ]] = None
    location: ExtractionLocation
    confidence: float = Field(ge=0.0, le=1.0)
    status: Literal[
        "EXTRACTED", "CALCULATED", "NOT_REPORTED", "AMBIGUOUS", "EXTRACTION_FAILED"
    ]
    verification_flag: Optional[Literal[
        "MATCH", "MISMATCH", "NOT_FOUND", "AMBIGUOUS"
    ]] = None
    mismatch_detail: Optional[dict] = None
    calculation_trace: Optional[str] = None

    @model_validator(mode="after")
    def validate_extracted_fields(self) -> ExtractedVariable:
        if self.status == "EXTRACTED" and self.value is not None:
            if not self.evidence_quote:
                raise ValueError(
                    "evidence_quote is required for status='EXTRACTED' "
                    "with a non-null value."
                )
            value_norm = str(self.value).replace(",", "").rstrip("0").rstrip(".")
            quote_norm = self.evidence_quote.replace(",", "")
            if (value_norm not in quote_norm
                    and str(self.value) not in self.evidence_quote):
                object.__setattr__(self, "confidence", min(self.confidence, 0.85))
        if self.arm_role is not None and self.study_arm is None:
            raise ValueError("study_arm is required when arm_role is set.")
        if self.value == 0 and self.status not in {"EXTRACTED", "CALCULATED"}:
            raise ValueError(
                "value=0 is valid only when explicitly extracted (EXTRACTED) "
                "or deterministically calculated (CALCULATED)."
            )
        if self.status == "CALCULATED" and not self.calculation_trace:
            raise ValueError(
                "calculation_trace is required for status='CALCULATED'."
            )
        return self


class RoBSignal(BaseModel):
    """
    Output of signalling evidence extraction mode.
    Status governs which payload fields must be present or null.
    """
    study_id: str
    tool: str
    domain_id: str
    domain_name: str
    signalling_question: str
    evidence_quote: Optional[str] = None
    location: Optional[ExtractionLocation] = None
    source_type: Optional[Literal[
        "text_block", "table_cell", "footnote",
        "header_derived", "caption_derived"
    ]] = None
    signal_answer: Optional[Literal[
        "Yes", "Probably yes", "Probably no", "No", "No information"
    ]] = None
    status: Literal["EXTRACTED", "NOT_REPORTED", "AMBIGUOUS"] = "EXTRACTED"

    @model_validator(mode="after")
    def validate_signal(self) -> RoBSignal:
        if self.status == "EXTRACTED":
            if not self.evidence_quote:
                raise ValueError(
                    "evidence_quote required for RoBSignal status='EXTRACTED'."
                )
            if self.location is None:
                raise ValueError(
                    "location required for RoBSignal status='EXTRACTED'."
                )
            if self.source_type is None:
                raise ValueError(
                    "source_type required for RoBSignal status='EXTRACTED'."
                )
        elif self.status == "NOT_REPORTED":
            populated = {k: v for k, v in {
                "evidence_quote": self.evidence_quote,
                "location": self.location,
                "source_type": self.source_type,
                "signal_answer": self.signal_answer,
            }.items() if v is not None}
            if populated:
                raise ValueError(
                    f"RoBSignal payload fields must be null when "
                    f"status='NOT_REPORTED'. Non-null: {list(populated)}."
                )
        elif self.status == "AMBIGUOUS":
            if not self.evidence_quote:
                raise ValueError(
                    "evidence_quote required for RoBSignal status='AMBIGUOUS'. "
                    "Include all conflicting instances with page numbers."
                )
            # location may be null — multiple conflicting locations
            # cannot be represented in a single ExtractionLocation
        return self


class RoBJudgment(BaseModel):
    """
    Output of explicit judgment extraction mode.
    Status governs which payload fields must be present or null.
    """
    study_id: str
    tool: str
    domain_id: str
    domain_name: str
    judgment: Optional[str] = None
    rationale_quote: Optional[str] = None
    location: Optional[ExtractionLocation] = None
    source_type: Optional[Literal[
        "text_block", "table_cell", "footnote",
        "header_derived", "caption_derived"
    ]] = None
    assessment_source: Optional[Literal[
        "EXPLICIT_IN_PDF", "EXTRACTED_FROM_REVIEW_TABLE"
    ]] = None
    status: Literal["EXTRACTED", "NOT_REPORTED", "AMBIGUOUS"] = "EXTRACTED"

    @model_validator(mode="after")
    def validate_judgment(self) -> RoBJudgment:
        if self.status == "EXTRACTED":
            if not self.judgment:
                raise ValueError(
                    "judgment required for RoBJudgment status='EXTRACTED'."
                )
            if not self.rationale_quote:
                raise ValueError(
                    "rationale_quote required for RoBJudgment status='EXTRACTED'. "
                    "If absent in source, use: 'No rationale provided in source.'"
                )
            if self.location is None:
                raise ValueError(
                    "location required for RoBJudgment status='EXTRACTED'."
                )
            if self.assessment_source is None:
                raise ValueError(
                    "assessment_source required for RoBJudgment status='EXTRACTED'."
                )
        elif self.status == "NOT_REPORTED":
            populated = {k: v for k, v in {
                "judgment": self.judgment,
                "rationale_quote": self.rationale_quote,
                "location": self.location,
                "source_type": self.source_type,
                "assessment_source": self.assessment_source,
            }.items() if v is not None}
            if populated:
                raise ValueError(
                    f"RoBJudgment payload fields must be null when "
                    f"status='NOT_REPORTED'. Non-null: {list(populated)}."
                )
        elif self.status == "AMBIGUOUS":
            if not self.rationale_quote:
                raise ValueError(
                    "rationale_quote required for RoBJudgment status='AMBIGUOUS'. "
                    "Include all conflicting judgments and their source locations."
                )
            # judgment may be null — conflicting values cannot resolve to one
            # location may be null — multiple conflicting locations
        return self


# ── Persisted validated models ──────────────────────────────────────────────────

class NOSResult(BaseModel):
    study_id: str
    variant: Literal["cohort", "case_control"]
    item_evidence: dict[str, str]
    item_stars: Optional[dict[str, int]] = None
    total_stars: Optional[int] = None
    quality_category: Optional[Literal["high", "moderate", "low"]] = None
    star_source: NOSStarSource = NOSStarSource.HUMAN_REVIEW

    @model_validator(mode="after")
    def validate_stars(self) -> NOSResult:
        if self.star_source == NOSStarSource.HUMAN_REVIEW:
            if self.item_stars is not None:
                raise ValueError(
                    "item_stars must be None when star_source=HUMAN_REVIEW."
                )
            object.__setattr__(self, "total_stars", None)
            object.__setattr__(self, "quality_category", None)
        else:
            if self.item_stars is None:
                raise ValueError(
                    f"item_stars required when star_source='{self.star_source.value}'."
                )
            total = sum(self.item_stars.values())
            object.__setattr__(self, "total_stars", total)
            object.__setattr__(self, "quality_category",
                "high" if total >= 7 else "moderate" if total >= 5 else "low")
        return self


class StudyMetadata(BaseModel):
    study_id: str
    treatment_arms: list[TreatmentArm]
    primary_outcomes: list[str]
    population_description: str
    detected_study_design: StudyDesign
    design_confidence: float = Field(ge=0.0, le=1.0)
    design_alternatives: list[DesignCandidate]
    design_overridden_by_user: bool = False
    # Economic subtype is encoded in detected_study_design (ECONOMIC_CEA etc.).
    # Do not add a redundant economic_subtype field.
    candidate_rob_tools: list[str]
    pass0_confidence: float = Field(ge=0.0, le=1.0)
    pass0_warnings: list[str]


class RoBAssessment(BaseModel):
    """
    Validated, persisted container for per-study risk-of-bias appraisal results.
    Must be BaseModel — it is exported, audited, and user-visible.
    overall_judgements values are validated against tool judgment vocabularies
    by a model_validator to prevent silent vocabulary violations.
    """
    study_id: str
    study_design: StudyDesign
    design_confidence: float = Field(ge=0.0, le=1.0)
    design_alternatives: list[DesignCandidate]
    design_overridden_by_user: bool
    tools_applied: list[str]
    tool_mismatch_warning: Optional[str] = None
    signals: dict[str, list[RoBSignal]]
    judgments: dict[str, list[RoBJudgment]]
    nos_result: Optional[NOSResult] = None
    overall_judgements: dict[str, str]  # {tool_name: overall_judgment_string}
    assessment_source: Literal["EXPLICIT_IN_PDF", "SIGNAL_ONLY", "MIXED"]
    casp_screening_only: bool = False

    # Allowed overall judgment values per tool — used by validator below.
    _TOOL_OVERALL_VOCAB: dict[str, set[str]] = {
        "RoB2":           {"Low", "Some concerns", "High"},
        "RoB2_cluster":   {"Low", "Some concerns", "High"},
        "RoB2_crossover": {"Low", "Some concerns", "High"},
        "ROBINS_I":       {"Low", "Moderate", "Serious", "Critical", "NI"},
        "QUADAS2":        {"Low", "High", "Unclear"},
        "NOS_cohort":     set(),   # NOS uses star scores, not overall judgment strings
        "NOS_case_control": set(),
        "AMSTAR2":        {"High", "Moderate", "Low", "Critically low"},
        "JBI_cross_sectional": set(),  # No formal overall — individual items only
        "SYRCLE":         {"Low", "High", "Unclear"},
        "CASP_qualitative": set(),
        "Drummond_CEA":   set(),
        "Drummond_CUA":   set(),
        "Drummond_CMA":   set(),
        "Drummond_CBA":   set(),
    }

    @model_validator(mode="after")
    def validate_overall_judgements(self) -> RoBAssessment:
        for tool, judgment in self.overall_judgements.items():
            allowed = self._TOOL_OVERALL_VOCAB.get(tool)
            if allowed is None:
                # Unknown tool — warn but do not reject
                continue
            if len(allowed) == 0:
                # Tool has no formal overall judgment — flag if one is present
                raise ValueError(
                    f"Tool '{tool}' does not use an overall summary judgment. "
                    f"Remove '{judgment}' from overall_judgements."
                )
            if judgment not in allowed:
                raise ValueError(
                    f"Invalid overall judgment '{judgment}' for tool '{tool}'. "
                    f"Allowed: {sorted(allowed)}."
                )
        return self


class IRRResult(BaseModel):
    """
    Inter-rater reliability result for a single variable comparison.
    Lives here because it is a validated, exported, audited object.
    IRR computation logic lives in utils/irr_engine.py; this model is only the output contract.
    """
    variable: str
    variable_type: Literal["categorical", "continuous"]
    n_compared: int
    insufficient_n: bool = False
    insufficient_n_warning: Optional[str] = None
    # Categorical (Cohen's Kappa)
    cohen_kappa: Optional[float] = None
    kappa_se: Optional[float] = None
    kappa_interpretation: Optional[Literal[
        "poor", "slight", "fair", "moderate", "substantial", "almost perfect"
    ]] = None
    # Continuous
    percentage_agreement: Optional[float] = None
    mean_absolute_error: Optional[float] = None
    max_absolute_error: Optional[float] = None
    # Both
    match_count: int = 0
    mismatch_count: int = 0
    not_found_count: int = 0

    @model_validator(mode="after")
    def validate_irr_completeness(self) -> IRRResult:
        if self.insufficient_n:
            if self.insufficient_n_warning is None:
                raise ValueError(
                    "insufficient_n_warning is required when insufficient_n=True. "
                    f"Include the actual n and the minimum threshold "
                    f"(config.IRR_MINIMUM_N)."
                )
        else:
            # Sufficient n — appropriate fields must be populated
            if self.variable_type == "categorical" and self.cohen_kappa is None:
                raise ValueError(
                    "cohen_kappa is required for categorical IRRResult "
                    "when n is sufficient."
                )
            if self.variable_type == "continuous" and self.percentage_agreement is None:
                raise ValueError(
                    "percentage_agreement is required for continuous IRRResult "
                    "when n is sufficient."
                )
        return self
