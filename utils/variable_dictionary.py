"""
Built-in synonym map and public API for variable header resolution.

Import direction: imports config only.
"""
from __future__ import annotations

import yaml
from pathlib import Path
from rapidfuzz import fuzz
import config

# ── Built-in synonym map (canonical_name → list[variant]) ─────────────────────

_BUILTIN_MAP: dict[str, list[str]] | None = None


def _load_builtin_map() -> dict[str, list[str]]:
    global _BUILTIN_MAP
    if _BUILTIN_MAP is not None:
        return _BUILTIN_MAP
    yaml_path = Path(config.SYNONYM_YAML_PATH)
    if yaml_path.exists():
        with open(yaml_path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
        _BUILTIN_MAP = {k: list(v) for k, v in raw.items()}
    else:
        _BUILTIN_MAP = {}
    return _BUILTIN_MAP


def get_synonyms() -> dict[str, list[str]]:
    """Built-in synonym map: {canonical_name: [accepted string variants]}."""
    return dict(_load_builtin_map())


def get_canonical_domains() -> dict[str, list[str]]:
    """Canonical names by domain.

    Domains: demographics, outcome_statistics, effect_sizes,
    rob2_domains, robins_i_domains, quadas2_domains,
    nos_items_cohort, nos_items_case_control, amstar2_items,
    jbi_items, syrcle_items, casp_items, economic_items.
    """
    return {
        "demographics": [
            "sample_size", "mean_age", "age_sd", "sex_ratio_male",
            "bmi_mean", "dropout_rate", "follow_up_duration",
        ],
        "outcome_statistics": [
            "mean", "sd", "sem", "median", "iqr_lower", "iqr_upper",
            "ci_lower", "ci_upper", "p_value", "event_count", "event_rate",
        ],
        "effect_sizes": [
            "hazard_ratio", "odds_ratio", "risk_ratio", "mean_difference",
            "absolute_risk_reduction", "nnt", "cohens_d",
            "sensitivity", "specificity", "auc", "icer",
        ],
        "rob2_domains": [
            "rob2_randomisation", "rob2_allocation_concealment",
            "rob2_blinding", "rob2_outcome_measurement",
            "rob2_selective_reporting",
        ],
        "robins_i_domains": [
            "robins_i_confounding", "robins_i_selection",
            "robins_i_classification", "robins_i_deviations",
            "robins_i_missing_data", "robins_i_outcome_measurement",
            "robins_i_selective_reporting",
        ],
        "quadas2_domains": [
            "quadas2_patient_selection", "quadas2_index_test",
            "quadas2_reference_standard", "quadas2_flow_timing",
        ],
        "nos_items_cohort": [
            "nos_cohort_representativeness", "nos_cohort_selection_unexposed",
            "nos_cohort_ascertainment_exposure", "nos_cohort_outcome_not_present",
            "nos_cohort_comparability", "nos_cohort_assessment_outcome",
            "nos_cohort_follow_up_length", "nos_cohort_adequacy_follow_up",
        ],
        "nos_items_case_control": [
            "nos_cc_case_definition", "nos_cc_representativeness_cases",
            "nos_cc_selection_controls", "nos_cc_definition_controls",
            "nos_cc_comparability", "nos_cc_ascertainment_exposure",
            "nos_cc_same_method", "nos_cc_non_response",
        ],
        "amstar2_items": [
            "amstar2_pico", "amstar2_protocol", "amstar2_study_designs",
            "amstar2_search_strategy", "amstar2_duplicate_selection",
            "amstar2_duplicate_extraction", "amstar2_excluded_studies",
            "amstar2_included_studies", "amstar2_rob_assessment",
            "amstar2_funding", "amstar2_meta_analysis_methods",
            "amstar2_rob_impact", "amstar2_heterogeneity",
            "amstar2_publication_bias", "amstar2_conflicts", "amstar2_overall",
        ],
        "jbi_items": [
            "jbi_cs_inclusion_criteria", "jbi_cs_participants",
            "jbi_cs_exposure", "jbi_cs_objective_criteria",
            "jbi_cs_confounders", "jbi_cs_strategies",
            "jbi_cs_outcomes", "jbi_cs_statistical",
        ],
        "syrcle_items": [],   # TODO: expand SYRCLE item list
        "casp_items": [],     # TODO: expand CASP item list
        "economic_items": [
            "economic_study_question", "economic_alternatives",
            "economic_effectiveness", "economic_costs",
            "economic_outcomes", "economic_time_horizon",
            "economic_discount_rate", "economic_sensitivity_analysis",
            "economic_incremental_analysis", "economic_equity",
        ],
    }


def get_all_variants(canonical_name: str) -> list[str]:
    """All accepted variants for a canonical name.

    Raises KeyError if canonical_name is not registered.
    """
    builtin = _load_builtin_map()
    if canonical_name not in builtin:
        raise KeyError(
            f"'{canonical_name}' is not a registered canonical name. "
            f"Known names: {sorted(builtin.keys())}"
        )
    return list(builtin[canonical_name])


def resolve(
    header: str,
    extra_synonyms: dict[str, list[str]] | None = None,
) -> tuple[str, float]:
    """Resolve raw header to (canonical_name, score).

    Merges built-in map with extra_synonyms.
    Built-in wins on tied score.
    User override requires score diff > config.SYNONYM_OVERRIDE_MARGIN.
    Returns ('UNRESOLVED', 0.0) if below config.SYNONYM_THRESHOLD.
    """
    builtin = _load_builtin_map()

    # Build merged lookup: {variant_lower: (canonical, score_bonus)}
    # score_bonus=1 for builtin (wins ties), 0 for user-supplied
    best_canonical = "UNRESOLVED"
    best_score = 0.0

    def _score_map(
        syn_map: dict[str, list[str]],
        bonus: float,
    ) -> tuple[str, float]:
        local_best_canonical = "UNRESOLVED"
        local_best_score = 0.0
        for canonical, variants in syn_map.items():
            for variant in variants:
                score = fuzz.ratio(header.lower(), variant.lower())
                adj = score + bonus
                if adj > local_best_score:
                    local_best_score = adj
                    local_best_canonical = canonical
        return local_best_canonical, local_best_score

    builtin_canonical, builtin_score = _score_map(builtin, bonus=0.0)

    if extra_synonyms:
        user_canonical, user_score = _score_map(extra_synonyms, bonus=0.0)
    else:
        user_canonical, user_score = "UNRESOLVED", 0.0

    # Tie-breaking: builtin wins on equal score
    if builtin_canonical != "UNRESOLVED" and user_canonical != "UNRESOLVED":
        if builtin_score >= user_score:
            best_canonical, best_score = builtin_canonical, builtin_score
        elif user_score - builtin_score > config.SYNONYM_OVERRIDE_MARGIN:
            best_canonical, best_score = user_canonical, user_score
        else:
            best_canonical, best_score = builtin_canonical, builtin_score
    elif builtin_canonical != "UNRESOLVED":
        best_canonical, best_score = builtin_canonical, builtin_score
    elif user_canonical != "UNRESOLVED":
        best_canonical, best_score = user_canonical, user_score

    # Apply threshold (strip bonus for threshold comparison)
    if best_score < config.SYNONYM_THRESHOLD:
        return ("UNRESOLVED", 0.0)

    return (best_canonical, best_score)
