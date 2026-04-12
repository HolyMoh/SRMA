"""
Risk-of-bias tool registry and domain schemas.

Import rule: imports enums and models from models.extraction_schema only.
Never imported by extraction_schema.

GRADE operates at review level across pooled evidence from multiple studies.
Do not call GRADE logic from Layer A or Layer B modules.
"""
from __future__ import annotations
from typing import Any

from models.extraction_schema import (
    StudyDesign, NOSStarSource, RoBSignal, RoBJudgment, RoBAssessment
)

# ── Tool-to-design mapping ─────────────────────────────────────────────────────

ROB_TOOL_MAP: dict[StudyDesign, list[str]] = {
    StudyDesign.RCT:                  ["RoB2"],
    StudyDesign.CLUSTER_RCT:          ["RoB2_cluster"],
    StudyDesign.CROSSOVER_RCT:        ["RoB2_crossover"],
    StudyDesign.NON_RANDOMISED_INT:   ["ROBINS_I"],
    StudyDesign.PROSPECTIVE_COHORT:   ["NOS_cohort"],
    StudyDesign.RETROSPECTIVE_COHORT: ["NOS_cohort"],
    StudyDesign.CASE_CONTROL:         ["NOS_case_control"],
    StudyDesign.CROSS_SECTIONAL:      ["JBI_cross_sectional"],
    StudyDesign.DIAGNOSTIC_ACCURACY:  ["QUADAS2"],
    StudyDesign.SYSTEMATIC_REVIEW:    ["AMSTAR2"],
    StudyDesign.ECONOMIC_CEA:         ["Drummond_CEA"],
    StudyDesign.ECONOMIC_CUA:         ["Drummond_CUA"],
    StudyDesign.ECONOMIC_CMA:         ["Drummond_CMA"],
    StudyDesign.ECONOMIC_CBA:         ["Drummond_CBA"],
    StudyDesign.QUALITATIVE:          ["CASP_qualitative"],
    StudyDesign.ANIMAL:               ["SYRCLE"],
    StudyDesign.UNKNOWN:              [],
}


# ── Domain schemas ─────────────────────────────────────────────────────────────
# Each domain entry:
#   domain_id: str
#   domain_name: str
#   signalling_questions: list[str]
#   judgment_vocabulary: list[str]   (empty list = no formal per-domain judgment)
#   notes: str (optional)

ROB2_DOMAINS: list[dict[str, Any]] = [
    {
        "domain_id": "D1",
        "domain_name": "Bias arising from the randomisation process",
        "signalling_questions": [
            "Was the allocation sequence random?",
            "Was the allocation sequence concealed until participants were enrolled and assigned to interventions?",
            "Did baseline differences between intervention groups suggest a problem with the randomisation process?",
        ],
        "judgment_vocabulary": ["Low", "Some concerns", "High"],
    },
    {
        "domain_id": "D2",
        "domain_name": "Bias due to deviations from intended interventions",
        "signalling_questions": [
            "Were participants aware of their assigned intervention during the trial?",
            "Were carers and people delivering the interventions aware of participants' assigned intervention?",
            "If yes, were there deviations from the intended intervention that arose because of the experimental context?",
            "Were any such deviations likely to have affected the outcome?",
            "Was an appropriate analysis used to estimate the effect of assignment to intervention?",
        ],
        "judgment_vocabulary": ["Low", "Some concerns", "High"],
    },
    {
        "domain_id": "D3",
        "domain_name": "Bias due to missing outcome data",
        "signalling_questions": [
            "Were outcome data available for all, or nearly all, participants?",
            "If not, is there evidence that the result was not biased by missing outcome data?",
            "Could missingness in the outcome depend on its true value?",
        ],
        "judgment_vocabulary": ["Low", "Some concerns", "High"],
    },
    {
        "domain_id": "D4",
        "domain_name": "Bias in measurement of the outcome",
        "signalling_questions": [
            "Was the method for measuring the outcome inappropriate?",
            "Could measurement or ascertainment of the outcome have differed between intervention groups?",
            "Were outcome assessors aware of the intervention received by study participants?",
            "Could assessment of the outcome have been influenced by knowledge of intervention received?",
        ],
        "judgment_vocabulary": ["Low", "Some concerns", "High"],
    },
    {
        "domain_id": "D5",
        "domain_name": "Bias in selection of the reported result",
        "signalling_questions": [
            "Were the trial outcomes pre-specified?",
            "Was the numerical result that was reported selected from multiple eligible outcome measurements?",
            "Was the numerical result that was reported selected from multiple eligible analyses?",
        ],
        "judgment_vocabulary": ["Low", "Some concerns", "High"],
    },
]

ROB2_CLUSTER_DOMAINS: list[dict[str, Any]] = [
    {
        "domain_id": "D1",
        "domain_name": "Bias arising from the randomisation process (cluster)",
        "signalling_questions": [
            "Was the allocation sequence random?",
            "Was the allocation sequence concealed until clusters were enrolled and assigned to interventions?",
            "Did baseline differences between intervention groups suggest a problem with the randomisation process?",
            "Was the analysis based on a cluster-level summary measure or appropriate for cluster randomisation?",
        ],
        "judgment_vocabulary": ["Low", "Some concerns", "High"],
    },
    {
        "domain_id": "D1b",
        "domain_name": "Bias arising from identification or recruitment of individual participants within clusters",
        "signalling_questions": [
            "Were individuals recruited to the trial after clusters were randomised?",
            "If yes, is it likely that recruitment was affected by knowledge of the assigned intervention?",
        ],
        "judgment_vocabulary": ["Low", "Some concerns", "High"],
    },
] + ROB2_DOMAINS[1:]  # Domains D2–D5 identical to standard RoB2

ROB2_CROSSOVER_DOMAINS: list[dict[str, Any]] = [
    {
        "domain_id": "D1",
        "domain_name": "Bias arising from the randomisation process (crossover)",
        "signalling_questions": [
            "Was the allocation sequence random?",
            "Was the allocation sequence concealed?",
            "Were there baseline imbalances suggesting a problem with randomisation?",
        ],
        "judgment_vocabulary": ["Low", "Some concerns", "High"],
    },
    {
        "domain_id": "D2",
        "domain_name": "Bias due to period and carryover effects",
        "signalling_questions": [
            "Was an appropriate washout period used?",
            "Is there evidence of a carryover effect?",
            "Is the condition being studied likely stable across periods?",
        ],
        "judgment_vocabulary": ["Low", "Some concerns", "High"],
    },
] + ROB2_DOMAINS[1:]  # D3–D5 identical

ROBINS_I_DOMAINS: list[dict[str, Any]] = [
    {
        "domain_id": "D1",
        "domain_name": "Bias due to confounding",
        "signalling_questions": [
            "Is there potential for confounding by variables that predict initiation of the intervention?",
            "Were all important confounders measured?",
            "Were the important confounders appropriately controlled?",
        ],
        "judgment_vocabulary": ["Low", "Moderate", "Serious", "Critical", "NI"],
    },
    {
        "domain_id": "D2",
        "domain_name": "Bias in selection of participants into the study",
        "signalling_questions": [
            "Was selection of participants into the study based on participant characteristics observed after the start of intervention?",
            "If yes, is there evidence that selection was not related to the outcome?",
        ],
        "judgment_vocabulary": ["Low", "Moderate", "Serious", "Critical", "NI"],
    },
    {
        "domain_id": "D3",
        "domain_name": "Bias in classification of interventions",
        "signalling_questions": [
            "Were intervention groups clearly defined?",
            "Was the information used to define intervention groups recorded at the start of the intervention?",
            "Could the classification of intervention status have been affected by knowledge of the outcome?",
        ],
        "judgment_vocabulary": ["Low", "Moderate", "Serious", "Critical", "NI"],
    },
    {
        "domain_id": "D4",
        "domain_name": "Bias due to deviations from intended interventions",
        "signalling_questions": [
            "Were there deviations from the intended intervention beyond what would be expected in usual practice?",
            "Were these deviations likely to have affected the outcome?",
            "Was an appropriate analysis used to estimate the effect of interest?",
        ],
        "judgment_vocabulary": ["Low", "Moderate", "Serious", "Critical", "NI"],
    },
    {
        "domain_id": "D5",
        "domain_name": "Bias due to missing data",
        "signalling_questions": [
            "Were outcome data available for all, or nearly all, participants?",
            "Are there indications that the result was biased by missing data?",
            "Could missingness in the outcome depend on its true value?",
        ],
        "judgment_vocabulary": ["Low", "Moderate", "Serious", "Critical", "NI"],
    },
    {
        "domain_id": "D6",
        "domain_name": "Bias in measurement of outcomes",
        "signalling_questions": [
            "Could the outcome measure have been influenced by knowledge of the intervention received?",
            "Were the methods of outcome assessment comparable across intervention groups?",
            "Were any systematic errors in measurement of the outcome related to the intervention received?",
        ],
        "judgment_vocabulary": ["Low", "Moderate", "Serious", "Critical", "NI"],
    },
    {
        "domain_id": "D7",
        "domain_name": "Bias in selection of the reported result",
        "signalling_questions": [
            "Were the trial outcomes pre-specified in a protocol?",
            "Was the result from a primary analysis, as pre-specified?",
            "Was the reported effect estimate selected from multiple eligible estimates?",
        ],
        "judgment_vocabulary": ["Low", "Moderate", "Serious", "Critical", "NI"],
    },
]

QUADAS2_DOMAINS: list[dict[str, Any]] = [
    {
        "domain_id": "D1",
        "domain_name": "Patient Selection",
        "signalling_questions": [
            "Was a consecutive or random sample of patients enrolled?",
            "Was a case-control design avoided?",
            "Did the study avoid inappropriate exclusions?",
        ],
        "applicability_concern": "Are there concerns that the included patients do not match the review question?",
        "judgment_vocabulary": ["Low", "High", "Unclear"],
    },
    {
        "domain_id": "D2",
        "domain_name": "Index Test",
        "signalling_questions": [
            "Were the index test results interpreted without knowledge of the results of the reference standard?",
            "If a threshold was used, was it pre-specified?",
        ],
        "applicability_concern": "Are there concerns that the index test, its conduct, or interpretation differ from the review question?",
        "judgment_vocabulary": ["Low", "High", "Unclear"],
    },
    {
        "domain_id": "D3",
        "domain_name": "Reference Standard",
        "signalling_questions": [
            "Is the reference standard likely to correctly classify the target condition?",
            "Were the reference standard results interpreted without knowledge of the results of the index test?",
        ],
        "applicability_concern": "Are there concerns that the target condition as defined by the reference standard does not match the review question?",
        "judgment_vocabulary": ["Low", "High", "Unclear"],
    },
    {
        "domain_id": "D4",
        "domain_name": "Flow and Timing",
        "signalling_questions": [
            "Was there an appropriate interval between index test and reference standard?",
            "Did all patients receive a reference standard?",
            "Did all patients receive the same reference standard?",
            "Were all patients included in the analysis?",
        ],
        "applicability_concern": None,
        "judgment_vocabulary": ["Low", "High", "Unclear"],
    },
]

NOS_COHORT_ITEMS: list[dict[str, Any]] = [
    {
        "item_id": "S1",
        "domain": "Selection",
        "item_name": "Representativeness of the exposed cohort",
        "stars_available": 1,
        "criteria": [
            "Truly representative of the average [exposed individual] in the community",
            "Somewhat representative of the average [exposed individual] in the community",
            "Selected group of users (e.g. nurses, volunteers)",
            "No description of the derivation of the cohort",
        ],
    },
    {
        "item_id": "S2",
        "domain": "Selection",
        "item_name": "Selection of the non-exposed cohort",
        "stars_available": 1,
        "criteria": [
            "Drawn from the same community as the exposed cohort",
            "Drawn from a different source",
            "No description of the derivation of the non-exposed cohort",
        ],
    },
    {
        "item_id": "S3",
        "domain": "Selection",
        "item_name": "Ascertainment of exposure",
        "stars_available": 1,
        "criteria": [
            "Secure record (e.g. surgical records)",
            "Structured interview",
            "Written self-report",
            "No description",
        ],
    },
    {
        "item_id": "S4",
        "domain": "Selection",
        "item_name": "Demonstration that outcome of interest was not present at start of study",
        "stars_available": 1,
        "criteria": [
            "Yes",
            "No",
        ],
    },
    {
        "item_id": "C1",
        "domain": "Comparability",
        "item_name": "Comparability of cohorts on the basis of the design or analysis",
        "stars_available": 2,
        "criteria": [
            "Study controls for [most important factor]",
            "Study controls for any additional factor",
        ],
        "notes": "Up to 2 stars for comparability",
    },
    {
        "item_id": "O1",
        "domain": "Outcome",
        "item_name": "Assessment of outcome",
        "stars_available": 1,
        "criteria": [
            "Independent blind assessment",
            "Record linkage",
            "Self-report",
            "No description",
        ],
    },
    {
        "item_id": "O2",
        "domain": "Outcome",
        "item_name": "Was follow-up long enough for outcomes to occur",
        "stars_available": 1,
        "criteria": [
            "Yes (select an adequate follow-up period for outcome of interest)",
            "No",
        ],
    },
    {
        "item_id": "O3",
        "domain": "Outcome",
        "item_name": "Adequacy of follow-up of cohorts",
        "stars_available": 1,
        "criteria": [
            "Complete follow-up: all subjects accounted for",
            "Subjects lost to follow-up unlikely to introduce bias (small number lost)",
            "Follow-up rate < [X]% and no description of those lost",
            "No statement",
        ],
    },
]

NOS_CASE_CONTROL_ITEMS: list[dict[str, Any]] = [
    {
        "item_id": "S1",
        "domain": "Selection",
        "item_name": "Is the case definition adequate?",
        "stars_available": 1,
        "criteria": [
            "Yes, with independent validation",
            "Yes, e.g. record linkage or based on self-report",
            "No description",
        ],
    },
    {
        "item_id": "S2",
        "domain": "Selection",
        "item_name": "Representativeness of the cases",
        "stars_available": 1,
        "criteria": [
            "Consecutive or obviously representative series of cases",
            "Potential for selection biases or not stated",
        ],
    },
    {
        "item_id": "S3",
        "domain": "Selection",
        "item_name": "Selection of Controls",
        "stars_available": 1,
        "criteria": [
            "Community controls",
            "Hospital controls",
            "No description",
        ],
    },
    {
        "item_id": "S4",
        "domain": "Selection",
        "item_name": "Definition of Controls",
        "stars_available": 1,
        "criteria": [
            "No history of disease (endpoint)",
            "No description of source",
        ],
    },
    {
        "item_id": "C1",
        "domain": "Comparability",
        "item_name": "Comparability of cases and controls on the basis of the design or analysis",
        "stars_available": 2,
        "criteria": [
            "Study controls for [most important factor]",
            "Study controls for any additional factor",
        ],
        "notes": "Up to 2 stars for comparability",
    },
    {
        "item_id": "E1",
        "domain": "Exposure",
        "item_name": "Ascertainment of exposure",
        "stars_available": 1,
        "criteria": [
            "Secure record (e.g. surgical records)",
            "Structured interview where blind to case/control status",
            "Interview not blinded to case/control status",
            "Written self-report or medical record only",
            "No description",
        ],
    },
    {
        "item_id": "E2",
        "domain": "Exposure",
        "item_name": "Same method of ascertainment for cases and controls",
        "stars_available": 1,
        "criteria": [
            "Yes",
            "No",
        ],
    },
    {
        "item_id": "E3",
        "domain": "Exposure",
        "item_name": "Non-response rate",
        "stars_available": 1,
        "criteria": [
            "Same rate for both groups",
            "Non-respondents described",
            "Rate different and no designation",
        ],
    },
]

AMSTAR2_ITEMS: list[dict[str, Any]] = [
    {
        "item_id": "1",
        "item_name": "Did the research questions and inclusion criteria for the review include the components of PICO?",
        "critical": False,
        "judgment_vocabulary": ["Yes", "Partial Yes", "No"],
    },
    {
        "item_id": "2",
        "item_name": "Did the report of the review contain an explicit statement that the review methods were established prior to the conduct of the review?",
        "critical": True,
        "judgment_vocabulary": ["Yes", "Partial Yes", "No"],
    },
    {
        "item_id": "3",
        "item_name": "Did the review authors explain their selection of the study designs for inclusion in the review?",
        "critical": False,
        "judgment_vocabulary": ["Yes", "No"],
    },
    {
        "item_id": "4",
        "item_name": "Did the review authors use a comprehensive literature search strategy?",
        "critical": True,
        "judgment_vocabulary": ["Yes", "Partial Yes", "No"],
    },
    {
        "item_id": "5",
        "item_name": "Did the review authors perform study selection in duplicate?",
        "critical": False,
        "judgment_vocabulary": ["Yes", "No"],
    },
    {
        "item_id": "6",
        "item_name": "Did the review authors perform data extraction in duplicate?",
        "critical": False,
        "judgment_vocabulary": ["Yes", "No"],
    },
    {
        "item_id": "7",
        "item_name": "Did the review authors provide a list of excluded studies and justify the exclusions?",
        "critical": False,
        "judgment_vocabulary": ["Yes", "Partial Yes", "No"],
    },
    {
        "item_id": "8",
        "item_name": "Did the review authors describe the included studies in adequate detail?",
        "critical": False,
        "judgment_vocabulary": ["Yes", "Partial Yes", "No"],
    },
    {
        "item_id": "9",
        "item_name": "Did the review authors use a satisfactory technique for assessing the risk of bias (RoB) in individual studies that were included in the review?",
        "critical": True,
        "judgment_vocabulary": ["Yes", "Partial Yes", "No"],
    },
    {
        "item_id": "10",
        "item_name": "Did the review authors report on the sources of funding for the studies included in the review?",
        "critical": False,
        "judgment_vocabulary": ["Yes", "No"],
    },
    {
        "item_id": "11",
        "item_name": "If meta-analysis was performed, did the review authors use appropriate methods for the statistical combination of results?",
        "critical": True,
        "judgment_vocabulary": ["Yes", "No", "Not applicable"],
    },
    {
        "item_id": "12",
        "item_name": "If meta-analysis was performed, did the review authors assess the potential impact of RoB in individual studies on the results of the meta-analysis?",
        "critical": True,
        "judgment_vocabulary": ["Yes", "No", "Not applicable"],
    },
    {
        "item_id": "13",
        "item_name": "Did the review authors account for RoB in individual studies when interpreting/discussing the results of the review?",
        "critical": True,
        "judgment_vocabulary": ["Yes", "No"],
    },
    {
        "item_id": "14",
        "item_name": "Did the review authors provide a satisfactory explanation for, and discussion of, any heterogeneity observed in the results of the review?",
        "critical": False,
        "judgment_vocabulary": ["Yes", "No"],
    },
    {
        "item_id": "15",
        "item_name": "If they performed quantitative synthesis, did the review authors carry out an adequate investigation of publication bias (small study bias) and discuss its likely impact on the results of the review?",
        "critical": False,
        "judgment_vocabulary": ["Yes", "No", "Not applicable"],
    },
    {
        "item_id": "16",
        "item_name": "Did the review authors report any potential sources of conflict of interest, including any funding they received for conducting the review?",
        "critical": False,
        "judgment_vocabulary": ["Yes", "No"],
    },
]

JBI_CROSS_SECTIONAL_ITEMS: list[dict[str, Any]] = [
    {
        "item_id": "1",
        "item_name": "Were the criteria for inclusion in the sample clearly defined?",
        "judgment_vocabulary": ["Yes", "No", "Unclear", "Not applicable"],
    },
    {
        "item_id": "2",
        "item_name": "Were the study subjects and the setting described in detail?",
        "judgment_vocabulary": ["Yes", "No", "Unclear", "Not applicable"],
    },
    {
        "item_id": "3",
        "item_name": "Was the exposure measured in a valid and reliable way?",
        "judgment_vocabulary": ["Yes", "No", "Unclear", "Not applicable"],
    },
    {
        "item_id": "4",
        "item_name": "Were objective, standard criteria used for measurement of the condition?",
        "judgment_vocabulary": ["Yes", "No", "Unclear", "Not applicable"],
    },
    {
        "item_id": "5",
        "item_name": "Were confounding factors identified?",
        "judgment_vocabulary": ["Yes", "No", "Unclear", "Not applicable"],
    },
    {
        "item_id": "6",
        "item_name": "Were strategies to deal with confounding factors stated?",
        "judgment_vocabulary": ["Yes", "No", "Unclear", "Not applicable"],
    },
    {
        "item_id": "7",
        "item_name": "Were the outcomes measured in a valid and reliable way?",
        "judgment_vocabulary": ["Yes", "No", "Unclear", "Not applicable"],
    },
    {
        "item_id": "8",
        "item_name": "Was appropriate statistical analysis used?",
        "judgment_vocabulary": ["Yes", "No", "Unclear", "Not applicable"],
    },
]

# Secondary tools — scaffold with # TODO: expand item definitions

SYRCLE_DOMAINS: list[dict[str, Any]] = [
    # TODO: expand SYRCLE item definitions
    {
        "domain_id": "D1", "domain_name": "Selection bias",
        "signalling_questions": ["Was the allocation sequence adequately generated?"],
        "judgment_vocabulary": ["Low", "High", "Unclear"],
    },
    {
        "domain_id": "D2", "domain_name": "Selection bias (baseline characteristics)",
        "signalling_questions": ["Were the groups similar at baseline?"],
        "judgment_vocabulary": ["Low", "High", "Unclear"],
    },
    {
        "domain_id": "D3", "domain_name": "Selection bias (allocation concealment)",
        "signalling_questions": ["Was the allocation adequately concealed?"],
        "judgment_vocabulary": ["Low", "High", "Unclear"],
    },
    {
        "domain_id": "D4", "domain_name": "Performance bias",
        "signalling_questions": ["Were the animals randomly housed during the experiment?"],
        "judgment_vocabulary": ["Low", "High", "Unclear"],
    },
    {
        "domain_id": "D5", "domain_name": "Performance bias (blinding caregivers)",
        "signalling_questions": ["Were the investigators blinded?"],
        "judgment_vocabulary": ["Low", "High", "Unclear"],
    },
    {
        "domain_id": "D6", "domain_name": "Detection bias",
        "signalling_questions": ["Were the animals selected randomly for outcome assessment?"],
        "judgment_vocabulary": ["Low", "High", "Unclear"],
    },
    {
        "domain_id": "D7", "domain_name": "Detection bias (blinding outcome assessors)",
        "signalling_questions": ["Was the outcome assessor blinded?"],
        "judgment_vocabulary": ["Low", "High", "Unclear"],
    },
    {
        "domain_id": "D8", "domain_name": "Attrition bias",
        "signalling_questions": ["Were incomplete outcome data adequately addressed?"],
        "judgment_vocabulary": ["Low", "High", "Unclear"],
    },
    {
        "domain_id": "D9", "domain_name": "Reporting bias",
        "signalling_questions": ["Are reports of the study free of selective outcome reporting?"],
        "judgment_vocabulary": ["Low", "High", "Unclear"],
    },
    {
        "domain_id": "D10", "domain_name": "Other bias",
        "signalling_questions": ["Was the study apparently free of other problems that could result in high risk of bias?"],
        "judgment_vocabulary": ["Low", "High", "Unclear"],
    },
]

CASP_QUALITATIVE_ITEMS: list[dict[str, Any]] = [
    # TODO: expand CASP qualitative item definitions
    {
        "item_id": "1",
        "item_name": "Was there a clear statement of the aims of the research?",
        "judgment_vocabulary": ["Yes", "Can't tell", "No"],
    },
    {
        "item_id": "2",
        "item_name": "Is a qualitative methodology appropriate?",
        "judgment_vocabulary": ["Yes", "Can't tell", "No"],
    },
    {
        "item_id": "3",
        "item_name": "Was the research design appropriate to address the aims of the research?",
        "judgment_vocabulary": ["Yes", "Can't tell", "No"],
    },
    {
        "item_id": "4",
        "item_name": "Was the recruitment strategy appropriate to the aims of the research?",
        "judgment_vocabulary": ["Yes", "Can't tell", "No"],
    },
    {
        "item_id": "5",
        "item_name": "Was the data collected in a way that addressed the research issue?",
        "judgment_vocabulary": ["Yes", "Can't tell", "No"],
    },
    {
        "item_id": "6",
        "item_name": "Has the relationship between researcher and participants been adequately considered?",
        "judgment_vocabulary": ["Yes", "Can't tell", "No"],
    },
    {
        "item_id": "7",
        "item_name": "Have ethical issues been taken into consideration?",
        "judgment_vocabulary": ["Yes", "Can't tell", "No"],
    },
    {
        "item_id": "8",
        "item_name": "Was the data analysis sufficiently rigorous?",
        "judgment_vocabulary": ["Yes", "Can't tell", "No"],
    },
    {
        "item_id": "9",
        "item_name": "Is there a clear statement of findings?",
        "judgment_vocabulary": ["Yes", "Can't tell", "No"],
    },
    {
        "item_id": "10",
        "item_name": "How valuable is the research?",
        "judgment_vocabulary": ["Yes", "Can't tell", "No"],
    },
]

# Drummond checklist — common items (10) plus subtype additions
DRUMMOND_COMMON_ITEMS: list[dict[str, Any]] = [
    # TODO: expand Drummond subtype-specific items
    {"item_id": "1", "item_name": "Was a well-defined question posed in answerable form?", "judgment_vocabulary": ["Yes", "Partially", "No"]},
    {"item_id": "2", "item_name": "Was a comprehensive description of the competing alternatives given?", "judgment_vocabulary": ["Yes", "Partially", "No"]},
    {"item_id": "3", "item_name": "Was the effectiveness of the programme or service established?", "judgment_vocabulary": ["Yes", "Partially", "No"]},
    {"item_id": "4", "item_name": "Were all the important and relevant costs and consequences for each alternative identified?", "judgment_vocabulary": ["Yes", "Partially", "No"]},
    {"item_id": "5", "item_name": "Were costs and consequences measured accurately in appropriate physical units?", "judgment_vocabulary": ["Yes", "Partially", "No"]},
    {"item_id": "6", "item_name": "Were costs and consequences valued credibly?", "judgment_vocabulary": ["Yes", "Partially", "No"]},
    {"item_id": "7", "item_name": "Were costs and consequences adjusted for differential timing?", "judgment_vocabulary": ["Yes", "Partially", "No"]},
    {"item_id": "8", "item_name": "Was an incremental analysis of costs and consequences of alternatives performed?", "judgment_vocabulary": ["Yes", "Partially", "No"]},
    {"item_id": "9", "item_name": "Was allowance made for uncertainty in the estimates of costs and consequences?", "judgment_vocabulary": ["Yes", "Partially", "No"]},
    {"item_id": "10", "item_name": "Did the presentation and discussion of study results include all issues of concern to users?", "judgment_vocabulary": ["Yes", "Partially", "No"]},
]

DRUMMOND_CEA_ITEMS: list[dict[str, Any]] = DRUMMOND_COMMON_ITEMS  # TODO: CEA-specific additions
DRUMMOND_CUA_ITEMS: list[dict[str, Any]] = DRUMMOND_COMMON_ITEMS  # TODO: CUA-specific additions
DRUMMOND_CMA_ITEMS: list[dict[str, Any]] = DRUMMOND_COMMON_ITEMS  # TODO: CMA-specific additions
DRUMMOND_CBA_ITEMS: list[dict[str, Any]] = DRUMMOND_COMMON_ITEMS  # TODO: CBA-specific additions


# ── Registry lookup ────────────────────────────────────────────────────────────

TOOL_DOMAIN_REGISTRY: dict[str, list[dict[str, Any]]] = {
    "RoB2":                ROB2_DOMAINS,
    "RoB2_cluster":        ROB2_CLUSTER_DOMAINS,
    "RoB2_crossover":      ROB2_CROSSOVER_DOMAINS,
    "ROBINS_I":            ROBINS_I_DOMAINS,
    "QUADAS2":             QUADAS2_DOMAINS,
    "NOS_cohort":          NOS_COHORT_ITEMS,
    "NOS_case_control":    NOS_CASE_CONTROL_ITEMS,
    "AMSTAR2":             AMSTAR2_ITEMS,
    "JBI_cross_sectional": JBI_CROSS_SECTIONAL_ITEMS,
    "SYRCLE":              SYRCLE_DOMAINS,
    "CASP_qualitative":    CASP_QUALITATIVE_ITEMS,
    "Drummond_CEA":        DRUMMOND_CEA_ITEMS,
    "Drummond_CUA":        DRUMMOND_CUA_ITEMS,
    "Drummond_CMA":        DRUMMOND_CMA_ITEMS,
    "Drummond_CBA":        DRUMMOND_CBA_ITEMS,
}


def get_tools_for_design(design: StudyDesign) -> list[str]:
    """Return the list of RoB tool names appropriate for the given study design."""
    return list(ROB_TOOL_MAP.get(design, []))


def get_domains_for_tool(tool_name: str) -> list[dict[str, Any]]:
    """Return the domain/item schema for the given tool name.

    Returns an empty list for unknown tool names.
    """
    return list(TOOL_DOMAIN_REGISTRY.get(tool_name, []))


def get_judgment_vocabulary(tool_name: str, domain_id: str) -> list[str]:
    """Return allowed judgment values for a specific tool domain.

    Returns an empty list if the tool or domain is not found.
    """
    domains = TOOL_DOMAIN_REGISTRY.get(tool_name, [])
    for domain in domains:
        did = domain.get("domain_id") or domain.get("item_id", "")
        if did == domain_id:
            return list(domain.get("judgment_vocabulary", []))
    return []


def is_nos_tool(tool_name: str) -> bool:
    """Return True if the tool uses NOS star scoring (no formal overall judgment)."""
    return tool_name in {"NOS_cohort", "NOS_case_control"}


def assign_nos_stars(
    tool_name: str,
    item_evidence: dict[str, str],
) -> tuple[dict[str, int] | None, NOSStarSource]:
    """Attempt rule-based NOS star assignment from item evidence text.

    Returns (item_stars_dict, NOSStarSource) where:
      - NOSStarSource.EXPLICIT if the paper directly reports star totals
      - NOSStarSource.RULE_BASED if deterministic rules produce stars
      - NOSStarSource.HUMAN_REVIEW if neither is possible (item_stars=None)

    This is a conservative implementation. Rule-based scoring is only applied
    when evidence strings contain clear confirmatory language.
    """
    if not item_evidence:
        return None, NOSStarSource.HUMAN_REVIEW

    # Check for explicit star report in any evidence string
    import re
    for text in item_evidence.values():
        if re.search(r'\b(\d)\s*(?:NOS\s*)?stars?\b', text, re.IGNORECASE):
            return None, NOSStarSource.EXPLICIT

    # Rule-based: not yet fully implemented — flag for human review
    return None, NOSStarSource.HUMAN_REVIEW
