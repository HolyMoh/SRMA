# All configurable parameters. Imports nothing from the project.
# All other modules import from here.

SEMAPHORE_N: int = 3

COST_WARN_USD: float = 1.00
COST_HARD_STOP_USD: float = 5.00
COST_PER_1K_INPUT_TOKENS: float = 0.003
COST_PER_1K_OUTPUT_TOKENS: float = 0.015

PASS0_MAX_INPUT_TOKENS: int = 8_000
EXTRACTION_MAX_INPUT_TOKENS: int = 150_000

IRR_MINIMUM_N: int = 5

SYNONYM_YAML_PATH: str = "data/synonyms.yaml"
SYNONYM_THRESHOLD: int = 85
SYNONYM_OVERRIDE_MARGIN: int = 5

DESIGN_WARN_THRESHOLD: float = 0.60
DESIGN_BLOCK_APPRAISAL_THRESHOLD: float = 0.40

BBOX_ASSERT_ORDER: bool = True
EXPORT_EVIDENCE_AS_CELL_COMMENT: bool = True
