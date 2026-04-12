# Deterministic nodes — no external deps beyond numpy/pandas
from .model_builder import model_builder_node
from .decomposer import decomposer_node
from .spec_reconciler import spec_reconciler_node

# Agentic nodes — require langchain_core / langgraph
try:
    from .paper_analyst import paper_analyst_node
    from .classification_mapper import classification_mapper_node
    from .data_acquirer import data_acquirer_node
    from .data_guide import data_guide_node
    from .data_preparer import data_preparer_node
    from .output_producer import output_producer_node
    from .reviewer import reviewer_node
    _agentic_available = True
except ImportError as _import_err:
    _agentic_available = False
    _agentic_error = str(_import_err)

    def _agentic_unavailable(*args, **kwargs):
        raise ImportError(
            f"Agentic nodes require langchain dependencies: {_agentic_error}\n"
            "Install with: pip install langchain-anthropic langchain-openai langgraph"
        )

    paper_analyst_node = _agentic_unavailable          # type: ignore[assignment]
    classification_mapper_node = _agentic_unavailable  # type: ignore[assignment]
    data_acquirer_node = _agentic_unavailable          # type: ignore[assignment]
    data_guide_node = _agentic_unavailable             # type: ignore[assignment]
    data_preparer_node = _agentic_unavailable          # type: ignore[assignment]
    output_producer_node = _agentic_unavailable        # type: ignore[assignment]
    reviewer_node = _agentic_unavailable               # type: ignore[assignment]

__all__ = [
    "model_builder_node",
    "decomposer_node",
    "spec_reconciler_node",
    "paper_analyst_node",
    "classification_mapper_node",
    "data_acquirer_node",
    "data_guide_node",
    "data_preparer_node",
    "output_producer_node",
    "reviewer_node",
]
